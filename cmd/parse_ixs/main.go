package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"strings"
	"taxemon/pkg/assert"
	dbutils "taxemon/pkg/db_utils"
	ixparser "taxemon/pkg/ix_parser"
	"taxemon/pkg/logger"
	walletsync "taxemon/pkg/wallet_sync"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	"golang.org/x/sync/errgroup"
)

type printableIx struct {
	signature string
	line      string
}

func ixToString(tx *walletsync.SavedTransaction, all map[string]printableIx, programs map[string]bool) {
	signature := tx.Signature()

	for _, ix := range tx.Instructions() {
		data := ix.Data()
		l := 8
		if len(data) < 8 {
			l = len(data)
		}
		ixDisc := data[:l]
		discHex := hex.EncodeToString(ixDisc)

		programAddress := ix.ProgramAddress()

		programs[programAddress] = true

		key := fmt.Sprintf("%s%s%s", signature, programAddress, discHex)
		idx := ix.(*walletsync.SavedInstruction).Idx
		all[key] = printableIx{
			signature: signature,
			line:      fmt.Sprintf("Program: %s idx: %d", programAddress, idx),
		}
	}
}

func printTxs(txs []*SelectUnknownTransactionRow) {
	all := make(map[string]printableIx)
	programs := make(map[string]bool)
	for _, tx := range txs {
		dtx := walletsync.DeserializeSavedTransaction(tx.SelectTransactionsRow)
		ixToString(dtx, all, programs)
	}

	out := make(map[string]*strings.Builder)
	for _, ix := range all {
		b, ok := out[ix.signature]
		if !ok {
			b = &strings.Builder{}
			b.WriteString("\t")
			b.WriteString(ix.line)
			out[ix.signature] = b
		} else {
			b.WriteString("\n\t")
			b.WriteString(ix.line)
		}
	}

	for signature, rest := range out {
		fmt.Println(signature)
		fmt.Println(rest)
	}

	fmt.Println("\nPrograms")
	for address := range programs {
		fmt.Printf("\t%s\n", address)
	}
}

type SelectUnknownTransactionRow struct {
	*dbutils.SelectTransactionsRow
	Address  string
	WalletId int32 `db:"wallet_id"`
}

func fetchTransactions(db *sqlx.DB, signature string) []*SelectUnknownTransactionRow {
	q := strings.Builder{}
	q.WriteString(`
		select
			t.id,
			t.signature,
			t.accounts,
			t.logs,
			get_instructions(t.id, true) as instructions,
			wallet.address,
			wallet.id as wallet_id
		from
			"transaction" t
		join
			transaction_to_wallet on transaction_to_wallet.transaction_id = t.id
		join
			wallet on wallet.id = transaction_to_wallet.wallet_id
	`)

	var err error
	result := make([]*SelectUnknownTransactionRow, 0)
	if signature == "" {
		q.WriteString(`
			where exists (
				select
					1
				from
					instruction ix
				where
					ix.is_known = false AND ix.transaction_id = t.id
			)
		`)
		err = db.Select(&result, q.String())
	} else {
		q.WriteString(`
			where t.signature = $1
		`)
		err = db.Select(&result, q.String(), signature)
	}
	assert.NoErr(err, "unable to fetch unknown transactions")
	return result
}

func main() {
	err := godotenv.Load()
	assert.NoErr(err, "unable to read .env")

	err = logger.NewPrettyLogger("", int(slog.LevelDebug))
	assert.NoErr(err, "unable to use pretty logger")

	var parse bool
	flag.BoolVar(&parse, "parse", false, "if true ixs will be parsed")
	var save bool
	flag.BoolVar(&save, "save", false, "if true parsed data will be saved")
	var signature string
	flag.StringVar(&signature, "signature", "", "signature of a transaction to be parsed")
	flag.Parse()

	rpcUrl := os.Getenv("RPC_URL")
	dbUrl := os.Getenv("DB_URL")
	assert.NoEmptyStr(rpcUrl, "Missing RPC_URL")
	assert.NoEmptyStr(dbUrl, "Missing DB_PATH")

	db, err := sqlx.Connect("postgres", dbUrl)
	assert.NoErr(err, "unable to open db", "dbPath", dbUrl)

	txs := fetchTransactions(db, signature)

	if len(txs) == 0 {
		slog.Info("transactions empty")
		return
	}

	if !parse {
		printTxs(txs)
		return
	}

	txsByWallet := make(map[int32][]*SelectUnknownTransactionRow, 0)
	for _, tx := range txs {
		_, ok := txsByWallet[tx.WalletId]
		if ok {
			txsByWallet[tx.WalletId] = append(txsByWallet[tx.WalletId], tx)
		} else {
			txsByWallet[tx.WalletId] = []*SelectUnknownTransactionRow{tx}
		}
	}

	updateIxsParams := make([]*dbutils.UpdateInstructionToKnownParams, 0)
	insertableEvents := make([]*dbutils.InsertEventParams, 0)

	for walletId, txs := range txsByWallet {
		associatedAccounts := walletsync.NewAssociatedAccounts()
		associatedAccounts.FetchExisting(db, walletId)

		for _, tx := range txs {
			dtx := walletsync.DeserializeSavedTransaction(tx.SelectTransactionsRow)

			currentAssociatedAccounts, err := ixparser.ParseTx(dtx, tx.Address)
			assert.NoErr(err, "unable to parse tx")

			maps.Insert(associatedAccounts.CurrentIter, maps.All(currentAssociatedAccounts))

			for _, ixInterface := range dtx.Instructions() {
				ix := ixInterface.(*walletsync.SavedInstruction)
				if !ix.IsKnown {
					continue
				}

				updateIxsParams = append(updateIxsParams, &dbutils.UpdateInstructionToKnownParams{
					TransactionId: tx.Id,
					Idx:           ix.Idx,
				})

				for i, event := range ix.Events {
					eventSerialized, err := json.Marshal(event)
					assert.NoErr(err, "unable to serialize event", "event data", event)

					insertableEvents = append(insertableEvents, &dbutils.InsertEventParams{
						TransactionId: tx.Id,
						IxIdx:         ix.Idx,
						Idx:           int16(i),
						Type:          int16(event.Type()),
						Data:          string(eventSerialized),
					})
				}
			}
		}
	}

	tx, err := db.Beginx()
	assert.NoErr(err, "unable to begin tx")

	if !save {
		return
	}
	if len(updateIxsParams) == 0 {
		return
	}

	var eg errgroup.Group
	eg.TryGo(func() error {
		if err = dbutils.UpdateInstructionsToKnown(tx, updateIxsParams); err != nil {
			return fmt.Errorf("unable to update instructions to known: %w", err)
		}
		return nil
	})
	eg.TryGo(func() error {
		if err = dbutils.InsertEvents(tx, insertableEvents); err != nil {
			return fmt.Errorf("unable to insert events: %w", err)
		}
		return nil
	})
	err = eg.Wait()
	assert.NoErr(err, "")

	slog.Info("succesfully parsed ixs")
}
