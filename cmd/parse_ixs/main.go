package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"taxemon/pkg/assert"
	dbutils "taxemon/pkg/db_utils"
	"taxemon/pkg/logger"
	walletsync "taxemon/pkg/wallet_sync"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
)

type printableIx struct {
	signature string
	line      string
}

func ixToString(tx *walletsync.SavedTransaction, all map[string]printableIx, programs map[string]bool) {
	signature := tx.Signature

	for _, ix := range tx.Instructions {
		data := ix.Data
		l := 8
		if len(data) < 8 {
			l = len(data)
		}
		ixDisc := data[:l]
		discHex := hex.EncodeToString(ixDisc)

		programAddress := ix.ProgramAddress

		programs[programAddress] = true

		key := fmt.Sprintf("%s%s%s", signature, programAddress, discHex)
		all[key] = printableIx{
			signature: signature,
			line:      fmt.Sprintf("Program: %s idx: %d", programAddress, ix.Idx),
		}
	}
}

func printTxs(txs []*deserializedTransaction) {
	all := make(map[string]printableIx)
	programs := make(map[string]bool)
	for _, tx := range txs {
		ixToString(tx.deserialized, all, programs)
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

type selectUnknownTransactionsRow struct {
	*dbutils.SelectTransactionsRow
	Address  string
	WalletId int32 `db:"wallet_id"`
}

func fetchUnknownTransactions(db *sqlx.DB) []*selectUnknownTransactionsRow {
	q := `
		with tx_instructions as (
		    select
		        t.id,
		        t.signature,
		        t.accounts,
		        t.logs,
		        get_instructions(t.id, true)::jsonb as instructions,
		        wallet.address,
		        wallet.id as wallet_id
		    from
		        "transaction" t
		    join
		        transaction_to_wallet on transaction_to_wallet.transaction_id = t.id
		    join
		        wallet on wallet.id = transaction_to_wallet.wallet_id
		    where
		        t.err = false
		)
		select
			id, signature, accounts, logs, instructions, address, wallet_id
		from
			tx_instructions
		where
			jsonb_array_length(instructions) > 0
	`
	result := make([]*selectUnknownTransactionsRow, 0)
	err := db.Select(&result, q)
	assert.NoErr(err, "unable to fetch transactions")
	return result
}

func fetchTransactionBySignature(db *sqlx.DB, signature string) []*selectUnknownTransactionsRow {
	q := `
		select
		    t.id,
		    t.signature,
		    t.accounts,
		    t.logs,
		    get_instructions(t.id, true)::jsonb as instructions,
		    wallet.address,
		    wallet.id as wallet_id
		from
		    "transaction" t
		join
		    transaction_to_wallet on transaction_to_wallet.transaction_id = t.id
		join
		    wallet on wallet.id = transaction_to_wallet.wallet_id
		where
		    t.err = false
			and t.signature = string
	`
	result := new(selectUnknownTransactionsRow)
	err := db.Get(result, q, signature)
	assert.NoErr(err, "unable to fetch transaction")
	return []*selectUnknownTransactionsRow{result}
}

type knownInstruction struct {
	ProgramAddress string `db:"program_address"`
	Discriminator  [][]byte
}

var knownInstructions = []*knownInstruction{
	{
		ProgramAddress: walletsync.JupV6ProgramAddress,
		Discriminator:  [][]byte{walletsync.IxJupV6SharedAccountsRoute},
	},
	{
		ProgramAddress: walletsync.JupLimitProgramAddress,
		Discriminator:  [][]byte{walletsync.IxJupLimitPreFlashFillOrder},
	},
	{
		ProgramAddress: walletsync.ComputeBudgetProgramAddress,
	},
	{
		ProgramAddress: walletsync.SystemProgramAddress,
	},
	{
		ProgramAddress: walletsync.TokenProgramAddress,
	},
	{
		ProgramAddress: walletsync.AssociatedTokenProgramAddress,
	},
	{
		ProgramAddress: walletsync.BubblegumProgramAddress,
		Discriminator: [][]byte{
			walletsync.IxBubblegumMintV1,
			walletsync.IxBubblegumMintToCollectionV1,
		},
	},
}

func isKnown(programAddress string, data []byte) bool {
	for _, ki := range knownInstructions {
		if programAddress != ki.ProgramAddress {
			continue
		}
		if len(ki.Discriminator) == 0 {
			return true
		}
		for _, disc := range ki.Discriminator {
			if len(data) < len(disc) {
				continue
			}
			if slices.Equal(data[:len(disc)], disc) {
				return true
			}
		}
	}
	return false
}

type deserializedTransaction struct {
	deserialized *walletsync.SavedTransaction
	serialized   *selectUnknownTransactionsRow
}

func deserializeTransactions(serialized []*selectUnknownTransactionsRow) []*deserializedTransaction {
	deserialized := make([]*deserializedTransaction, len(serialized))
	for i, tx := range serialized {
		deserialized[i] = &deserializedTransaction{
			deserialized: walletsync.NewParsableTxFromDb(tx.SelectTransactionsRow),
			serialized:   tx,
		}
	}
	return deserialized
}

func removeKnownInstructions(
	txs []*deserializedTransaction,
) []*deserializedTransaction {
	unknownTxs := make([]*deserializedTransaction, 0)
	for i, tx := range txs {
		unknownIxs := make([]*walletsync.SavedInstruction, 0)

		ixs := txs[i].deserialized.Instructions
		for _, ix := range tx.deserialized.Instructions {
			if !isKnown(ix.ProgramAddress, ix.Data) {
				unknownIxs = append(unknownIxs, ix)
			}
		}

		if len(ixs) > 0 {
			tx.deserialized.Instructions = unknownIxs
			unknownTxs = append(unknownTxs, txs[i])
		}
	}

	return unknownTxs
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

	var txs []*deserializedTransaction
	if signature == "" {
		txs = removeKnownInstructions(deserializeTransactions(fetchUnknownTransactions(db)))
	} else {
		txs = deserializeTransactions(fetchTransactionBySignature(db, signature))
	}

	fmt.Printf("TXS %d\n", len(txs))

	if len(txs) == 0 {
		slog.Info("transactions empty")
		return
	}

	if !parse {
		printTxs(txs)
		return
	}

	txsByWallet := make(map[int32][]*deserializedTransaction, 0)
	for _, tx := range txs {
		walletId := tx.serialized.WalletId
		_, ok := txsByWallet[walletId]
		if ok {
			txsByWallet[walletId] = append(txsByWallet[walletId], tx)
		} else {
			txsByWallet[walletId] = []*deserializedTransaction{tx}
		}
	}

	insertableEvents := make([]*dbutils.InsertEventParams, 0)

	for walletId, txs := range txsByWallet {
		associatedAccounts := walletsync.NewAssociatedAccounts()
		associatedAccounts.FetchExisting(db, walletId)

		for _, tx := range txs {
			dtx := tx.deserialized
			events, err := walletsync.ParseTx(dtx, tx.serialized.Address, associatedAccounts)
			assert.NoErr(err, "unable to parse tx")

			for _, event := range events {
				eventSerialized, err := json.Marshal(event.Data)
				assert.NoErr(err, "unable to serialize event", "event data", event)

				insertableEvents = append(insertableEvents, &dbutils.InsertEventParams{
					TransactionId: tx.serialized.Id,
					IxIdx:         event.IxIdx,
					Idx:           event.Idx,
					Type:          event.Data.Type(),
					Data:          string(eventSerialized),
				})
			}
		}
	}

	if len(insertableEvents) > 0 && save {
		err = dbutils.InsertEvents(db, insertableEvents)
		assert.NoErr(err, "unable to insert events")
		slog.Info("succesfully inserted ixs")
	}
}
