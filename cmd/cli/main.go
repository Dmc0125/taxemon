package main

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"taxemon/pkg/assert"
	dbutils "taxemon/pkg/db_utils"
	"taxemon/pkg/logger"
	"taxemon/pkg/rpc"
	walletsync "taxemon/pkg/wallet_sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
)

type knownInstructionMeta struct {
	allKnown bool
	discLen  int
	discs    []string
}

var knownInstructions = func() map[string]*knownInstructionMeta {
	m := make(map[string]*knownInstructionMeta)
	m[walletsync.ComputeBudgetProgramAddress] = &knownInstructionMeta{allKnown: true}
	m[walletsync.SystemProgramAddress] = &knownInstructionMeta{allKnown: true}
	m[walletsync.TokenProgramAddress] = &knownInstructionMeta{allKnown: true}
	m[walletsync.AssociatedTokenProgramAddress] = &knownInstructionMeta{discLen: -1, discs: []string{"", "01", "00"}}
	m[walletsync.JupLimitProgramAddress] = &knownInstructionMeta{
		discLen: -1,
		discs:   []string{hex.EncodeToString(walletsync.IxJupLimitPreFlashFillOrder)},
	}
	m[walletsync.JupV6ProgramAddress] = &knownInstructionMeta{
		discLen: -1,
		discs:   []string{hex.EncodeToString(walletsync.IxJupV6SharedAccountsRoute)},
	}
	m[walletsync.BubblegumProgramAddress] = &knownInstructionMeta{
		discLen: -1,
		discs: []string{
			hex.EncodeToString(walletsync.IxBubblegumMintToCollectionV1),
			hex.EncodeToString(walletsync.IxBubblegumMintV1),
		},
	}
	m[walletsync.JupMerkleDistributorProgramAddress] = &knownInstructionMeta{
		discLen: -1,
		discs:   []string{hex.EncodeToString(walletsync.IxMerkleCloseClaimAccount), hex.EncodeToString(walletsync.IxMerkleDistributorClaim)},
	}
	return m
}()

const fetchTransactionQuery = `
	select
		t.err
		t.id,
		t.signature,
		t.accounts,
		t.logs,
		get_instructions(t.id)::jsonb as instructions,
		wallet.address,
		wallet.id as wallet_id
	from
		"transaction" t
		join
			transaction_to_wallet on transaction_to_wallet.transaction_id = t.id
		join
			wallet on wallet.id = transaction_to_wallet.wallet_id
`

type unknownInstruction struct {
	signature string
	ixIdx     int32
}

func printUnknownInstructions(db dbutils.DBTX) {
	q := `
		with tx_instructions as (
			select
				t.err,
				t.id,
				t.signature,
				t.accounts,
				t.logs,
				get_instructions(t.id, true)::jsonb as instructions
			from
				"transaction" t
		)
		select
			signature, accounts, logs, instructions
		from
			tx_instructions
		where
			jsonb_array_length(instructions) > 0
			and err = false
	`
	result := make([]*dbutils.SelectTransactionsRow, 0)
	err := db.Select(&result, q)
	assert.NoErr(err, "unable to query unknown transactions")

	unknown := make(map[string]map[string]*unknownInstruction)

	for _, tx := range result {
		parsableIx := walletsync.NewParsableTxFromDb(tx)

	ixs:
		for _, ix := range parsableIx.Instructions {

			data := hex.EncodeToString(ix.Data)
			var disc string

			k, ok := knownInstructions[ix.ProgramAddress]
			if ok {
				if k.allKnown {
					continue ixs
				}
				for _, d := range k.discs {
					if strings.HasPrefix(data, d) {
						continue ixs
					}
				}
				if k.discLen != -1 {
					if len(data) < k.discLen {
						disc = data
					} else {
						disc = data[:k.discLen]
					}
				}
			}

			if disc == "" {
				discLen := 8
				if len(ix.Data) < 8 {
					discLen = len(ix.Data)
				}
				disc = hex.EncodeToString(ix.Data[:discLen])
			}

			unknownIxs, ok := unknown[ix.ProgramAddress]
			if !ok {
				unknown[ix.ProgramAddress] = make(map[string]*unknownInstruction)
				unknownIxs = unknown[ix.ProgramAddress]
			}
			if _, ok = unknownIxs[disc]; !ok {
				unknownIxs[disc] = &unknownInstruction{
					signature: tx.Signature,
					ixIdx:     ix.Idx,
				}
			}
		}
	}

	out := strings.Builder{}
	for programAddress, ixs := range unknown {
		out.WriteString(programAddress)
		for disc, ix := range ixs {
			out.WriteString(fmt.Sprintf("\n\tSignature: %s disc: %s idx: %d", ix.signature, disc, ix.ixIdx))
		}
		out.WriteString("\n")
	}

	fmt.Println(out.String())
}

type getTransactionsRow struct {
	*dbutils.SelectTransactionsRow
	Address  string
	WalletId int32 `db:"wallet_id"`
}

type tParsableTx struct {
	*walletsync.SavedTransaction
	Address  string
	WalletId int32
}

type getTransactionsConfig struct {
	signature      string
	programAddress string
	discriminator  string
	walletAddress  string
	ignorePrograms []string
}

func getTransactions(db dbutils.DBTX, config *getTransactionsConfig) map[int32][]*tParsableTx {
	var txs []*getTransactionsRow

	q := strings.Builder{}
	q.WriteString(`
		select
			t.err,
			t.id,
			t.signature,
			t.accounts,
			t.logs,
			get_instructions(t.id)::jsonb as instructions,
			wallet.address,
			wallet.id as wallet_id
		from
			"transaction" t
			join
				transaction_to_wallet on transaction_to_wallet.transaction_id = t.id
			join
				wallet on wallet.id = transaction_to_wallet.wallet_id
	`)

	if config.signature == "" {
		var err error
		if config.walletAddress != "" {
			q.WriteString("and wallet.address = $1 ")
			q.WriteString("order by t.slot asc, t.block_index asc")
			err = db.Select(&txs, q.String(), config.walletAddress)
		} else {
			q.WriteString("order by t.slot asc, t.block_index asc")
			err = db.Select(&txs, q.String())
		}
		assert.NoErr(err, "unable to select txs")
	} else {
		q.WriteString("where t.signature = $1")
		err := db.Select(&txs, q.String(), config.signature)
		assert.NoErr(err, "unbale to get tx")
		if len(txs) == 0 {
			fmt.Printf("Transaction with signature \"%s\" does not exist", config.signature)
			os.Exit(0)
		}
	}

	filteredTxsByWallet := make(map[int32][]*tParsableTx, 0)

	for _, tx := range txs {
		parsableTx := walletsync.NewParsableTxFromDb(tx.SelectTransactionsRow)
		filteredIxs := make([]*walletsync.SavedInstruction, 0)

		for _, ix := range parsableTx.Instructions {
			if slices.Contains(config.ignorePrograms, ix.ProgramAddress) {
				continue
			}
			if config.programAddress == "" {
				filteredIxs = append(filteredIxs, ix)
				continue
			}
			if ix.ProgramAddress != config.programAddress {
				continue
			}
			if config.discriminator == "" {
				filteredIxs = append(filteredIxs, ix)
				continue
			}
			if strings.HasPrefix(hex.EncodeToString(ix.Data), config.discriminator) {
				filteredIxs = append(filteredIxs, ix)
			}
		}

		if len(filteredIxs) > 0 {
			_, ok := filteredTxsByWallet[tx.WalletId]
			if !ok {
				txs := make([]*tParsableTx, 0)
				filteredTxsByWallet[tx.WalletId] = txs
			}
			parsableTx.Instructions = filteredIxs
			filteredTxsByWallet[tx.WalletId] = append(filteredTxsByWallet[tx.WalletId], &tParsableTx{
				SavedTransaction: parsableTx,
				Address:          tx.Address,
				WalletId:         tx.WalletId,
			})
		}
	}

	return filteredTxsByWallet
}

func parseInstructions(
	db dbutils.DBTX,
	signature,
	programAddress,
	discriminator,
	walletAddress,
	ignorePrograms string,
	save bool,
) {
	txsByWallet := getTransactions(db, &getTransactionsConfig{
		signature:      signature,
		programAddress: programAddress,
		discriminator:  discriminator,
		walletAddress:  walletAddress,
		ignorePrograms: strings.Split(ignorePrograms, ","),
	})

	insertableEvents := make([]*dbutils.InsertEventParams, 0)

	for walletId, txs := range txsByWallet {
		associatedAccounts := walletsync.NewAssociatedAccounts()
		associatedAccounts.FetchExisting(db, walletId)
		for _, tx := range txs {
			err := associatedAccounts.ParseTx(tx.SavedTransaction, tx.Address)
			assert.NoErr(err, "unable to parse tx associated accounts", "signature", tx.Signature)
		}

		for _, tx := range txs {
			events, err := walletsync.ParseTxIntoEvents(tx.SavedTransaction, tx.Address, associatedAccounts)
			assert.NoErr(err, "unable to parse tx into events", "signature", tx.Signature)

			for _, event := range events {
				ser, err := json.Marshal(event.Data)
				assert.NoErr(err, "unable to serialize event", "type", event.Data.Type())
				serStr := string(ser)

				fmt.Printf("\nSignature %s ix idx %d\n", tx.Signature, event.IxIdx)
				fmt.Printf("Serialized event: %s\n", serStr)

				if save {
					insertableEvents = append(insertableEvents, &dbutils.InsertEventParams{
						TransactionId: tx.Id,
						IxIdx:         event.IxIdx,
						Idx:           event.Idx,
						Data:          serStr,
						Type:          event.Data.Type(),
					})
				}
			}
		}
	}

	if len(insertableEvents) > 0 {
		err := dbutils.InsertEvents(db, insertableEvents)
		assert.NoErr(err, "unable to insert events")
	}
}

func fetchTransactions(db *sqlx.DB, rpcClient *rpc.Client, walletAddress, signature string) {
	wallet := new(dbutils.SelectWalletsRow)
	selectWalletByAddressQ := "select id, address, last_signature from wallet where address = $1 limit 1"
	err := db.Get(wallet, selectWalletByAddressQ, walletAddress)
	if errors.Is(err, sql.ErrNoRows) {
		if wallet.Id == 0 {
			insertWalletQ := "insert into wallet (address) values ($1) returning id, address, last_signature"
			err = db.Get(wallet, insertWalletQ, walletAddress)
			assert.NoErr(err, "unable to insert wallet")
		}
	}
	assert.NoErr(err, "unable to select wallet")

	if signature == "" {
		walletsync.SyncWallet(rpcClient, db, wallet.Id, wallet.Address, wallet.LastSignature.String)
	} else {
		tx, err := rpcClient.GetTransaction(signature, rpc.CommitmentConfirmed)
		assert.NoErr(err, "unable to fetch transaction")
		insertableTx, err := walletsync.NewInsertableTransaction(tx)
		assert.NoErr(err, "unable to create insertale tx")

		parsableTx := walletsync.NewParsableTxFromInsertable(insertableTx)
		associatedAccounts := walletsync.NewAssociatedAccounts()
		associatedAccounts.FetchExisting(db, wallet.Id)

		err = associatedAccounts.ParseTx(parsableTx, walletAddress)
		assert.NoErr(err, "unable to parse associated accounts")

	}
}

func main() {
	err := godotenv.Load()
	assert.NoErr(err, "unable to read .env")

	err = logger.NewPrettyLogger("", int(slog.LevelDebug))
	assert.NoErr(err, "unable to use pretty logger")

	var (
		fetch, print, parseEvents, parseIxs, save               bool
		signature, programAddress, discriminator, walletAddress string
		ignorePrograms                                          string
	)

	flag.BoolVar(&fetch, "fetch", false, "will fetch transactions for wallet")
	flag.BoolVar(&print, "print", false, "will print instructions that are currently unparsed")
	flag.BoolVar(&parseEvents, "parse-events", false, "will parse events if true")
	flag.BoolVar(&parseIxs, "parse-ixs", false, "will parse ixs if true")
	flag.BoolVar(&save, "save", false, "will save parse data if true")
	flag.StringVar(&signature, "signature", "", "will only parse transaction with this siganture if provided")
	flag.StringVar(&programAddress, "program-address", "", "will only parse instructions with this program address (only works for --parse-ixs)")
	flag.StringVar(&discriminator, "discriminator", "", "(hex) will only parse instructions with this discriminator (only works with program address, and for --parse-ixs)")
	flag.StringVar(&walletAddress, "wallet-address", "", "instructions only for this wallet will be parsed (only works for --parse-ixs)")
	flag.StringVar(&ignorePrograms, "ignore-programs", "", "program addresses to ignore when parsing ixs separated by comma (only works for --parse-ixs)")
	flag.Parse()

	dbUrl := os.Getenv("DB_URL")
	assert.NoEmptyStr(dbUrl, "Missing DB_URL")
	db, err := sqlx.Connect("postgres", dbUrl)
	assert.NoErr(err, "unable to open db", "dbPath", dbUrl)

	switch {
	case fetch:
		assert.NoEmptyStr(walletAddress, "wallet-address needs to be provided for fetch")

		rpcUrl := os.Getenv("RPC_URL")
		assert.NoEmptyStr(rpcUrl, "Missing RPC_URL")

		slog.Info("Fetching transactions for wallet", "walletAddress", walletAddress)

		rpc := rpc.NewClientWithTimer(rpcUrl, 100*time.Millisecond)
		fetchTransactions(db, rpc, walletAddress, signature)
	case print:
		slog.Info("Printing unknown instructions")
		printUnknownInstructions(db)
	case parseIxs:
		slog.Info("Parsing isntructions")
		parseInstructions(
			db,
			signature,
			programAddress,
			discriminator,
			walletAddress,
			ignorePrograms,
			save,
		)
	case parseEvents:
	}
}
