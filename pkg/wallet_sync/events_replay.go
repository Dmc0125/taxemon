package walletsync

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"taxemon/pkg/assert"
	dbutils "taxemon/pkg/db_utils"
)

// mint -> balance
type tAccountBalances map[string]uint64

type tAccounts map[string]tAccountBalances

func (accounts tAccounts) balance(accountAddress, mintAddress string) (uint64, error) {
	if account, ok := accounts[accountAddress]; ok {
		if balance, ok := account[mintAddress]; ok {
			return balance, nil
		}
		return 0, errors.New("mint balance does not exist")
	}
	return 0, errors.New("account does not exist")
}

func (accounts tAccounts) withdraw(accountAddress, mintAddress string, amount uint64, signature string, ixIdx int32) {
	accountBalances, ok := accounts[accountAddress]
	if !ok {
		return
	}
	if balance, ok := accountBalances[mintAddress]; ok {
		assert.True(
			balance >= amount,
			"balance is smaller than withdrawn amount",
			"account", accountAddress,
			"balance", balance,
			"amount", amount,
			"signature", signature,
			"ixIdx", ixIdx,
		)
		accountBalances[mintAddress] -= amount
	}
	slog.Info(
		"withdraw",
		"account", accountAddress,
		"mint", mintAddress,
		"amount", amount,
		"newBalance", accountBalances[mintAddress],
		"signature", signature,
	)
}

func (accounts tAccounts) deposit(accountAddress, mintAddress string, amount uint64, signature string) {
	accountBalances, ok := accounts[accountAddress]
	if !ok {
		accountBalances = make(map[string]uint64)
		accounts[accountAddress] = accountBalances
	}
	if _, ok := accountBalances[mintAddress]; ok {
		accountBalances[mintAddress] += amount
	} else {
		accountBalances[mintAddress] = amount
	}
	slog.Info(
		"deposit",
		"account", accountAddress,
		"mint", mintAddress,
		"amount", amount,
		"newBalance", accountBalances[mintAddress],
		"signature", signature,
	)
}

type tReplay struct {
	walletAddresses    []string
	associatedAccounts *AssociatedAccounts
	accounts           tAccounts
}

func NewReplay(db dbutils.DBTX) *tReplay {
	wallets, err := dbutils.SelectWallets(db)
	assert.NoErr(err, "unable to select addresses")

	addresses := make([]string, len(wallets))
	associatedAccounts := NewAssociatedAccounts()

	for i, wallet := range wallets {
		addresses[i] = wallet.Address
		associatedAccounts.FetchExisting(db, wallet.Id)
	}

	return &tReplay{
		walletAddresses:    addresses,
		associatedAccounts: associatedAccounts,
		accounts:           tAccounts(make(map[string]tAccountBalances)),
	}
}

func (r *tReplay) AccountsString() string {
	out := strings.Builder{}
	for accountAddress, balances := range r.accounts {
		out.WriteString(accountAddress)
		out.WriteString("\n")
		for mintAddress, balance := range balances {
			out.WriteString(fmt.Sprintf("\t%s: %d\n", mintAddress, balance))
		}
	}
	return out.String()
}

const solanaMintAddress = "So11111111111111111111111111111111111111112"

var (
	errMsgMissingAccount  = "missing associated account"
	errMsgNotTokenAccount = "associated account is not token account"
)

func (r *tReplay) processSwapTransfers(from []*SwapToken, to []*SwapToken, signature string, ixIdx int32) {
	for _, tokenData := range from {
		account := r.associatedAccounts.Get(tokenData.Account)
		assert.True(account != nil, errMsgMissingAccount, "address", tokenData.Account)
		assert.True(account.Type() == dbutils.AssociatedAccountToken, errMsgNotTokenAccount)

		tokenAccountData := account.(*AssociatedAccountToken)
		r.accounts.withdraw(tokenData.Account, tokenAccountData.mint, tokenData.Amount, signature, ixIdx)
	}

	for _, tokenData := range to {
		account := r.associatedAccounts.Get(tokenData.Account)
		assert.True(account != nil, errMsgMissingAccount, "address", tokenData.Account)
		assert.True(account.Type() == dbutils.AssociatedAccountToken, errMsgNotTokenAccount)

		tokenAccountData := account.(*AssociatedAccountToken)
		r.accounts.deposit(tokenData.Account, tokenAccountData.mint, tokenData.Amount, signature)
	}
}

func (r *tReplay) ProcessEvent(event *dbutils.SelectEventsRow) {
	switch event.Type {
	case dbutils.EventTypeNativeTransfer:
		data := new(EventNativeTransfer)
		err := json.Unmarshal([]byte(event.Data), data)
		assert.NoErr(err, "unable to unmarshal native transfer event data", "data", event.Data)

		if slices.Contains(r.walletAddresses, data.From) {
			r.accounts.withdraw(data.From, solanaMintAddress, data.Amount, event.Signature, event.IxIdx)
		}
		if slices.Contains(r.walletAddresses, data.To) || r.associatedAccounts.Contains(data.To) {
			r.accounts.deposit(data.To, solanaMintAddress, data.Amount, event.Signature)
		}
	case dbutils.EventTypeTransfer:
		data := new(EventTransfer)
		err := json.Unmarshal([]byte(event.Data), data)
		assert.NoErr(err, "unable to unmarshal transfer event data", "data", event.Data)

		if account := r.associatedAccounts.Get(data.From); account != nil {
			assert.True(account.Type() == dbutils.AssociatedAccountToken, errMsgNotTokenAccount)
			tokenAccountData := account.(*AssociatedAccountToken)
			r.accounts.withdraw(data.From, tokenAccountData.mint, data.Amount, event.Signature, event.IxIdx)
		}

		if account := r.associatedAccounts.Get(data.To); account != nil {
			assert.True(account.Type() == dbutils.AssociatedAccountToken, errMsgNotTokenAccount)
			tokenAccountData := account.(*AssociatedAccountToken)
			r.accounts.deposit(data.To, tokenAccountData.mint, data.Amount, event.Signature)
		}
	case dbutils.EventTypeMint:
		data := new(EventMint)
		err := json.Unmarshal([]byte(event.Data), &data)
		assert.NoErr(err, "unable to unmarshal mint event data")

		account := r.associatedAccounts.Get(data.To)
		assert.True(account != nil, errMsgMissingAccount, "address", data.To)
		// TODO:
		// maybe not all accounts with mint / burn are token accounts ?
		assert.True(account.Type() == dbutils.AssociatedAccountToken, errMsgNotTokenAccount)

		tokenAccountData := account.(*AssociatedAccountToken)
		r.accounts.deposit(data.To, tokenAccountData.mint, data.Amount, event.Signature)
	case dbutils.EventTypeBurn:
		data := new(EventBurn)
		err := json.Unmarshal([]byte(event.Data), &data)
		assert.NoErr(err, "unable to unmarshal burn event data")

		account := r.associatedAccounts.Get(data.From)
		assert.True(account != nil, errMsgMissingAccount, "address", data.From)
		// TODO:
		// maybe not all accounts with mint / burn are token accounts ?
		assert.True(account.Type() == dbutils.AssociatedAccountToken, errMsgNotTokenAccount)

		tokenAccountData := account.(*AssociatedAccountToken)
		r.accounts.withdraw(data.From, tokenAccountData.mint, data.Amount, event.Signature, event.IxIdx)
	case dbutils.EventTypeCloseAccount:
		data := new(EventCloseAccount)
		err := json.Unmarshal([]byte(event.Data), &data)
		assert.NoErr(err, "unable to unmarshal close account event data")

		if !r.associatedAccounts.Contains(data.From) {
			// TODO: somehow this needs to be handled
			// if the closed account is not owned by the wallet
			// we don't know what balance does it have
			return
		}
		accountBalance, err := r.accounts.balance(data.From, solanaMintAddress)
		assert.NoErr(err, "missing account balance", "address", data.From, "mint", solanaMintAddress, "signature", event.Signature)
		r.accounts.withdraw(data.From, solanaMintAddress, accountBalance, event.Signature, event.IxIdx)

		if slices.Contains(r.walletAddresses, data.To) {
			r.accounts.deposit(data.To, solanaMintAddress, accountBalance, event.Signature)
		}
	case dbutils.EventTypeSwap:
		data := new(EventSwap)
		err := json.Unmarshal([]byte(event.Data), &data)
		assert.NoErr(err, "unable to unmarshal swap event data")

		r.processSwapTransfers(data.From, data.To, event.Signature, event.IxIdx)
	case dbutils.EventTypeMintCNft:

	case dbutils.EventTypeUnstakeLiquid:
		data := new(EventUnstakeLiquid)
		err := json.Unmarshal([]byte(event.Data), data)
		assert.NoErr(err, "unbale to unmarshal unstake event data")

		r.processSwapTransfers(data.From, data.To, event.Signature, event.IxIdx)
	}
}
