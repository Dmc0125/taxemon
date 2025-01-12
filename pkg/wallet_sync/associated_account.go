package walletsync

import (
	"encoding/json"
	"slices"
	"taxemon/pkg/assert"
	dbutils "taxemon/pkg/db_utils"

	"github.com/jmoiron/sqlx"
	"github.com/mr-tron/base58"
)

type iAssociatedAccount interface {
	Address() string
	Type() dbutils.AssociatedAccountType
	Data() ([]byte, error)
	ShouldFetch() bool
}

type AssociatedAccounts struct {
	CurrentIter    map[string]iAssociatedAccount
	all            map[string]iAssociatedAccount
	lastSignatures map[string]string
}

func NewAssociatedAccounts() *AssociatedAccounts {
	return &AssociatedAccounts{
		CurrentIter:    make(map[string]iAssociatedAccount),
		all:            make(map[string]iAssociatedAccount),
		lastSignatures: make(map[string]string),
	}
}

func (a *AssociatedAccounts) Append(account iAssociatedAccount) {
	a.CurrentIter[account.Address()] = account
}

func (a *AssociatedAccounts) Contains(address string) bool {
	_, ok := a.CurrentIter[address]
	if !ok {
		_, ok = a.all[address]
	}
	return ok
}

func (a *AssociatedAccounts) Get(address string) iAssociatedAccount {
	account, ok := a.CurrentIter[address]
	if !ok {
		account, ok := a.all[address]
		if !ok {
			return nil
		}
		return account
	}
	return account
}

func (a *AssociatedAccounts) FetchExisting(db *sqlx.DB, walletId int32) {
	associatedAccounts, err := dbutils.SelectAssociatedAccounts(db, walletId)
	assert.NoErr(err, "unable to select associated accounts")
	for _, account := range associatedAccounts {
		switch account.Type {
		case dbutils.AssociatedAccountToken:
			a.all[account.Address] = newAssociatedAccountToken(account)
			a.lastSignatures[account.Address] = account.LastSignature.String
		case dbutils.AssociatedAccountJupLimit:
			a.all[account.Address] = newAssociatedAccountJupLimit(account)
			a.lastSignatures[account.Address] = account.LastSignature.String
		}
	}
}

func (a *AssociatedAccounts) Flush() []iAssociatedAccount {
	if len(a.CurrentIter) == 0 {
		return nil
	}

	new := make([]iAssociatedAccount, 0)
	for address, account := range a.CurrentIter {
		_, ok := a.all[address]
		if !ok {
			new = append(new, account)
			a.all[address] = account
			a.lastSignatures[address] = ""
		}
	}
	a.CurrentIter = make(map[string]iAssociatedAccount)
	return new
}

func (associatedAccounts *AssociatedAccounts) ParseTx(
	tx *SavedTransaction,
	walletAddress string,
) error {
	for _, ix := range tx.Instructions {
		for _, p := range ixParsers {
			if p.parseAssociatedAccounts == nil {
				continue
			}
			if p.programAddress == ix.ProgramAddress {
				ctx := &parseIxContext{
					signature:          tx.Signature,
					walletAddress:      walletAddress,
					associatedAccounts: associatedAccounts,
					ix:                 ix,
					logs:               tx.Logs,
				}
				if err := p.parseAssociatedAccounts(ctx); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

type AssociatedAccountToken struct {
	address     string
	mint        string
	shouldFetch bool
}

var _ iAssociatedAccount = (*AssociatedAccountToken)(nil)

func newAssociatedAccountToken(account *dbutils.SelectAssociatedAccountsRow) *AssociatedAccountToken {
	data := make(map[string]string)
	if account.Data.Valid {
		err := json.Unmarshal([]byte(account.Data.String), &data)
		assert.NoErr(err, "unable to unmarshal associated account token data")
	}
	return &AssociatedAccountToken{
		address:     account.Address,
		mint:        data["mint"],
		shouldFetch: account.ShouldFetch,
	}
}
func (account *AssociatedAccountToken) Address() string {
	return account.address
}

func (account *AssociatedAccountToken) Type() dbutils.AssociatedAccountType {
	return dbutils.AssociatedAccountToken
}

func (account *AssociatedAccountToken) Data() ([]byte, error) {
	d := map[string]interface{}{
		"mint": account.mint,
	}
	return json.Marshal(d)
}

func (account *AssociatedAccountToken) ShouldFetch() bool {
	return account.shouldFetch
}

func parseTokenIxAssociatedAccounts(ctx *parseIxContext) error {
	ix := ctx.ix
	if len(ix.Data) < 1 {
		return ctx.errMissingDiscriminator()
	}

	var (
		owner, newAccount, mint string
	)

	switch ix.Data[0] {
	case ixTokenInitAccount3:
		if len(ix.Data) < 33 {
			return ctx.errInvalidData()
		}
		owner = base58.Encode(ix.Data[1:])

		if len(ix.Accounts) < 2 {
			return ctx.errMissingAccounts()
		}
		mint = ix.Accounts[1]
		owner = ix.Accounts[2]
	case ixTokenInitAccount2:
		if len(ix.Data) < 33 {
			return ctx.errInvalidData()
		}
		owner = base58.Encode(ix.Data[1:])

		if len(ix.Accounts) < 2 {
			return ctx.errMissingAccounts()
		}
		mint = ix.Accounts[1]
		owner = ix.Accounts[2]
	case ixTokenInitAccount:
		if len(ix.Accounts) < 3 {
			return ctx.errMissingAccounts()
		}
		newAccount = ix.Accounts[0]
		mint = ix.Accounts[1]
		owner = ix.Accounts[2]
	default:
		return nil
	}

	if owner == ctx.walletAddress {
		ctx.associatedAccounts.Append(&AssociatedAccountToken{
			address:     newAccount,
			mint:        mint,
			shouldFetch: true,
		})
	}

	return nil
}

func parseAssociatedTokenIxAssociatedAccounts(ctx *parseIxContext) error {
	ix := ctx.ix
	isCreate := len(ix.Data) == 0 || ix.Data[0] == 0 || ix.Data[0] == 1

	if isCreate {
		if len(ix.Accounts) < 4 {
			return ctx.errMissingAccounts()
		}

		owner := ix.Accounts[2]
		if owner == ctx.walletAddress {
			ctx.associatedAccounts.Append(&AssociatedAccountToken{
				address:     ix.Accounts[1],
				mint:        ix.Accounts[3],
				shouldFetch: true,
			})
			return nil
		}
	}

	return nil
}

type AssociatedAccountJupLimit struct {
	address    string
	amountIn   uint64
	accountIn  string
	accountOut string
}

var _ iAssociatedAccount = (*AssociatedAccountJupLimit)(nil)

func newAssociatedAccountJupLimit(account *dbutils.SelectAssociatedAccountsRow) *AssociatedAccountJupLimit {
	data := make(map[string]string)
	if account.Data.Valid {
		err := json.Unmarshal([]byte(account.Data.String), &data)
		assert.NoErr(err, "unable to unmarshal associated account jup limit data")
	}
	return &AssociatedAccountJupLimit{
		address:    account.Address,
		accountIn:  data["accountIn"],
		accountOut: data["accountOut"],
	}
}

func (account *AssociatedAccountJupLimit) Type() dbutils.AssociatedAccountType {
	return dbutils.AssociatedAccountJupLimit
}

func (account *AssociatedAccountJupLimit) Address() string {
	return account.address
}

func (account *AssociatedAccountJupLimit) Data() ([]byte, error) {
	data := map[string]string{
		"accountIn":  account.accountIn,
		"accountOut": account.accountOut,
	}
	return json.Marshal(data)
}

func (account *AssociatedAccountJupLimit) ShouldFetch() bool {
	return false
}

func parseJupLimitIxAssociatedAccounts(ctx *parseIxContext) error {
	ix := ctx.ix
	if len(ix.Data) < 8 {
		return ctx.errMissingDiscriminator()
	}
	disc := ix.Data[:8]

	if slices.Equal(disc, IxJupLimitInitOrder) {
		maker := ix.Accounts[1]
		if maker != ctx.walletAddress {
			return nil
		}

		if len(ix.innerInstructions) == 4 {
			orderAccount := ix.Accounts[2]
			accountIn := ix.Accounts[3]
			accountOut := ix.Accounts[6]
			ctx.associatedAccounts.Append(&AssociatedAccountJupLimit{
				address:    orderAccount,
				accountIn:  accountIn,
				accountOut: accountOut,
			})
		}

		if len(ix.innerInstructions) != 1 {
			tokenAccount := ix.Accounts[3]
			mint := ix.Accounts[5]
			ctx.associatedAccounts.Append(&AssociatedAccountToken{
				address:     tokenAccount,
				mint:        mint,
				shouldFetch: false,
			})
		}
	}

	return nil
}
