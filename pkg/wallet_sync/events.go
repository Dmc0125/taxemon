package walletsync

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	dbutils "taxemon/pkg/db_utils"
)

const (
	ComputeBudgetProgramAddress        = "ComputeBudget111111111111111111111111111111"
	SystemProgramAddress               = "11111111111111111111111111111111"
	TokenProgramAddress                = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	Token2022ProgramAddress            = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
	AssociatedTokenProgramAddress      = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
	JupV6ProgramAddress                = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
	JupV4ProgramAddress                = "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB"
	JupLimitProgramAddress             = "jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu"
	JupMerkleDistributorProgramAddress = "meRjbQXFNf5En86FXT2YPz1dQzLj4Yb3xK8u1MVgqpb"
	BubblegumProgramAddress            = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY"
	AtlasStakeProgramAddress           = "63LGG86NVj87f3mkbWoqQWEd4RLNRp6a54E1qMbK3Ls4"
	// TODO
	JupLimit2ProgramAddress = "j1o2qRpjcyUwEvwtcfhEQefh773ZgjxcVRry7LDqg5X"
)

type EventData interface {
	Type() dbutils.EventType
}

type parseIxContext struct {
	signature          string
	walletAddress      string
	associatedAccounts *AssociatedAccounts

	ix   *SavedInstruction
	logs []string
}

func (ctx *parseIxContext) errMissingDiscriminator() error {
	return fmt.Errorf("ix missing discriminator: program %s signature %s", ctx.ix.ProgramAddress, ctx.signature)
}

func (ctx *parseIxContext) errInvalidData() error {
	return fmt.Errorf("ix invalid data: idx %d program %s signature %s", ctx.ix.Idx, ctx.ix.ProgramAddress, ctx.signature)
}

func (ctx *parseIxContext) errMissingAccounts() error {
	return fmt.Errorf("ix accounts missing: idx %d program %s signature %s", ctx.ix.Idx, ctx.ix.ProgramAddress, ctx.signature)
}

type EventTransfer struct {
	ProgramAddress string `json:"program_address"`
	IsRent         bool   `json:"is_rent"`
	From           string `json:"from"`
	To             string `json:"to"`
	Amount         uint64 `json:"amount"`
}

var _ EventData = (*EventTransfer)(nil)

func (e *EventTransfer) Type() dbutils.EventType {
	return dbutils.EventTypeTransfer
}

const (
	ixSystemCreateAccount        = 0
	ixSystemTransfer             = 2
	ixSystemCreateWithSeed       = 3
	ixSystemWithdrawNonceAccount = 5
	ixSystemTransferWithSeed     = 11
)

// ctx needs:
//   - walletAdress
//   - signature
//   - ix
func parseSystemIx(ctx *parseIxContext) (EventData, error) {
	ix := ctx.ix
	if len(ix.Data) < 1 {
		return nil, ctx.errMissingDiscriminator()
	}
	data := ix.Data[1:]

	switch ix.Data[0] {
	case ixSystemCreateAccount:
		if len(ix.Accounts) < 2 {
			return nil, ctx.errMissingAccounts()
		}

		from := ix.Accounts[0]
		to := ix.Accounts[1]

		if from != ctx.walletAddress && to != ctx.walletAddress {
			return nil, nil
		}

		if len(data) < 8 {
			return nil, ctx.errInvalidData()
		}
		lamports := binary.LittleEndian.Uint64(data)

		event := &EventTransfer{
			IsRent: true,
			From:   from,
			To:     to,
			Amount: lamports,
		}
		return event, nil
	case ixSystemWithdrawNonceAccount:
		fallthrough
	case ixSystemTransfer:
		if len(ix.Accounts) < 2 {
			return nil, ctx.errMissingAccounts()
		}

		from := ix.Accounts[0]
		to := ix.Accounts[1]

		if from != ctx.walletAddress && to != ctx.walletAddress {
			return nil, nil
		}

		if len(data) < 8 {
			return nil, ctx.errInvalidData()
		}
		lamports := binary.LittleEndian.Uint64(data)

		event := &EventTransfer{
			IsRent: false,
			From:   from,
			To:     to,
			Amount: lamports,
		}
		return event, nil
	case ixSystemCreateWithSeed:
		if len(ix.Accounts) < 2 {
			return nil, ctx.errMissingAccounts()
		}
		from := ix.Accounts[0]
		to := ix.Accounts[1]

		if from != ctx.walletAddress && to != ctx.walletAddress {
			return nil, nil
		}

		if len(data) < 40 {
			return nil, ctx.errInvalidData()
		}
		seedLen := binary.LittleEndian.Uint32(data[32:])
		seedPadding := binary.LittleEndian.Uint32(data[36:])
		if len(data) < 40+int(seedLen+seedPadding) {
			return nil, ctx.errInvalidData()
		}
		lamports := binary.LittleEndian.Uint64(data[40+seedLen+seedPadding:])

		event := &EventTransfer{
			IsRent: true,
			From:   from,
			To:     to,
			Amount: lamports,
		}
		return event, nil
	case ixSystemTransferWithSeed:
		if len(ix.Accounts) < 3 {
			return nil, ctx.errMissingAccounts()
		}
		from := ix.Accounts[0]
		to := ix.Accounts[2]

		if from != ctx.walletAddress && to != ctx.walletAddress {
			return nil, nil
		}

		if len(data) < 8 {
			return nil, ctx.errInvalidData()
		}
		lamports := binary.LittleEndian.Uint64(data)

		event := &EventTransfer{
			IsRent: false,
			From:   from,
			To:     to,
			Amount: lamports,
		}
		return event, nil
	default:
		return nil, nil
	}
}

const (
	ixTokenTransfer        = 3
	ixTokenTransferChecked = 12
	ixTokenMintTo          = 7
	ixTokenMintToChecked   = 14
	ixTokenBurn            = 8
	ixTokenBurnChecked     = 15
	ixTokenCloseAccount    = 9

	ixTokenInitAccount  = 1
	ixTokenInitAccount2 = 16
	ixTokenInitAccount3 = 18
)

type EventMint struct {
	ProgramAddress string `json:"program_address"`
	To             string `json:"to"`
	Amount         uint64 `json:"amount"`
}

var _ EventData = (*EventMint)(nil)

func (e *EventMint) Type() dbutils.EventType {
	return dbutils.EventTypeMint
}

type EventBurn struct {
	ProgramAddress string `json:"program_address"`
	From           string `json:"from"`
	Amount         uint64 `json:"amount"`
}

var _ EventData = (*EventBurn)(nil)

func (e *EventBurn) Type() dbutils.EventType {
	return dbutils.EventTypeBurn
}

type EventCloseAccount struct {
	ProgramAddress string `json:"program_address"`
	From           string `json:"from"`
	To             string `json:"to"`
}

var _ EventData = (*EventCloseAccount)(nil)

func (e *EventCloseAccount) Type() dbutils.EventType {
	return dbutils.EventTypeCloseAccount
}

// ctx needs:
//   - signature
//   - ix
//   - associated accounts
func parseTokenIx(ctx *parseIxContext) (EventData, error) {
	ix := ctx.ix
	if len(ix.Data) < 1 {
		return nil, ctx.errMissingDiscriminator()
	}
	data := ix.Data[1:]

	switch ix.Data[0] {
	case ixTokenTransfer:
		if len(ix.Accounts) < 2 {
			return nil, ctx.errMissingAccounts()
		}
		from := ix.Accounts[0]
		to := ix.Accounts[1]

		if !ctx.associatedAccounts.Contains(from) && !ctx.associatedAccounts.Contains(to) {
			return nil, nil
		}

		if len(data) < 8 {
			return nil, ctx.errInvalidData()
		}

		amount := binary.LittleEndian.Uint64(data)
		event := &EventTransfer{
			ProgramAddress: TokenProgramAddress,
			IsRent:         false,
			From:           from,
			To:             to,
			Amount:         amount,
		}
		return event, nil
	case ixTokenTransferChecked:
		if len(ix.Accounts) < 3 {
			return nil, ctx.errMissingAccounts()
		}
		from := ix.Accounts[0]
		to := ix.Accounts[2]

		if !ctx.associatedAccounts.Contains(from) && !ctx.associatedAccounts.Contains(to) {
			return nil, nil
		}

		if len(data) < 8 {
			return nil, ctx.errInvalidData()
		}
		amount := binary.LittleEndian.Uint64(data)
		event := &EventTransfer{
			ProgramAddress: TokenProgramAddress,
			IsRent:         false,
			From:           from,
			To:             to,
			Amount:         amount,
		}
		return event, nil
	case ixTokenMintToChecked:
		fallthrough
	case ixTokenMintTo:
		if len(ix.Accounts) < 2 {
			return nil, ctx.errMissingAccounts()
		}
		to := ix.Accounts[1]

		if !ctx.associatedAccounts.Contains(to) {
			return nil, nil
		}

		if len(data) < 8 {
			return nil, ctx.errInvalidData()
		}
		amount := binary.LittleEndian.Uint64(data)
		event := &EventMint{
			ProgramAddress: TokenProgramAddress,
			To:             to,
			Amount:         amount,
		}
		return event, nil
	case ixTokenBurnChecked:
		fallthrough
	case ixTokenBurn:
		if len(ix.Accounts) < 1 {
			return nil, ctx.errMissingAccounts()
		}
		from := ix.Accounts[0]
		if !ctx.associatedAccounts.Contains(from) {
			return nil, nil
		}
		if len(data) < 8 {
			return nil, ctx.errInvalidData()
		}
		amount := binary.LittleEndian.Uint64(data)
		event := &EventBurn{
			ProgramAddress: TokenProgramAddress,
			From:           from,
			Amount:         amount,
		}
		return event, nil
	case ixTokenCloseAccount:
		if len(ix.Accounts) < 2 {
			return nil, ctx.errMissingAccounts()
		}
		from := ix.Accounts[0]
		to := ix.Accounts[1]

		if !ctx.associatedAccounts.Contains(from) && ctx.walletAddress != to {
			return nil, nil
		}

		event := &EventCloseAccount{
			ProgramAddress: TokenProgramAddress,
			From:           from,
			To:             to,
		}
		return event, nil
	default:
		return nil, nil
	}
}

// ctx needs:
//   - signature
//   - ix
//   - walletAddress
func parseAssociatedTokenIx(ctx *parseIxContext) (EventData, error) {
	ix := ctx.ix
	isCreate := len(ix.Data) == 0 || ix.Data[0] == 0 || ix.Data[0] == 1

	if isCreate {
		// CREATE || CREATE IDEMPOTENT
		innerIxsLen := len(ix.innerInstructions)

		if innerIxsLen == 0 {
			return nil, nil
		}

		if len(ix.Accounts) < 3 {
			return nil, ctx.errMissingAccounts()
		}

		from := ix.Accounts[0]
		to := ix.Accounts[1]

		if from != ctx.walletAddress {
			return nil, nil
		}

		if innerIxsLen == 4 || innerIxsLen == 6 {
			// create from zero lamports || create with lamports
			// rent index = 1
			createAccountIx := ix.innerInstructions[1]
			lamports := binary.LittleEndian.Uint64(createAccountIx.Data[4:])

			event := &EventTransfer{
				ProgramAddress: AssociatedTokenProgramAddress,
				From:           from,
				To:             to,
				Amount:         lamports,
				IsRent:         true,
			}
			return event, nil
		}
	} else {
		// RECOVER NESTED
		slog.Error("unimplemented associated token instruction (recover nested)", "signature", ctx.signature)
	}

	return nil, nil
}

var (
	IxJupV6Route, _               = hex.DecodeString("e517cb977ae3ad2a")
	IxJupV6SharedAccountsRoute, _ = hex.DecodeString("c1209b3341d69c81")
	IxJupV6Log, _                 = hex.DecodeString("e445a52e51cb9a1d")

	evJupV6Swap, _ = hex.DecodeString("40c6cde8260871e2")
)

type SwapToken struct {
	Amount  uint64 `json:"amount"`
	Account string `json:"account"`
}

type EventSwap struct {
	ProgramAddress string `json:"program_address"`
	// tokens sent away
	From []*SwapToken `json:"from"`
	// tokens received
	To []*SwapToken `json:"to"`
}

var _ EventData = (*EventSwap)(nil)

func (e *EventSwap) Type() dbutils.EventType {
	return dbutils.EventTypeSwap
}

func (e *EventSwap) String() string {
	b := strings.Builder{}
	b.WriteString("FROM:")
	for _, from := range e.From {
		b.WriteString(fmt.Sprintf("\n\t%s: %d", from.Account, from.Amount))
	}
	b.WriteString("\nTO:")
	for _, to := range e.To {
		b.WriteString(fmt.Sprintf("\n\t%s: %d", to.Account, to.Amount))
	}
	return b.String()
}

func parseJupIxIntoSwapEvent(
	innerIxs []*SavedInstructionBase,
	associatedAccounts *AssociatedAccounts,
	signature string,
) *EventSwap {
	amounts := make(map[string]int64)

	for _, iix := range innerIxs {
		if iix.ProgramAddress != TokenProgramAddress {
			continue
		}

		var (
			from, to string
		)

		switch iix.Data[0] {
		case ixTokenTransfer:
			from = iix.Accounts[0]
			to = iix.Accounts[1]
		case ixTokenTransferChecked:
			from = iix.Accounts[0]
			to = iix.Accounts[2]
		default:
			continue
		}

		var amount int64
		var key string

		if associatedAccounts.Contains(from) {
			amount = -int64(binary.LittleEndian.Uint64(iix.Data[1:]))
			key = from
		} else if associatedAccounts.Contains(to) {
			amount = int64(binary.LittleEndian.Uint64(iix.Data[1:]))
			key = to
		} else {
			continue
		}

		if _, ok := amounts[key]; !ok {
			amounts[key] = amount
		} else {
			amounts[key] += amount
		}
	}

	tokensIn := make([]*SwapToken, 0)
	tokensOut := make([]*SwapToken, 0)

	for account, amount := range amounts {
		if amount > 0 {
			tokensIn = append(tokensIn, &SwapToken{
				Amount:  uint64(amount),
				Account: account,
			})
		} else if amount < 0 {
			tokensOut = append(tokensOut, &SwapToken{
				Amount:  uint64(-amount),
				Account: account,
			})
		}
	}

	if (len(tokensIn) != 0 && len(tokensOut) == 0) || (len(tokensIn) == 0 && len(tokensOut) != 0) {
		slog.Warn("jupiter v6 swap with another wallet", "signature", signature)
	}
	if len(tokensIn) == 0 || len(tokensOut) == 0 {
		return nil
	}

	return &EventSwap{
		From: tokensOut,
		To:   tokensIn,
	}
}

// ctx needs:
//   - signature
//   - ix
//   - walletAddress
//   - associatedAccounts
func parseJupV6Ix(ctx *parseIxContext) (EventData, error) {
	ix := ctx.ix
	if len(ix.Data) < 8 {
		return nil, ctx.errMissingDiscriminator()
	}
	disc := ix.Data[0:8]
	shouldParse := false

	if slices.Equal(disc, IxJupV6SharedAccountsRoute) {
		shouldParse = ix.Accounts[2] == ctx.walletAddress
	} else if slices.Equal(disc, IxJupV6Route) {
		shouldParse = ix.Accounts[1] == ctx.walletAddress
	} else {
		slog.Warn("unhandled ix: unknown", "signature", ctx.signature)
	}

	if shouldParse {
		swapEvent := parseJupIxIntoSwapEvent(ix.innerInstructions, ctx.associatedAccounts, ctx.signature)
		if swapEvent != nil {
			swapEvent.ProgramAddress = JupV6ProgramAddress
			return swapEvent, nil
		}
	}

	return nil, nil
}

// ctx needs:
//   - signature
//   - ix
//   - walletAddress
//   - associatedAccounts
func parseJupV4Ix(ctx *parseIxContext) (EventData, error) {
	ix := ctx.ix
	if len(ix.Data) < 8 {
		return nil, ctx.errMissingDiscriminator()
	}
	disc := ix.Data[:8]

	if slices.Equal(disc, IxJupV6Route) {
		authority := ix.Accounts[1]

		if authority != ctx.walletAddress {
			return nil, nil
		}

		swapEvent := parseJupIxIntoSwapEvent(ix.innerInstructions, ctx.associatedAccounts, ctx.signature)
		if swapEvent != nil {
			swapEvent.ProgramAddress = JupV4ProgramAddress
			return swapEvent, nil
		}
	} else {
		slog.Warn("unhandled ix: unknown", "signature", ctx.signature)
	}

	return nil, nil
}

var (
	IxJupLimitInitOrder, _         = hex.DecodeString("856e4aaf709ff59f")
	IxJupLimitFlashFillOrder, _    = hex.DecodeString("fc681286a44e128c")
	IxJupLimitPreFlashFillOrder, _ = hex.DecodeString("f02f99440dbee12a")
)

// if order account does not exist
// 		count is 4 (create oa and ta)
// if order account exsists
// 		count is 3 (create ta if it does not exist)

// ctx needs:
//   - signature
//   - ix
//   - walletAddress
//   - associatedAccounts
func parseJupLimitIx(ctx *parseIxContext) ([]EventData, error) {
	ix := ctx.ix
	if len(ix.Data) < 8 {
		return nil, ctx.errMissingDiscriminator()
	}

	disc := ix.Data[:8]
	if slices.Equal(disc, IxJupLimitInitOrder) {
		maker := ix.Accounts[1]
		if maker != ctx.walletAddress {
			return nil, nil
		}

		events := make([]EventData, 0)

		if len(ix.innerInstructions) == 4 {
			iix := ix.innerInstructions[0]
			amount := binary.LittleEndian.Uint64(iix.Data[1:])
			from := iix.Accounts[0]
			to := iix.Accounts[1]

			events = append(events, &EventTransfer{
				ProgramAddress: JupLimitProgramAddress,
				IsRent:         true,
				From:           from,
				To:             to,
				Amount:         amount,
			})
		}

		if len(ix.innerInstructions) != 1 {
			iix := ix.innerInstructions[1]
			amount := binary.LittleEndian.Uint64(iix.Data[1:])
			from := iix.Accounts[0]
			to := iix.Accounts[1]

			events = append(events, &EventTransfer{
				ProgramAddress: JupLimitProgramAddress,
				IsRent:         true,
				From:           from,
				To:             to,
				Amount:         amount,
			})
		}

		transferIix := ix.innerInstructions[len(ix.innerInstructions)-1]
		amount := binary.LittleEndian.Uint64(transferIix.Data)

		events = append(events, &EventTransfer{
			ProgramAddress: JupLimitProgramAddress,
			IsRent:         false,
			From:           transferIix.Accounts[0],
			To:             transferIix.Accounts[2],
			Amount:         amount,
		})

		return events, nil
	} else if slices.Equal(disc, IxJupLimitPreFlashFillOrder) {
		orderAccountAddress := ix.Accounts[0]

		oa := ctx.associatedAccounts.Get(orderAccountAddress)
		if oa != nil {
			orderAccount := oa.(*AssociatedAccountJupLimit)
			orderAccount.amountIn = binary.LittleEndian.Uint64(ix.Data[8:])
		}

		return nil, nil
	} else if slices.Equal(disc, IxJupLimitFlashFillOrder) {
		orderAddress := ix.Accounts[0]

		if oa := ctx.associatedAccounts.Get(orderAddress); oa != nil {
			orderAccount := oa.(*AssociatedAccountJupLimit)

			transferIx := ix.innerInstructions[0]
			// feeIx := ix.innerInstructions[1]
			amountOut := binary.LittleEndian.Uint64(transferIx.Data[1:])

			events := []EventData{&EventSwap{
				ProgramAddress: JupLimitProgramAddress,
				From: []*SwapToken{{
					Amount:  orderAccount.amountIn,
					Account: orderAccount.accountIn,
				}},
				To: []*SwapToken{{
					Amount:  amountOut,
					Account: orderAccount.accountOut,
				}},
			}}

			lastIix := ix.innerInstructions[len(ix.innerInstructions)-1]

			if lastIix.ProgramAddress == TokenProgramAddress && lastIix.Data[0] == ixTokenCloseAccount {
				walletAddress := ix.Accounts[2]
				events = append(
					events,
					&EventCloseAccount{
						ProgramAddress: JupLimitProgramAddress,
						From:           ix.Accounts[1],
						To:             walletAddress,
					},
					&EventCloseAccount{
						ProgramAddress: JupLimitProgramAddress,
						From:           orderAddress,
						To:             walletAddress,
					},
				)
			}

			return events, nil
		}

		return nil, nil
	} else {
		slog.Warn("unknown jup limit instruction", "signature", ctx.signature)
		return nil, nil
	}
}

var (
	ixMerkleDistributorClaim, _  = hex.DecodeString("4eb1627bd215bb53")
	IxMerkleCloseClaimAccount, _ = hex.DecodeString("a3d6bfa5f5bc11b9")
)

// ctx needs:
//   - signature
//   - ix
//   - walletAddress
//   - associatedAccounts
func parseMerkleDistributorIx(ctx *parseIxContext) ([]EventData, error) {
	ix := ctx.ix
	if len(ix.Data) < 8 {
		return nil, ctx.errMissingDiscriminator()
	}
	disc := ix.Data[:8]

	if slices.Equal(disc, ixMerkleDistributorClaim) {
		claimaint := ix.Accounts[4]

		if claimaint == ctx.walletAddress {
			if len(ix.innerInstructions) != 2 {
				// error
			}

			createAccountIx := ix.innerInstructions[0]
			createAccountEvent, err := parseSystemIx(&parseIxContext{
				walletAddress: ctx.walletAddress,
				signature:     ctx.signature,
				ix: &SavedInstruction{
					SavedInstructionBase: createAccountIx,
					Idx:                  ix.Idx,
				},
			})
			if err != nil {
				return nil, err
			}
			createAccountEvent.(*EventTransfer).ProgramAddress = JupMerkleDistributorProgramAddress

			transferIx := ix.innerInstructions[1]
			transferEvent, err := parseTokenIx(&parseIxContext{
				signature:          ctx.signature,
				associatedAccounts: ctx.associatedAccounts,
				ix: &SavedInstruction{
					SavedInstructionBase: transferIx,
					Idx:                  ix.Idx,
				},
			})
			if err != nil {
				return nil, err
			}

			return []EventData{createAccountEvent, transferEvent}, nil
		}
	} else if slices.Equal(disc, IxMerkleCloseClaimAccount) {
		claimant := ix.Accounts[1]
		if claimant == ctx.walletAddress {
			event := &EventCloseAccount{
				ProgramAddress: JupMerkleDistributorProgramAddress,
				From:           ix.Accounts[0],
				To:             claimant,
			}
			return []EventData{event}, nil
		}
	}

	return nil, nil
}

var (
	IxBubblegumMintV1, _             = hex.DecodeString("9162c076b8937668")
	IxBubblegumMintToCollectionV1, _ = hex.DecodeString("9912b22fc59e560f")
)

type EventCompressedNftMint struct {
	ProgramAddress string `json:"program_address"`
	Name           string `json:"name"`
	Symbol         string `json:"symbol"`
	To             string `json:"to"`
	// Address        string
}

var _ EventData = (*EventCompressedNftMint)(nil)

func (_ *EventCompressedNftMint) Type() dbutils.EventType {
	return dbutils.EventTypeMintCNft
}

// ctx needs:
//   - signature
//   - ix
//   - walletAddress
func parseBubblegumIx(ctx *parseIxContext) (EventData, error) {
	ix := ctx.ix
	if len(ix.Data) < 8 {
		return nil, ctx.errMissingDiscriminator()
	}
	disc := ix.Data[:8]

	if slices.Equal(disc, IxBubblegumMintV1) || slices.Equal(disc, IxBubblegumMintToCollectionV1) {
		leafOwner := ix.Accounts[1]

		if leafOwner != ctx.walletAddress {
			return nil, nil
		}

		// noopIix := ix.InnerIxs()[0]
		// noopData := noopIix.Data()

		// 1 uint8 noop enum prefix 0
		// 2 uint8 noop enum prefix 1
		// 3 uint8 bubblegumEventType 1
		// 4 uint8 version 1
		// 5 uint8 leafSchema prefix
		// if noopData

		data := ix.Data[8:]
		offset := uint32(0)

		nameLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		if uint32(len(data)) < offset+nameLen {
			return nil, ctx.errInvalidData()
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen

		if uint32(len(data)) < offset+4 {
			return nil, ctx.errInvalidData()
		}
		symbolLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		if uint32(len(data)) < offset+symbolLen {
			return nil, ctx.errInvalidData()
		}
		symbol := string(data[offset : offset+symbolLen])

		event := &EventCompressedNftMint{
			ProgramAddress: BubblegumProgramAddress,
			Name:           name,
			Symbol:         symbol,
			To:             ctx.walletAddress,
		}

		return event, nil
	} else {
		slog.Warn("unknown bubblegum ix", "signature", ctx.signature)
	}

	return nil, nil
}

func parseToken2022Ix(ctx *parseIxContext) (EventData, error) {
	event, err := parseTokenIx(ctx)
	if err != nil {
		return nil, err
	}
	if event != nil {
		switch e := event.(type) {
		case *EventBurn:
			e.ProgramAddress = Token2022ProgramAddress
		case *EventMint:
			e.ProgramAddress = Token2022ProgramAddress
		case *EventTransfer:
			e.ProgramAddress = Token2022ProgramAddress
		case *EventCloseAccount:
			e.ProgramAddress = Token2022ProgramAddress
		}
		return event, nil
	}
	// todo
	// handle token 2022 aditional ixs
	return nil, nil
}

type EventUnstakeLiquid struct {
	ProgramAddress string `json:"program_address"`
	// unstaked liquid token
	From []*SwapToken `json:"from"`
	// received base token
	To []*SwapToken `json:"to"`
}

var _ EventData = (*EventUnstakeLiquid)(nil)

func (_ *EventUnstakeLiquid) Type() dbutils.EventType {
	return dbutils.EventTypeUnstakeLiquid
}

var ixAtlasUnstake, _ = hex.DecodeString("5a5f6b2acd7c32e1")

func parseAtlasStakeIx(ctx *parseIxContext) (EventData, error) {
	ix := ctx.ix
	if len(ix.Data) < 8 {
		return nil, ctx.errMissingDiscriminator()
	}
	disc := ix.Data[:8]

	if slices.Equal(disc, ixAtlasUnstake) {
		burnIx := ix.innerInstructions[0]
		burnEvent, err := parseTokenIx(&parseIxContext{
			signature:          ctx.signature,
			associatedAccounts: ctx.associatedAccounts,
			ix: &SavedInstruction{
				SavedInstructionBase: burnIx,
				Idx:                  ix.Idx,
			},
		})
		if err != nil {
			return nil, err
		}

		transferIx := ctx.ix.innerInstructions[1]
		transferEvent, err := parseTokenIx(&parseIxContext{
			signature:          ctx.signature,
			associatedAccounts: ctx.associatedAccounts,
			ix: &SavedInstruction{
				SavedInstructionBase: transferIx,
				Idx:                  ix.Idx,
			},
		})
		if err != nil {
			return nil, err
		}

		be := burnEvent.(*EventBurn)
		te := transferEvent.(*EventTransfer)
		event := &EventUnstakeLiquid{
			ProgramAddress: AtlasStakeProgramAddress,
			From:           []*SwapToken{{Amount: be.Amount, Account: be.From}},
			To:             []*SwapToken{{Amount: te.Amount, Account: te.To}},
		}
		return event, nil
	}

	return nil, nil
}

type parseIxHandler struct {
	programAddress          string
	parse                   func(*parseIxContext) (EventData, error)
	parseMulti              func(*parseIxContext) ([]EventData, error)
	parseAssociatedAccounts func(*parseIxContext) error
}

var ixParsers = []parseIxHandler{
	{programAddress: SystemProgramAddress, parse: parseSystemIx},
	{programAddress: TokenProgramAddress, parse: parseTokenIx, parseAssociatedAccounts: parseTokenIxAssociatedAccounts},
	{programAddress: AssociatedTokenProgramAddress, parse: parseAssociatedTokenIx, parseAssociatedAccounts: parseAssociatedTokenIxAssociatedAccounts},
	{programAddress: JupV6ProgramAddress, parse: parseJupV6Ix},
	{programAddress: JupV4ProgramAddress, parse: parseJupV4Ix},
	{programAddress: JupLimitProgramAddress, parseMulti: parseJupLimitIx, parseAssociatedAccounts: parseJupLimitIxAssociatedAccounts},
	{programAddress: BubblegumProgramAddress, parse: parseBubblegumIx},
	{programAddress: JupMerkleDistributorProgramAddress, parseMulti: parseMerkleDistributorIx},
	{programAddress: Token2022ProgramAddress, parse: parseToken2022Ix, parseAssociatedAccounts: parseTokenIxAssociatedAccounts},
	{programAddress: AtlasStakeProgramAddress, parse: parseAtlasStakeIx},
}

type ParsedEvent struct {
	IxIdx int32
	Idx   int16
	Data  EventData
}

func ParseTx(
	tx *SavedTransaction,
	walletAddress string,
	associatedAccounts *AssociatedAccounts,
) ([]*ParsedEvent, error) {
	events := make([]*ParsedEvent, 0)

	for _, ix := range tx.Instructions {
		for _, p := range ixParsers {
			if p.programAddress == ix.ProgramAddress {
				ctx := &parseIxContext{
					signature:          tx.Signature,
					walletAddress:      walletAddress,
					associatedAccounts: associatedAccounts,
					ix:                 ix,
					logs:               tx.Logs,
				}
				if p.parse != nil {
					if eventData, err := p.parse(ctx); err != nil {
						return nil, err
					} else if eventData != nil {
						events = append(events, &ParsedEvent{
							IxIdx: ix.Idx,
							Idx:   0,
							Data:  eventData,
						})
					}
				} else if p.parseMulti != nil {
					if eventsData, err := p.parseMulti(ctx); err != nil {
						return nil, err
					} else if eventsData != nil {
						for i, e := range eventsData {
							events = append(events, &ParsedEvent{
								IxIdx: ix.Idx,
								Idx:   int16(i),
								Data:  e,
							})
						}
					}
				}
			}
		}
	}

	return events, nil
}
