package parser

type ParsableIxBase interface {
	programAddress() string
	accountsAddresses() []string
	data() []byte
}

type ParsableIx interface {
	ParsableIxBase
	innerIxs() []ParsableIx
}

type AssociatedAccount struct {
	Address string
	Type    int
}

type EventDataSerialized string

type ParsedIx struct {
	AssociatedAccounts []string
	EventsSerialized   []EventDataSerialized
}

func ParseIx(ix ParsableIx) *ParsedIx {
	return nil
}
