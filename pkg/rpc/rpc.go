package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

type Encoding string

const (
	EncodingBase64 Encoding = "base64"
	EncodingBase58 Encoding = "base58"
	EncodingJson   Encoding = "json"
)

type Commitment string

const (
	CommitmentProcessed Commitment = "processed"
	CommitmentConfirmed Commitment = "confirmed"
	CommitmentFinalized Commitment = "finalized"
)

type Response struct {
	Jsonrpc string
	Id      uint64
	Error   interface{}
}

func (r *Response) formatError() error {
	if r.Error != nil {
		return fmt.Errorf("rpcError: %#v", r.Error)
	}
	return nil
}

var reqId atomic.Uint64

type Client struct {
	url     string
	timeout *time.Duration
	limiter *time.Timer
}

func NewClient(url string) *Client {
	return &Client{
		url: url,
	}
}

func NewClientWithTimer(url string, timeout time.Duration) *Client {
	return &Client{
		url:     url,
		timeout: &timeout,
	}
}

func (c *Client) execute(method string, params any, out interface{}) error {
	if c.limiter != nil {
		<-c.limiter.C
		c.limiter = nil
	}

	rId := reqId.Add(1)
	body := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      rId,
		"method":  method,
		"params":  params,
	}
	buf, err := json.Marshal(body)
	if err != err {
		return err
	}
	req, err := http.NewRequest("POST", c.url, bytes.NewBuffer(buf))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if c.timeout != nil {
		c.limiter = time.NewTimer(*c.timeout)
	}

	text, _ := io.ReadAll(res.Body)
	defer res.Body.Close()

	if res.StatusCode != 200 {
		fmt.Printf("Response body: %s\n", string(text))
		return fmt.Errorf("response status code != 200: %s\n", string(text))
	}
	if err = json.Unmarshal(text, out); err != nil {
		return fmt.Errorf("unable to parse response body: %s\nErr: %s\n", string(text), err)
	}

	return nil
}

type SignatureResult struct {
	Err                map[string]interface{} `json:"err,omitempty"`
	Memo               bool                   `json:"memo,omitempty"`
	Signature          string
	Slot               uint64
	BlockTime          int64  `json:"blockTime,omitempty"`
	ConfirmationStatus string `json:"confirmationStatus,omitempty"`
}

type ResponseGetSignaturesForAddress struct {
	*Response
	Result []*SignatureResult
}

type GetSignaturesForAddressConfig struct {
	Limit      *uint64    `json:"limit"`
	Before     string     `json:"before"`
	After      string     `json:"until"`
	Commitment Commitment `json:"commitment"`
}

func (c *Client) GetSignaturesForAddress(address string, config *GetSignaturesForAddressConfig) ([]*SignatureResult, error) {
	conf := map[string]interface{}{}
	if config.Commitment != "" {
		conf["commitment"] = config.Commitment
	}
	if config.Limit != nil {
		conf["limit"] = config.Limit
	}
	if config.Before != "" {
		conf["before"] = config.Before
	}
	if config.After != "" {
		conf["after"] = config.After
	}
	params := []interface{}{address, conf}
	data := new(ResponseGetSignaturesForAddress)
	err := c.execute("getSignaturesForAddress", params, data)
	if err != nil {
		return nil, err
	}
	return data.Result, data.formatError()
}

type TransactionInstructionBase struct {
	ProgramIdIndex  uint8
	AccountsIndexes []int `json:"accounts"`
	Data            string
}

type TransactionInnerInstructions struct {
	IxIndex      uint16 `json:"index"`
	Instructions []*TransactionInstructionBase
}

type TokenBalanceUiTokenAmount struct {
	Amount   string
	Decimals uint8
}

type TransactionTokenBalance struct {
	AccountIndex  int
	Mint          string
	Owner         string `json:"owner,omitempty"`
	ProgramId     string `json:"programId,omitempty"`
	UiTokenAmount TokenBalanceUiTokenAmount
}

type TransactionReward struct {
	Pubkey      string `json:"pubkey"`
	Lamports    int64  `json:"lamports"`
	PostBalance uint64 `json:"postBalance"`
	RewardType  string `json:"rewardType"`
	Commission  uint8  `json:"commission,omitempty"`
}

type TransactionReturnData struct {
	ProgramId string
	Data      [2]string
}

type TransactionLoadedAddresses struct {
	Writable []string
	Readonly []string
}

type TransactionMeta struct {
	Err                  interface{}                     `json:"err,omitempty"`
	Fee                  uint64                          `json:"fee"`
	PreBalances          []uint64                        `json:"preBalances"`
	PostBalances         []uint64                        `json:"postBalances"`
	InnerInstructions    []*TransactionInnerInstructions `json:"innerInstructions,omitempty"`
	PreTokenBalances     []*TransactionTokenBalance      `json:"preTokenBalances,omitempty"`
	PostTokenBalances    []*TransactionTokenBalance      `json:"postTokenBalances,omitempty"`
	LogMessages          []string                        `json:"logMessages,omitempty"`
	Rewards              []*TransactionReward            `json:"rewards,omitempty"`
	LoadedAddresses      *TransactionLoadedAddresses     `json:"loadedAddresses,omitempty"`
	ReturnData           *TransactionReturnData          `json:"returnData,omitempty"`
	ComputeUnitsConsumed uint64                          `json:"computeUnitsConsumed,omitempty"`
}

type TransactionResult struct {
	Slot        uint64           `json:"slot"`
	Transaction [2]string        `json:"transaction"`
	BlockTime   int64            `json:"blockTime,omitempty"`
	Meta        *TransactionMeta `json:"meta"`
	Version     interface{}      `json:"version"`
}

type TransactionMessageHeader struct {
	NumRequiredSignatures       uint8 `json:"numRequiredSignatures"`
	NumReadonlySignedAccounts   uint8 `json:"numReadonlySignedAccounts"`
	NumReadonlyUnsignedAccounts uint8 `json:"numReadonlyUnsignedAccounts"`
}

type TransactionAddressTableLookup struct {
	AccountKey      string  `json:"accountKey"`
	WritableIndexes []uint8 `json:"writableIndexes"`
	ReadonlyIndexes []uint8 `json:"readonlyIndexes"`
}

type TransactionMessage struct {
	Version             uint8
	Header              *TransactionMessageHeader        `json:"header"`
	AccountKeys         []string                         `json:"accountKeys"`
	RecentBlockhash     string                           `json:"recentBlockhash"`
	Instructions        []*TransactionInstructionBase    `json:"instructions"`
	AddressTableLookups []*TransactionAddressTableLookup `json:"addressTableLookups,omitempty"`
}

type Transaction struct {
	Signatures []string            `json:"signatures"`
	Message    *TransactionMessage `json:"message"`
}

type ParsedTransactionResult struct {
	*TransactionResult
	Transaction *Transaction
}

type ResponseGetTransaction struct {
	*Response
	Result *TransactionResult `json:"result,omitempty"`
}

func (c *Client) GetTransaction(signature string, commitment Commitment) (*ParsedTransactionResult, error) {
	config := map[string]interface{}{
		"commitment":                     commitment,
		"maxSupportedTransactionVersion": 0,
		"encoding":                       EncodingBase64,
	}
	params := []interface{}{signature, config}
	data := new(ResponseGetTransaction)
	err := c.execute("getTransaction", params, data)
	if err != nil {
		return nil, err
	}
	if dataErr := data.formatError(); dataErr != nil {
		return nil, dataErr
	}
	if data.Result == nil {
		// for some reason some responses don't have result or error
		return nil, fmt.Errorf("invalid response")
	}

	tx, err := DeserializeTransaction(data.Result.Transaction[0], data.Result.Transaction[1])
	if err != nil {
		return nil, err
	}
	parsed := &ParsedTransactionResult{
		TransactionResult: data.Result,
		Transaction:       tx,
	}

	return parsed, nil
}

type BlockResult struct {
	Blockhash         string
	PreviousBlockhash string
	ParentSlot        uint64
	Signatures        []string
	BlockTime         int64  `json:"blockTime,omitempty"`
	BlockHeight       uint64 `json:"blockHeight,omitempty"`
}

type ResponseGetBlock struct {
	*Response
	Result *BlockResult
}

func (c *Client) GetBlock(slot uint64, commitment Commitment) (*BlockResult, error) {
	config := map[string]interface{}{
		"commitment":                     commitment,
		"encoding":                       EncodingBase64,
		"transactionDetails":             "signatures",
		"maxSupportedTransactionVersion": 0,
		"rewards":                        false,
	}
	params := []interface{}{slot, config}
	data := new(ResponseGetBlock)
	err := c.execute("getBlock", params, data)
	if err != nil {
		return nil, err
	}
	if data.Result == nil {
		return nil, fmt.Errorf("block is not confirmed")
	}
	return data.Result, data.formatError()
}
