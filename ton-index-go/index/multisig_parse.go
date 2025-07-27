package index

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"log"
	"strings"
)

type jettonTransferBodyTlb struct {
	_              tlb.Magic        `tlb:"#0f8a7ea5"`
	QueryID        uint64           `tlb:"## 64"`
	Amount         tlb.Coins        `tlb:"."`
	Destination    *address.Address `tlb:"addr"`
	Response       *address.Address `tlb:"addr"`
	CustomPayload  *cell.Cell       `tlb:"maybe ^"`
	ForwardAmount  tlb.Coins        `tlb:"."`
	ForwardPayload *cell.Cell       `tlb:"either . ^"`
}

type jettonBurnBodyTlb struct {
	_                   tlb.Magic        `tlb:"#595f07bc"`
	QueryID             uint64           `tlb:"## 64"`
	Amount              tlb.Coins        `tlb:"."`
	ResponseDestination *address.Address `tlb:"addr"`
}

type jettonMintBodyTlb struct {
	_         tlb.Magic        `tlb:"#642b7d07"`
	QueryID   uint64           `tlb:"## 64"`
	ToAddress *address.Address `tlb:"addr"`
	TonAmount tlb.Coins        `tlb:"."`
	MasterMsg *masterMsgTlb    `tlb:"^"`
}

type masterMsgTlb struct {
	Opcode       uint32    `tlb:"## 32"`
	QueryId      uint64    `tlb:"## 64"`
	JettonAmount tlb.Coins `tlb:"."`
}

type nftTransferTlb struct {
	_                   tlb.Magic        `tlb:"#5fcc3d14"`
	QueryID             uint64           `tlb:"## 64"`
	NewOwner            *address.Address `tlb:"addr"`
	ResponseDestination *address.Address `tlb:"addr"`
	CustomPayload       *cell.Cell       `tlb:"maybe ^"`
	ForwardAmount       tlb.Coins        `tlb:"."`
	ForwardPayload      *cell.Cell       `tlb:"either . ^"`
}

type jettonTopUpBodyTlb struct {
	_       tlb.Magic `tlb:"#00009fd3"`
	QueryID uint64    `tlb:"## 64"`
}

type jettonChangeAdminBodyTlb struct {
	_            tlb.Magic        `tlb:"#6501f354"`
	QueryID      uint64           `tlb:"## 64"`
	NewAdminAddr *address.Address `tlb:"addr"`
}

type jettonClaimAdminBodyTlb struct {
	_       tlb.Magic `tlb:"#fb88e119"`
	QueryID uint64    `tlb:"## 64"`
}

type jettonCallToBodyTlb struct {
	_         tlb.Magic        `tlb:"#235caf52"`
	QueryID   uint64           `tlb:"## 64"`
	ToAddress *address.Address `tlb:"addr"`
	TonAmount tlb.Coins        `tlb:"."`
	ActionRef *cell.Cell       `tlb:"^"`
}

type jettonSetStatusBodyTlb struct {
	_         tlb.Magic `tlb:"#eed236d3"`
	QueryID   uint64    `tlb:"## 64"`
	NewStatus uint8     `tlb:"## 8"`
}

type singleNominatorWithdrawBodyTlb struct {
	_       tlb.Magic `tlb:"#00001000"`
	QueryID uint64    `tlb:"## 64"`
	Coins   tlb.Coins `tlb:"."`
}

type singleNominatorChangeValidatorBodyTlb struct {
	_                tlb.Magic        `tlb:"#00001001"`
	QueryID          uint64           `tlb:"## 64"`
	ValidatorAddress *address.Address `tlb:"addr"`
}

type vestingInternalTransferBodyTlb struct {
	_        tlb.Magic  `tlb:"#75097f5d"`
	QueryID  uint64     `tlb:"## 64"`
	SendMode uint8      `tlb:"## 8"`
	MsgRef   *cell.Cell `tlb:"^"`
}

type orderSendMessageActionTlb struct {
	ActionType tlb.Magic   `tlb:"#f1381e5b"`
	Mode       uint8       `tlb:"## 8"`
	Body       tlb.Message `tlb:"^"`
}

type orderUpdateMultisigParamsActionTlb struct {
	ActionType   tlb.Magic          `tlb:"#1d0cfbd3"`
	NewThreshold uint8              `tlb:"## 8"`
	SignersRef   *multisigAddresses `tlb:"^"`
	ProposersRef *multisigAddresses `tlb:"maybe ^"`
}

type multisigAddresses struct {
	Addresses map[string]*address.Address `tlb:"dict inline 8 -> addr"`
}

// ParseOrder parses a base64-encoded multisig order and returns list of order actions
func ParseOrder(order string) (actions []OrderAction, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("failed to parse order: %s", r)
		}
	}()
	if order == "" {
		return nil, nil
	}

	payloadBytes, err := base64.StdEncoding.DecodeString(order)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	boc, err := cell.FromBOC(payloadBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse BOC: %w", err)
	}

	dictCell, err := wrapHashMap(boc)
	if err != nil {
		return nil, fmt.Errorf("failed to wrap hashmap: %w", err)
	}

	slice := dictCell.BeginParse()
	dict, err := slice.LoadDict(8)
	if err != nil {
		return nil, fmt.Errorf("failed to load dictionary: %w", err)
	}

	all, err := dict.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to load all dictionary entries: %w", err)
	}

	for _, kv := range all {
		action, err := parseOrderAction(kv)
		if err != nil {
			log.Println("ERROR: failed to parse order action: %w", err)

			parseError := fmt.Errorf("failed to parse order action: %w", err).Error()
			action = OrderAction{
				ParsedBody:     nil,
				ParsedBodyType: "unknown",
				Parsed:         false,
				Error:          &parseError,
			}
		}
		actions = append(actions, action)
	}

	return actions, nil
}

// parseOrderAction parses a single order action from dictionary key-value pair
func parseOrderAction(kv cell.DictKV) (OrderAction, error) {
	r, err := kv.Value.LoadRefCell()
	if err != nil {
		return OrderAction{}, fmt.Errorf("failed to load ref cell: %w", err)
	}

	orderSlice := r.BeginParse()
	orderType, err := orderSlice.PreloadUInt(32)
	if orderType == 0xf1381e5b {
		var orderAction orderSendMessageActionTlb
		err = tlb.LoadFromCell(&orderAction, r.BeginParse())
		if err != nil {
			return OrderAction{}, fmt.Errorf("failed to load order action from cell: %w", err)
		}

		// Extract destination and value from the message
		destination := orderAction.Body.Msg.DestAddr()
		value := tlb.ZeroCoins
		if intMsg, ok := orderAction.Body.Msg.(*tlb.InternalMessage); ok {
			value = intMsg.Amount
		}

		// Get raw BOC of the message body
		bodyRaw := orderAction.Body.Msg.Payload().ToBOC()

		// Parse the message body
		parsedBody, err := parseMessageBody(orderAction.Body.Msg.Payload())
		if err != nil {
			// If parsing fails, create an unknown message
			parsedBody = &UnknownMessage{
				Data: bodyRaw,
			}
		}

		return OrderAction{
			Destination:    formatAddress(destination),
			Value:          formatCoins(value),
			BodyRaw:        bodyRaw,
			ParsedBody:     &parsedBody,
			ParsedBodyType: parsedBody.GetType(),
			Parsed:         true,
			Error:          nil,
		}, nil
	} else if orderType == 0x1d0cfbd3 {
		var orderAction orderUpdateMultisigParamsActionTlb
		err = tlb.LoadFromCell(&orderAction, r.BeginParse())
		if err != nil {
			return OrderAction{}, fmt.Errorf("failed to load order action from cell: %w", err)
		}
		body, err := parseMultisigUpdateParamsBody(&orderAction)
		if err != nil {
			return OrderAction{}, fmt.Errorf("failed to parse multisig update params: %w", err)
		}
		var parsedBody ParsedBody = body
		return OrderAction{
			ParsedBody:     &parsedBody,
			ParsedBodyType: body.GetType(),
			Parsed:         true,
			Error:          nil,
		}, nil
	} else {
		return OrderAction{}, fmt.Errorf("unknown order action type: %d", orderType)
	}

}

// parseMessageBody parses the message body cell and returns the appropriate ParsedBody implementation
func parseMessageBody(bodyCell *cell.Cell) (ParsedBody, error) {
	if bodyCell == nil {
		return &TonTransferMessage{}, nil
	}

	slice := bodyCell.BeginParse()

	// Check if there's enough data for opcode
	if slice.BitsLeft() < 32 {
		// Handle as simple comment or empty body
		return parseSimpleMessage(slice)
	}

	opcode, err := slice.PreloadUInt(32)
	if err != nil {
		return parseSimpleMessage(slice)
	}

	// Reset slice to parse from beginning
	slice = bodyCell.BeginParse()

	switch opcode {
	case 0x0f8a7ea5: // JettonTransfer
		return parseJettonTransferBodyTlb(slice)
	case 0x595f07bc: // JettonBurn
		return parseJettonBurnBodyTlb(slice)
	case 0x00000015, 0x642b7d07: // JettonMint
		return parseJettonMinterBody(slice, uint32(opcode))
	case 0x5fcc3d14: // NFTTransfer
		return parseNFTTransferBodyTlb(slice)
	case 0x2167da4b: // EncryptedTextComment
		return parseEncryptedCommentTlb(slice)
	case 0x00009fd3: // JettonTopUp
		return parseJettonTopUpBody(slice)
	case 0x6501f354: // JettonChangeAdmin
		return parseJettonChangeAdminBody(slice)
	case 0xfb88e119: // JettonClaimAdmin
		return parseJettonClaimAdminBody(slice)
	case 0xcb862902: // JettonChangeContent
		return parseJettonChangeContentBody(slice)
	case 0x235caf52: // JettonCallTo
		return parseJettonCallToBody(slice)
	case 0xeed236d3: // JettonSetStatus
		return parseJettonSetStatusBody(slice)
	case 0x00001000: // SingleNominatorWithdraw
		return parseSingleNominatorWithdrawBody(slice)
	case 0x00001001: // SingleNominatorChangeValidator
		return parseSingleNominatorChangeValidatorBody(slice)
	case 0x75097f5d: // VestingInternalTransfer
		return parseVestingInternalTransferBody(slice)
	case 0: // TextComment or other
		return parseSimpleMessage(slice)
	default:
		return &UnknownMessage{
			Opcode: uint32(opcode),
			Data:   bodyCell.ToBOC(),
		}, nil
	}
}

func parseJettonTransferBodyTlb(slice *cell.Slice) (*JettonTransferBody, error) {
	var tlbStruct jettonTransferBodyTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse jetton transfer: %w", err)
	}
	jt := &JettonTransferBody{
		QueryID:       tlbStruct.QueryID,
		Amount:        formatCoins(tlbStruct.Amount),
		Destination:   formatAddress(tlbStruct.Destination),
		Response:      formatAddress(tlbStruct.Response),
		ForwardAmount: formatCoins(tlbStruct.ForwardAmount),
	}

	// Handle custom payload
	if tlbStruct.CustomPayload != nil {
		jt.CustomPayload = tlbStruct.CustomPayload.ToBOC()
	}

	// Handle forward payload
	if tlbStruct.ForwardPayload != nil {
		jt.ForwardPayload = tlbStruct.ForwardPayload.ToBOC()
		jt.parseForwardPayload(tlbStruct.ForwardPayload.BeginParse())
	}

	return jt, nil
}

func (jt *JettonTransferBody) parseForwardPayload(payloadSlice *cell.Slice) {
	if payloadSlice.BitsLeft() == 0 {
		return
	}

	if payloadSlice.BitsLeft() < 32 {
		jt.PayloadSumType = "Unknown"
		return
	}

	sumType, err := payloadSlice.PreloadUInt(32)
	if err != nil {
		jt.PayloadSumType = "Unknown"
		return
	}

	// Load the opcode
	actualSumType, err := payloadSlice.LoadUInt(32)
	if err != nil {
		jt.PayloadSumType = "Unknown"
		return
	}
	jt.PayloadSumType = fmt.Sprintf("0x%x", actualSumType)

	switch sumType {
	case 0:
		jt.PayloadSumType = "TextComment"
		if comment, err := payloadSlice.LoadStringSnake(); err == nil {
			jt.Comment = comment
		}
	case 0x2167da4b:
		jt.PayloadSumType = "EncryptedTextComment"
		jt.EncryptedComment = true
		if comment, err := payloadSlice.LoadStringSnake(); err == nil {
			jt.Comment = comment
		}
	case 0x25938561:
		jt.PayloadSumType = "StonfiSwap"
		jt.parseStonfiSwap(payloadSlice)
	default:
		jt.PayloadSumType = "Unknown"
	}
}

func (jt *JettonTransferBody) parseStonfiSwap(slice *cell.Slice) {
	defer func() {
		if recover() != nil {
			jt.PayloadSumType = "Unknown"
		}
	}()

	jettonWallet, err := slice.LoadAddr()
	if err != nil {
		return
	}

	minAmount, err := slice.LoadBigCoins()
	if err != nil {
		return
	}

	userAddress, err := slice.LoadAddr()
	if err != nil {
		return
	}

	jt.StonfiSwapBody = map[string]interface{}{
		"jetton_wallet": formatAddress(jettonWallet),
		"min_amount":    tlb.FromNanoTON(minAmount).String(),
		"user_address":  formatAddress(userAddress),
	}
}

func parseJettonBurnBodyTlb(slice *cell.Slice) (*JettonBurnBody, error) {
	var tlbStruct jettonBurnBodyTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse jetton burn: %w", err)
	}

	return &JettonBurnBody{
		QueryID:             tlbStruct.QueryID,
		Amount:              formatCoins(tlbStruct.Amount),
		ResponseDestination: formatAddress(tlbStruct.ResponseDestination),
	}, nil
}

func parseJettonMinterBody(slice *cell.Slice, opcode uint32) (*JettonMinterBody, error) {
	var tlbStruct jettonMintBodyTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse jetton mint: %w", err)
	}

	mjm := &JettonMinterBody{
		Opcode:                opcode,
		QueryID:               tlbStruct.QueryID,
		ToAddress:             formatAddress(tlbStruct.ToAddress),
		TonAmount:             formatCoins(tlbStruct.TonAmount),
		MasterMsgQueryID:      tlbStruct.MasterMsg.QueryId,
		MasterMsgJettonAmount: formatCoins(tlbStruct.MasterMsg.JettonAmount),
	}

	return mjm, nil
}

func parseNFTTransferBodyTlb(slice *cell.Slice) (*NFTTransferBody, error) {
	var tlbStruct nftTransferTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse NFT transfer: %w", err)
	}

	nt := &NFTTransferBody{
		QueryID:             tlbStruct.QueryID,
		NewOwner:            formatAddress(tlbStruct.NewOwner),
		ResponseDestination: formatAddress(tlbStruct.ResponseDestination),
		ForwardAmount:       formatCoins(tlbStruct.ForwardAmount),
	}

	// Handle custom payload
	if tlbStruct.CustomPayload != nil {
		nt.CustomPayload = tlbStruct.CustomPayload.ToBOC()
	}

	// Handle forward payload
	if tlbStruct.ForwardPayload != nil {
		nt.ForwardPayload = tlbStruct.ForwardPayload.ToBOC()
	}

	return nt, nil
}

func parseEncryptedCommentTlb(slice *cell.Slice) (*TonTransferMessage, error) {
	_, err := slice.LoadUInt(32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse encrypted comment: %w", err)
	}

	snake, err := slice.LoadStringSnake()
	if err != nil {
		return nil, fmt.Errorf("failed to parse encrypted comment: %w", err)
	}

	return &TonTransferMessage{
		Comment:   snake,
		Encrypted: true,
	}, nil
}

func parseSimpleMessage(slice *cell.Slice) (*TonTransferMessage, error) {
	ttm := &TonTransferMessage{}

	// Check if starts with encrypted opcode
	if slice.BitsLeft() >= 32 {
		opcode, err := slice.PreloadUInt(32)
		if err == nil && opcode == 0x2167da4b {
			ttm.Encrypted = true
			slice.LoadUInt(32) // Skip opcode
		}
	}

	// Try to load as string comment
	if slice.BitsLeft() >= 8 && slice.BitsLeft()%8 == 0 && slice.RefsNum() <= 1 {
		if comment, err := slice.LoadStringSnake(); err == nil {
			ttm.Comment = comment
		}
	}

	return ttm, nil
}

func parseJettonTopUpBody(slice *cell.Slice) (*JettonTopUpBody, error) {
	var tlbStruct jettonTopUpBodyTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse jetton top up: %w", err)
	}

	return &JettonTopUpBody{
		QueryID: tlbStruct.QueryID,
	}, nil
}

func parseJettonChangeAdminBody(slice *cell.Slice) (*JettonChangeAdminBody, error) {
	var tlbStruct jettonChangeAdminBodyTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse jetton change admin: %w", err)
	}

	return &JettonChangeAdminBody{
		QueryID:      tlbStruct.QueryID,
		NewAdminAddr: formatAddress(tlbStruct.NewAdminAddr),
	}, nil
}

func parseJettonClaimAdminBody(slice *cell.Slice) (*JettonClaimAdminBody, error) {
	var tlbStruct jettonClaimAdminBodyTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse jetton claim admin: %w", err)
	}

	return &JettonClaimAdminBody{
		QueryID: tlbStruct.QueryID,
	}, nil
}

func parseJettonChangeContentBody(slice *cell.Slice) (*JettonChangeContentBody, error) {
	_, err := slice.LoadUInt(32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse jetton change content: %w", err)
	}
	queryId, err := slice.LoadUInt(64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse jetton change content: %w", err)
	}

	newMetadataUrl, err := slice.LoadStringSnake()
	if err != nil {
		return nil, fmt.Errorf("failed to parse jetton change content: %w", err)
	}
	return &JettonChangeContentBody{
		QueryID:        queryId,
		NewMetadataUrl: newMetadataUrl,
	}, nil
}

func parseJettonCallToBody(slice *cell.Slice) (*JettonCallToBody, error) {
	var tlbStruct jettonCallToBodyTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse jetton call to: %w", err)
	}

	// Determine action type by examining the action reference
	actionSlice := tlbStruct.ActionRef.BeginParse()
	var actionType string

	if actionSlice.BitsLeft() >= 32 {
		actionOpcode, err := actionSlice.LoadUInt(32)
		if err == nil {
			switch actionOpcode {
			case 0xeed236d3: // set_status
				actionType = "JettonSetStatus"
			case 0x0f8a7ea5: // transfer
				actionType = "JettonTransfer"
			case 0x595f07bc: // burn
				actionType = "JettonBurn"
			default:
				actionType = "Unknown"
			}
		}
	}

	return &JettonCallToBody{
		QueryID:    tlbStruct.QueryID,
		ToAddress:  formatAddress(tlbStruct.ToAddress),
		TonAmount:  formatCoins(tlbStruct.TonAmount),
		ActionType: actionType,
		ActionData: tlbStruct.ActionRef.ToBOC(),
	}, nil
}

func parseJettonSetStatusBody(slice *cell.Slice) (*JettonSetStatusBody, error) {
	var tlbStruct jettonSetStatusBodyTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse jetton set status: %w", err)
	}

	return &JettonSetStatusBody{
		QueryID:   tlbStruct.QueryID,
		NewStatus: tlbStruct.NewStatus,
	}, nil
}

func parseSingleNominatorWithdrawBody(slice *cell.Slice) (*SingleNominatorWithdrawBody, error) {
	var tlbStruct singleNominatorWithdrawBodyTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse single nominator withdraw: %w", err)
	}

	return &SingleNominatorWithdrawBody{
		QueryID: tlbStruct.QueryID,
		Coins:   formatCoins(tlbStruct.Coins),
	}, nil
}

func parseSingleNominatorChangeValidatorBody(slice *cell.Slice) (*SingleNominatorChangeValidatorBody, error) {
	var tlbStruct singleNominatorChangeValidatorBodyTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse single nominator change validator: %w", err)
	}

	return &SingleNominatorChangeValidatorBody{
		QueryID:          tlbStruct.QueryID,
		ValidatorAddress: formatAddress(tlbStruct.ValidatorAddress),
	}, nil
}

func parseVestingInternalTransferBody(slice *cell.Slice) (*VestingInternalTransferBody, error) {
	var tlbStruct vestingInternalTransferBodyTlb
	err := tlb.LoadFromCell(&tlbStruct, slice)
	if err != nil {
		return nil, fmt.Errorf("failed to parse vesting internal transfer: %w", err)
	}

	// Parse the internal message
	var msg tlb.Message
	err = tlb.LoadFromCell(&msg, tlbStruct.MsgRef.BeginParse())
	if err != nil {
		return nil, fmt.Errorf("failed to parse internal message: %w", err)
	}

	var destination *AccountAddress
	var value *string
	var messageBody []byte
	var comment string

	if intMsg, ok := msg.Msg.(*tlb.InternalMessage); ok {
		destination = formatAddress(intMsg.DstAddr)
		value = formatCoins(intMsg.Amount)
		if intMsg.Body != nil {
			messageBody = intMsg.Body.ToBOC()
			// Try to parse comment
			body := intMsg.Body.BeginParse()
			if body.BitsLeft() >= 32 {
				opcode, err := body.LoadUInt(32)
				if err == nil && opcode == 0 {
					if commentStr, err := body.LoadStringSnake(); err == nil {
						comment = commentStr
					}
				}
			}
		}
	}

	return &VestingInternalTransferBody{
		QueryID:     tlbStruct.QueryID,
		SendMode:    tlbStruct.SendMode,
		Destination: destination,
		Value:       value,
		MessageBody: messageBody,
		Comment:     comment,
	}, nil
}

func parseMultisigUpdateParamsBody(tlbStruct *orderUpdateMultisigParamsActionTlb) (*MultisigUpdateParamsBody, error) {

	newSigners, err := parseAddressList(tlbStruct.SignersRef)
	if err != nil {
		return nil, fmt.Errorf("failed to parse new signers: %w", err)
	}
	newProposers, err := parseAddressList(tlbStruct.ProposersRef)
	if err != nil {
		return nil, fmt.Errorf("failed to parse new signers: %w", err)
	}

	return &MultisigUpdateParamsBody{
		NewThreshold: tlbStruct.NewThreshold,
		NewSigners:   newSigners,
		NewProposers: newProposers,
	}, nil
}

// parseAddressList parses a dictionary of addresses (used for signers/proposers)
func parseAddressList(addresses *multisigAddresses) ([]*AccountAddress, error) {
	if addresses == nil || addresses.Addresses == nil {
		return nil, nil
	}
	var addressList []*AccountAddress
	for _, addr := range addresses.Addresses {
		addressList = append(addressList, formatAddress(addr))
	}

	return addressList, nil
}

// formatAddress converts an address.Address to AccountAddress
func formatAddress(addr *address.Address) *AccountAddress {
	if addr == nil {
		return nil
	}
	formatted := fmt.Sprintf("%d:%s", addr.Workchain(), strings.ToUpper(hex.EncodeToString(addr.Data())))
	accountAddress := AccountAddress(formatted)
	return &accountAddress
}

// formatCoins converts tlb.Coins to *string
func formatCoins(coins tlb.Coins) *string {
	str := coins.Nano().String()
	return &str
}

// wrapHashMap wraps a hashmap cell to make it loadable as dictionary
func wrapHashMap(hashMapCell *cell.Cell) (*cell.Cell, error) {
	dictBuilder := cell.BeginCell()

	err := dictBuilder.StoreRef(hashMapCell)
	if err != nil {
		return nil, err
	}

	err = dictBuilder.StoreBoolBit(true)
	if err != nil {
		return nil, err
	}

	return dictBuilder.EndCell(), nil
}
