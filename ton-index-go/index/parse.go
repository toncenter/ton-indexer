package index

import (
	b64 "encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// json marshaling and unmarshaling
func (v *ShardId) String() string {
	return fmt.Sprintf("%X", uint64(*v))
}

func (v *ShardId) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

func (v *AccountAddress) String() string {
	return strings.Trim(string(*v), " ")
}

func (v *AccountAddress) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

func (v *HexInt) String() string {
	return fmt.Sprintf("0x%x", uint32(*v))
}

func (v *HexInt) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

func (v *OpcodeType) String() string {
	return fmt.Sprintf("0x%08x", uint32(*v))
}

func (v *OpcodeType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

// converters
func HashConverter(value string) reflect.Value {
	if len(value) == 64 || len(value) == 66 && strings.HasPrefix(value, "0x") {
		value = strings.TrimPrefix(value, "0x")
		if res, err := hex.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(b64.StdEncoding.EncodeToString(res)))
		} else {
			return reflect.Value{}
		}
	}
	if len(value) == 44 {
		if res, err := b64.StdEncoding.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(b64.StdEncoding.EncodeToString(res)))
		} else if res, err := b64.URLEncoding.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(b64.StdEncoding.EncodeToString(res)))
		} else {
			return reflect.Value{}
		}
	}
	return reflect.Value{}
}

func AccountAddressConverter(value string) reflect.Value {
	addr, err := address.ParseAddr(value)
	if err != nil {
		value_url := strings.Replace(value, "+", "-", -1)
		value_url = strings.Replace(value_url, "/", "_", -1)
		addr, err = address.ParseAddr(value_url)
	}
	if err != nil {
		addr, err = address.ParseRawAddr(value)
	}
	if err != nil {
		return reflect.Value{}
	}
	addr_str := fmt.Sprintf("%d:%s", addr.Workchain(), strings.ToUpper(hex.EncodeToString(addr.Data())))
	return reflect.ValueOf(AccountAddress(addr_str))
}

func AccountAddressNullableConverter(value string) reflect.Value {
	if value == "null" {
		return reflect.ValueOf(value)
	}
	return AccountAddressConverter(value)
}

func ShardIdConverter(value string) reflect.Value {
	value = strings.TrimPrefix(value, "0x")
	if shard, err := strconv.ParseUint(value, 16, 64); err == nil {
		return reflect.ValueOf(ShardId(shard))
	}
	if shard, err := strconv.ParseInt(value, 10, 64); err == nil {
		return reflect.ValueOf(ShardId(shard))
	}
	return reflect.Value{}
}

func UtimeTypeConverter(value string) reflect.Value {
	if utime, err := strconv.ParseUint(value, 10, 32); err == nil {
		return reflect.ValueOf(UtimeType(utime))
	}
	if utime, err := strconv.ParseFloat(value, 64); err == nil {
		return reflect.ValueOf(UtimeType(uint64(utime)))
	}
	return reflect.Value{}
}

func OpcodeTypeConverter(value string) reflect.Value {
	value = strings.TrimPrefix(value, "0x")
	if res, err := strconv.ParseUint(value, 16, 32); err == nil {
		return reflect.ValueOf(OpcodeType(res))
	}
	if res, err := strconv.ParseInt(value, 10, 32); err == nil {
		return reflect.ValueOf(OpcodeType(res))
	}
	return reflect.Value{}
}

// Parsing
func ParseBlockId(str string) (*BlockId, error) {
	str = strings.Trim(str, "()")
	parts := strings.Split(str, ",")
	var workchain int64
	var shard int64
	var seqno int64
	var err error
	if workchain, err = strconv.ParseInt(parts[0], 10, 32); err != nil {
		return nil, err
	}
	if shard, err = strconv.ParseInt(parts[1], 10, 64); err != nil {
		return nil, err
	}
	if seqno, err = strconv.ParseInt(parts[2], 10, 32); err != nil {
		return nil, err
	}
	return &BlockId{int32(workchain), ShardId(shard), int32(seqno)}, nil
}

func ParseBlockIdList(str string) ([]BlockId, error) {
	str = strings.Trim(str, "{}")

	var result []BlockId
	var start int
	for i, r := range str {
		switch r {
		case '(':
			start = i
		case ')':
			loc, err := ParseBlockId(str[start : i+1])
			if err != nil {
				return nil, err
			}
			result = append(result, *loc)
		}
	}
	return result, nil
}

// Parse wallet info
type walletInfoParserFunc func(string, *WalletState) error
type walletInfoParser struct {
	Name      string
	ParseFunc walletInfoParserFunc
}

func ParseWalletSeqno(data string, state *WalletState) error {
	boc, err := b64.StdEncoding.DecodeString(data)
	if err != nil {
		return err
	}
	if c, err := cell.FromBOC(boc); err == nil {
		l := c.BeginParse()
		state.Seqno = new(int64)
		*state.Seqno = int64(l.MustLoadUInt(32))
	} else {
		return err
	}
	return nil
}

func ParseWalletV3(data string, state *WalletState) error {
	boc, err := b64.StdEncoding.DecodeString(data)
	if err != nil {
		return err
	}
	if c, err := cell.FromBOC(boc); err == nil {
		l := c.BeginParse()
		state.Seqno = new(int64)
		state.WalletId = new(int64)
		*state.Seqno = int64(l.MustLoadUInt(32))
		*state.WalletId = int64(l.MustLoadUInt(32))
	} else {
		return err
	}
	return nil
}

func ParseWalletV5(data string, state *WalletState) error {
	boc, err := b64.StdEncoding.DecodeString(data)
	if err != nil {
		return err
	}
	if c, err := cell.FromBOC(boc); err == nil {
		l := c.BeginParse()
		state.IsSignatureAllowed = new(bool)
		state.Seqno = new(int64)
		state.WalletId = new(int64)
		*state.IsSignatureAllowed = l.MustLoadBoolBit()
		*state.Seqno = int64(l.MustLoadUInt(32))
		*state.WalletId = int64(l.MustLoadUInt(32))
	} else {
		return err
	}
	return nil
}

func (parser *walletInfoParser) Parse(data string, state *WalletState) error {
	state.IsWallet = true
	state.WalletType = &parser.Name
	err := parser.ParseFunc(data, state)
	if err != nil {
		return err
	}
	return nil
}

var walletParsersMap = map[string]walletInfoParser{
	"oM/CxIruFqJx8s/AtzgtgXVs7LEBfQd/qqs7tgL2how=": {Name: "wallet v1 r1", ParseFunc: ParseWalletSeqno},
	"1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=": {Name: "wallet v1 r2", ParseFunc: ParseWalletSeqno},
	"WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=": {Name: "wallet v1 r3", ParseFunc: ParseWalletSeqno},
	"XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=": {Name: "wallet v2 r1", ParseFunc: ParseWalletSeqno},
	"/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=": {Name: "wallet v2 r2", ParseFunc: ParseWalletSeqno},
	"thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=": {Name: "wallet v3 r1", ParseFunc: ParseWalletV3},
	"hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=": {Name: "wallet v3 r2", ParseFunc: ParseWalletV3},
	"ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=": {Name: "wallet v4 r1", ParseFunc: ParseWalletV3},
	"/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA=": {Name: "wallet v4 r2", ParseFunc: ParseWalletV3},
	"89fKU0k97trCizgZhqhJQDy6w9LFhHea8IEGWvCsS5M=": {Name: "wallet v5 beta", ParseFunc: ParseWalletV5},
	"IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8=": {Name: "wallet v5 r1", ParseFunc: ParseWalletV5},
}

func ParseWalletState(state AccountStateFull) (*WalletState, error) {
	var info WalletState
	if state.AccountAddress != nil && state.DataBoc != nil {
		if parser, ok := walletParsersMap[string(*state.CodeHash)]; ok {
			if err := parser.Parse(*state.DataBoc, &info); err != nil {
				return nil, err
			}

		} else {
			// log.Println("Parser not found for:", state)
		}
	} else if state.AccountStatus != nil && *state.AccountStatus == "active" {
		log.Println("Failed to parse state:", state)
	}
	info.AccountAddress = *state.AccountAddress
	info.Balance = state.Balance
	info.BalanceExtraCurrencies = state.BalanceExtraCurrencies
	info.AccountStatus = state.AccountStatus
	info.CodeHash = state.CodeHash
	info.LastTransactionHash = state.LastTransactionHash
	info.LastTransactionLt = state.LastTransactionLt
	return &info, nil
}

func ParseRawAction(raw *RawAction) (*Action, error) {
	var act Action

	act.TraceId = raw.TraceId
	act.ActionId = raw.ActionId
	act.StartLt = raw.StartLt
	act.EndLt = raw.EndLt
	act.StartUtime = raw.StartUtime
	act.EndUtime = raw.EndUtime
	act.TraceEndLt = raw.TraceEndLt
	act.TraceEndUtime = raw.TraceEndUtime
	act.TraceMcSeqnoEnd = raw.TraceMcSeqnoEnd
	act.TxHashes = raw.TxHashes
	act.Success = raw.Success
	act.Type = raw.Type
	act.TraceExternalHash = raw.TraceExternalHash

	switch act.Type {
	case "call_contract":
		var details ActionDetailsCallContract
		details.OpCode = raw.Opcode
		details.Source = raw.Source
		details.Destination = raw.Destination
		details.Value = raw.Value
		details.ExtraCurrencies = &raw.ExtraCurrencies
		if len(raw.ExtraCurrencies) > 0 {
			act.Type = "extra_currency_transfer"
		}
		act.Details = &details
	case "contract_deploy":
		var details ActionDetailsContractDeploy
		details.OpCode = raw.Opcode
		details.Source = raw.Source
		details.Destination = raw.Destination
		details.Value = raw.Value
		act.Details = &details
	case "ton_transfer":
		var details ActionDetailsTonTransfer
		details.Source = raw.Source
		details.Destination = raw.Destination
		details.Value = raw.Value
		details.Comment = raw.TonTransferContent
		details.Encrypted = raw.TonTransferEncrypted
		details.ExtraCurrencies = &raw.ExtraCurrencies
		if len(raw.ExtraCurrencies) > 0 {
			act.Type = "extra_currency_transfer"
		}
		act.Details = &details
	case "auction_bid":
		var details ActionDetailsAuctionBid
		details.Bidder = raw.Source
		details.Auction = raw.Destination
		details.Amount = raw.Value
		details.NftItem = raw.AssetSecondary
		details.NftCollection = raw.Asset
		details.NftItemIndex = raw.NFTTransferNFTItemIndex
		act.Details = &details
	case "change_dns":
		var details ActionDetailsChangeDns
		details.Key = raw.ChangeDNSRecordKey
		details.Value.SumType = raw.ChangeDNSRecordValueSchema
		details.NFTCollection = raw.Asset
		if raw.ChangeDNSRecordValueSchema != nil {
			switch *raw.ChangeDNSRecordValueSchema {
			case "DNSNextResolver":
				details.Value.DnsNextResolverAddress = raw.ChangeDNSRecordValue
				break
			case "DNSAdnlAddress":
				details.Value.DnsAdnlAddress = raw.ChangeDNSRecordValue
				break
			case "DNSSmcAddress":
				details.Value.DnsSmcAddress = raw.ChangeDNSRecordValue
				break
			case "DNSStorageAddress":
				details.Value.DnsStorageAddress = raw.ChangeDNSRecordValue
				break
			case "DNSText":
				details.Value.DnsStorageAddress = raw.ChangeDNSRecordValue
				break
			}
		}
		details.Value.Flags = raw.ChangeDNSRecordFlags
		details.Asset = raw.Destination
		details.Source = raw.Source
		act.Details = &details
	case "dex_deposit_liquidity":
		var details ActionDetailsDexDepositLiquidity
		details.Source = raw.Source
		details.Dex = raw.DexDepositLiquidityDataDex
		details.Pool = raw.Destination
		if raw.DexDepositLiquidityDataDex != nil && *raw.DexDepositLiquidityDataDex == "dedust" {
			details.DestinationLiquidity = raw.DestinationSecondary // deposit liquidity contract
		} else {
			details.DestinationLiquidity = raw.Destination // liquidity pool
		}
		details.Asset1 = raw.DexDepositLiquidityDataAsset1
		details.Asset2 = raw.DexDepositLiquidityDataAsset2
		details.Amount1 = raw.DexDepositLiquidityDataAmount1
		details.Amount2 = raw.DexDepositLiquidityDataAmount2
		details.UserJettonWallet1 = raw.DexDepositLiquidityDataUserJettonWallet1
		details.UserJettonWallet2 = raw.DexDepositLiquidityDataUserJettonWallet2
		details.LpTokensMinted = raw.DexDepositLiquidityDataLpTokensMinted
		act.Details = &details
	case "dex_withdraw_liquidity":
		var details ActionDetailsDexWithdrawLiquidity
		details.Source = raw.Source
		details.Dex = raw.DexWithdrawLiquidityDataDex
		details.Pool = raw.Destination
		details.DestinationLiquidity = raw.Destination
		details.Asset1 = raw.DexWithdrawLiquidityDataAsset1Out
		details.Asset2 = raw.DexWithdrawLiquidityDataAsset2Out
		details.Amount1 = raw.DexWithdrawLiquidityDataAmount1
		details.Amount2 = raw.DexWithdrawLiquidityDataAmount2
		details.UserJettonWallet1 = raw.DexWithdrawLiquidityDataUserJettonWallet1
		details.UserJettonWallet2 = raw.DexWithdrawLiquidityDataUserJettonWallet2
		details.LpTokensBurnt = raw.DexWithdrawLiquidityDataLpTokensBurnt
		act.Details = &details
	case "delete_dns":
		var details ActionDetailsDeleteDns
		details.Key = raw.ChangeDNSRecordKey
		details.Asset = raw.Destination
		details.Source = raw.Source
		details.NFTCollection = raw.Asset
		act.Details = &details
	case "renew_dns":
		var details ActionDetailsRenewDns
		details.Asset = raw.Destination
		details.Source = raw.Source
		details.NFTCollection = raw.Asset
		act.Details = &details
	case "election_deposit":
		var details ActionDetailsElectionDeposit
		details.StakeHolder = raw.Source
		details.Amount = raw.Amount
		act.Details = &details
	case "election_recover":
		var details ActionDetailsElectionRecover
		details.StakeHolder = raw.Source
		details.Amount = raw.Amount
		act.Details = &details
	case "jetton_burn":
		var details ActionDetailsJettonBurn
		details.Owner = raw.Source
		details.OwnerJettonWallet = raw.SourceSecondary
		details.Asset = raw.Asset
		details.Amount = raw.Amount
		act.Details = &details
	case "jetton_swap":
		var details ActionDetailsJettonSwap
		details.Dex = raw.JettonSwapDex
		details.Sender = raw.Source
		details.AssetIn = raw.Asset
		details.AssetOut = raw.Asset2
		details.DexIncomingTransfer = &ActionDetailsJettonSwapTransfer{}
		details.DexOutgoingTransfer = &ActionDetailsJettonSwapTransfer{}
		details.DexIncomingTransfer.Asset = raw.JettonSwapDexIncomingTransferAsset
		details.DexIncomingTransfer.Source = raw.JettonSwapDexIncomingTransferSource
		details.DexIncomingTransfer.Destination = raw.JettonSwapDexIncomingTransferDestination
		details.DexIncomingTransfer.SourceJettonWallet = raw.JettonSwapDexIncomingTransferSourceJettonWallet
		details.DexIncomingTransfer.DestinationJettonWallet = raw.JettonSwapDexIncomingTransferDestinationJettonWallet
		details.DexIncomingTransfer.Amount = raw.JettonSwapDexIncomingTransferAmount
		details.DexOutgoingTransfer.Asset = raw.JettonSwapDexOutgoingTransferAsset
		details.DexOutgoingTransfer.Source = raw.JettonSwapDexOutgoingTransferSource
		details.DexOutgoingTransfer.Destination = raw.JettonSwapDexOutgoingTransferDestination
		details.DexOutgoingTransfer.SourceJettonWallet = raw.JettonSwapDexOutgoingTransferSourceJettonWallet
		details.DexOutgoingTransfer.DestinationJettonWallet = raw.JettonSwapDexOutgoingTransferDestinationJettonWallet
		details.DexOutgoingTransfer.Amount = raw.JettonSwapDexOutgoingTransferAmount

		details.PeerSwaps = []ActionDetailsJettonSwapPeerSwap{}
		for _, peer := range raw.JettonSwapPeerSwaps {
			details.PeerSwaps = append(details.PeerSwaps, ActionDetailsJettonSwapPeerSwap(peer))
		}

		act.Details = &details
	case "jetton_transfer":
		var details ActionDetailsJettonTransfer
		details.Asset = raw.Asset
		details.Sender = raw.Source
		details.SenderJettonWallet = raw.SourceSecondary
		details.Receiver = raw.Destination
		details.ReceiverJettonWallet = raw.DestinationSecondary
		details.Amount = raw.Amount
		details.Comment = raw.JettonTransferComment
		details.IsEncryptedComment = raw.JettonTransferIsEncryptedComment
		details.QueryId = raw.JettonTransferQueryId
		details.ResponseDestination = raw.JettonTransferResponseDestination
		details.CustomPayload = raw.JettonTransferCustomPayload
		details.ForwardPayload = raw.JettonTransferForwardPayload
		details.ForwardAmount = raw.JettonTransferForwardAmount
		act.Details = &details
	case "jetton_mint":
		var details ActionDetailsJettonMint
		details.Asset = raw.Asset
		details.Amount = raw.Amount
		details.TonAmount = raw.Value
		details.Receiver = raw.Destination
		details.ReceiverJettonWallet = raw.DestinationSecondary
		act.Details = &details

	case "nft_mint":
		var details ActionDetailsNftMint
		details.Owner = raw.Source
		details.NftCollection = raw.Asset
		details.NftItem = raw.AssetSecondary
		details.NftItemIndex = raw.NFTMintNFTItemIndex
		act.Details = &details
	case "nft_transfer":
		// TODO: asset = collection, asset_secondary = item, payload, forward_amount, response_dest
		var details ActionDetailsNftTransfer
		details.NftCollection = raw.Asset
		details.NftItem = raw.AssetSecondary
		details.NftItemIndex = raw.NFTTransferNFTItemIndex
		details.OldOwner = raw.Source
		details.NewOwner = raw.Destination
		details.IsPurchase = raw.NFTTransferIsPurchase
		details.Price = raw.NFTTransferPrice
		details.QueryId = raw.NFTTransferQueryId
		details.ResponseDestination = raw.NFTTransferResponseDestination
		details.CustomPayload = raw.NFTTransferCustomPayload
		details.ForwardPayload = raw.NFTTransferForwardPayload
		details.ForwardAmount = raw.NFTTransferForwardAmount
		if raw.NFTTransferForwardPayload != nil {
			comment, isEncrypted, err := ParseCommentFromPayload(*raw.NFTTransferForwardPayload)
			if err == nil {
				details.Comment = comment
				details.IsEncryptedComment = &isEncrypted
			}
		}
		act.Details = &details
	case "tick_tock":
		var details ActionDetailsTickTock
		act.Details = &details
	case "stake_deposit":
		var details ActionDetailsStakeDeposit
		details.StakeHolder = raw.Source
		details.Amount = raw.Amount
		details.Pool = raw.Destination
		details.Provider = raw.StakingDataProvider
		details.TokensMinted = raw.StakingDataTokensMinted
		details.Asset = raw.Asset
		act.Details = &details
	case "stake_withdrawal":
		var details ActionDetailsWithdrawStake
		details.StakeHolder = raw.Source
		details.Amount = raw.Amount
		details.Pool = raw.Destination
		details.Provider = raw.StakingDataProvider
		details.PayoutNft = raw.StakingDataTsNft
		details.TokensBurnt = raw.StakingDataTokensBurnt
		details.Asset = raw.Asset
		act.Details = &details
	case "stake_withdrawal_request":
		var details ActionDetailsWithdrawStakeRequest
		details.StakeHolder = raw.Source
		details.Pool = raw.Destination
		details.Provider = raw.StakingDataProvider
		details.PayoutNft = raw.StakingDataTsNft
		details.Asset = raw.Asset
		if details.Provider != nil && *details.Provider == "tonstakers" {
			details.TokensBurnt = raw.Amount
		}
		act.Details = &details
	case "subscribe":
		var details ActionDetailsSubscribe
		details.Subscriber = raw.Source
		details.Beneficiary = raw.Destination
		details.Subscription = raw.DestinationSecondary
		details.Amount = raw.Amount
		act.Details = &details
	case "unsubscribe":
		var details ActionDetailsUnsubscribe
		details.Subscriber = raw.Source
		details.Beneficiary = raw.Destination
		details.Subscription = raw.DestinationSecondary
		details.Amount = raw.Amount
		act.Details = &details
	case "multisig_create_order":
		act.Details = ActionDetailsMultisigCreateOrder{
			QueryId:           raw.MultisigCreateOrderQueryId,
			OrderSeqno:        raw.MultisigCreateOrderOrderSeqno,
			IsCreatedBySigner: raw.MultisigCreateOrderIsCreatedBySigner,
			IsSignedByCreator: raw.MultisigCreateOrderIsSignedByCreator,
			CreatorIndex:      raw.MultisigCreateOrderCreatorIndex,
			ExpirationDate:    raw.MultisigCreateOrderExpirationDate,
			OrderBoc:          raw.MultisigCreateOrderOrderBoc,
			Source:            raw.Source,
			Destination:       raw.Destination,
			DestinationOrder:  raw.DestinationSecondary,
		}
	case "multisig_approve":
		act.Details = ActionDetailsMultisigApprove{
			SignerIndex: raw.MultisigApproveSignerIndex,
			ExitCode:    raw.MultisigApproveExitCode,
			Source:      raw.Source,
			Destination: raw.Destination,
		}
	case "multisig_execute":
		act.Details = ActionDetailsMultisigExecute{
			QueryId:        raw.MultisigExecuteQueryId,
			OrderSeqno:     raw.MultisigExecuteOrderSeqno,
			ExpirationDate: raw.MultisigExecuteExpirationDate,
			ApprovalsNum:   raw.MultisigExecuteApprovalsNum,
			SignersHash:    raw.MultisigExecuteSignersHash,
			OrderBoc:       raw.MultisigExecuteOrderBoc,
			Source:         raw.Source,
			Destination:    raw.Destination,
		}
	case "vesting_send_message":
		act.Details = ActionDetailsVestingSendMessage{
			QueryId:     raw.VestingSendMessageQueryId,
			MessageBoc:  raw.VestingSendMessageMessageBoc,
			Source:      raw.Source,
			Vesting:     raw.Destination,
			Destination: raw.DestinationSecondary,
			Amount:      raw.Amount,
		}
	case "vesting_add_whitelist":
		act.Details = ActionDetailsVestingAddWhitelist{
			QueryId:       raw.VestingAddWhitelistQueryId,
			AccountsAdded: raw.VestingAddWhitelistAccountsAdded,
			Source:        raw.Source,
			Vesting:       raw.Destination,
		}
	case "evaa_supply":
		act.Details = ActionDetailsEvaaSupply{
			SenderJettonWallet:    raw.EvaaSupplySenderJettonWallet,
			RecipientJettonWallet: raw.EvaaSupplyRecipientJettonWallet,
			MasterJettonWallet:    raw.EvaaSupplyMasterJettonWallet,
			Master:                raw.EvaaSupplyMaster,
			AssetId:               raw.EvaaSupplyAssetId,
			IsTon:                 raw.EvaaSupplyIsTon,
			Source:                raw.Source,
			SourceWallet:          raw.SourceSecondary,
			Recipient:             raw.Destination,
			RecipientContract:     raw.DestinationSecondary,
			Asset:                 raw.Asset,
			Amount:                raw.Amount,
		}
	case "evaa_withdraw":
		act.Details = ActionDetailsEvaaWithdraw{
			RecipientJettonWallet: raw.EvaaWithdrawRecipientJettonWallet,
			MasterJettonWallet:    raw.EvaaWithdrawMasterJettonWallet,
			Master:                raw.EvaaWithdrawMaster,
			FailReason:            raw.EvaaWithdrawFailReason,
			AssetId:               raw.EvaaWithdrawAssetId,
			Source:                raw.Source,
			Recipient:             raw.Destination,
			OwnerContract:         raw.DestinationSecondary,
			Asset:                 raw.Asset,
			Amount:                raw.Amount,
		}
	case "evaa_liquidate":
		act.Details = ActionDetailsEvaaLiquidate{
			FailReason:       raw.EvaaLiquidateFailReason,
			DebtAmount:       raw.EvaaLiquidateDebtAmount,
			Source:           raw.Source,
			Borrower:         raw.Destination,
			BorrowerContract: raw.DestinationSecondary,
			Collateral:       raw.Asset,
			AssetId:          raw.EvaaLiquidateAssetId,
			Amount:           raw.Amount,
		}
	case "jvault_claim":
		claimedRewards := make([]JettonAmountPair, 0)
		if len(raw.JvaultClaimClaimedJettons) == len(raw.JvaultClaimClaimedAmounts) {
			for i := range raw.JvaultClaimClaimedJettons {
				claimedRewards = append(claimedRewards, JettonAmountPair{
					Jetton: &raw.JvaultClaimClaimedJettons[i],
					Amount: &raw.JvaultClaimClaimedAmounts[i],
				})
			}
		}
		act.Details = ActionDetailsJvaultClaim{
			ClaimedRewards: claimedRewards,
			Source:         raw.Source,
			StakeWallet:    raw.SourceSecondary,
			Pool:           raw.Destination,
		}
	case "jvault_stake":
		act.Details = ActionDetailsJvaultStake{
			Period:             raw.JvaultStakePeriod,
			MintedStakeJettons: raw.JvaultStakeMintedStakeJettons,
			StakeWallet:        raw.JvaultStakeStakeWallet,
			Source:             raw.Source,
			SourceJettonWallet: raw.SourceSecondary,
			Asset:              raw.Asset,
			Pool:               raw.Destination,
			Amount:             raw.Amount,
		}
	case "jvault_unstake":
		act.Details = ActionDetailsJvaultUnstake{
			Source:      raw.Source,
			StakeWallet: raw.SourceSecondary,
			Pool:        raw.Destination,
			Amount:      raw.Amount,
			ExitCode:    raw.JvaultExitCode,
		}
	case "nft_discovery":
		act.Details = ActionDetailsNftDiscovery{
			Source:        raw.Source,
			NftItem:       raw.AssetSecondary,
			NftCollection: raw.Asset,
			NftItemIndex:  raw.NFTTransferNFTItemIndex,
		}
	default:
		details := map[string]string{}
		details["error"] = fmt.Sprintf("unsupported action type: '%s'", act.Type)
		act.Details = &details
		act.RawAction = raw
	}
	return &act, nil
}

// query to model
func ScanBlock(row pgx.Row) (*Block, error) {
	var blk Block
	var prev_blocks_str string
	err := row.Scan(&blk.Workchain, &blk.Shard, &blk.Seqno, &blk.RootHash,
		&blk.FileHash, &blk.MasterchainBlockRef.Workchain,
		&blk.MasterchainBlockRef.Shard, &blk.MasterchainBlockRef.Seqno,
		&blk.GlobalId, &blk.Version, &blk.AfterMerge,
		&blk.BeforeSplit, &blk.AfterSplit, &blk.WantMerge, &blk.WantSplit,
		&blk.KeyBlock, &blk.VertSeqnoIncr, &blk.Flags, &blk.GenUtime,
		&blk.StartLt, &blk.EndLt, &blk.ValidatorListHashShort,
		&blk.GenCatchainSeqno, &blk.MinRefMcSeqno, &blk.PrevKeyBlockSeqno,
		&blk.VertSeqno, &blk.MasterRefSeqno, &blk.RandSeed, &blk.CreatedBy,
		&blk.TxCount, &prev_blocks_str)
	if err != nil {
		return nil, err
	}

	if prev_blocks, err := ParseBlockIdList(prev_blocks_str); err != nil {
		return nil, err
	} else {
		blk.PrevBlocks = prev_blocks
	}
	return &blk, nil
}

func ScanTransaction(row pgx.Row) (*Transaction, error) {
	var t Transaction
	t.OutMsgs = []*Message{}

	var st StoragePhase
	var cr CreditPhase
	var co ComputePhase
	var ac ActionPhase
	var bo BouncePhase
	var sp SplitInfo
	var ms1 MsgSize
	var ms2 MsgSize

	err := row.Scan(&t.Account, &t.Hash, &t.Lt, &t.Workchain, &t.Shard, &t.Seqno,
		&t.McSeqno, &t.TraceId, &t.PrevTransHash, &t.PrevTransLt, &t.Now,
		&t.OrigStatus, &t.EndStatus, &t.TotalFees, &t.TotalFeesExtraCurrencies, &t.AccountStateHashBefore,
		&t.AccountStateHashAfter, &t.Descr.Type, &t.Descr.Aborted, &t.Descr.Destroyed,
		&t.Descr.CreditFirst, &t.Descr.IsTock, &t.Descr.Installed,
		&st.StorageFeesCollected, &st.StorageFeesDue, &st.StatusChange,
		&cr.DueFeesCollected, &cr.Credit, &cr.CreditExtraCurrencies,
		&co.IsSkipped, &co.Reason, &co.Success, &co.MsgStateUsed,
		&co.AccountActivated, &co.GasFees, &co.GasUsed, &co.GasLimit,
		&co.GasCredit, &co.Mode, &co.ExitCode, &co.ExitArg,
		&co.VmSteps, &co.VmInitStateHash, &co.VmFinalStateHash,
		&ac.Success, &ac.Valid, &ac.NoFunds, &ac.StatusChange, &ac.TotalFwdFees,
		&ac.TotalActionFees, &ac.ResultCode, &ac.ResultArg, &ac.TotActions,
		&ac.SpecActions, &ac.SkippedActions, &ac.MsgsCreated, &ac.ActionListHash,
		&ms1.Cells, &ms1.Bits,
		&bo.Type, &ms2.Cells, &ms2.Bits,
		&bo.ReqFwdFees, &bo.MsgFees, &bo.FwdFees,
		&sp.CurShardPfxLen, &sp.AccSplitDepth, &sp.ThisAddr, &sp.SiblingAddr, &t.Emulated)

	if err != nil {
		return nil, err
	}

	// fix for incorrectly inserted data - gas_fees and gas_used were swapped
	if co.GasFees != nil && co.GasUsed != nil && *co.GasFees < *co.GasUsed {
		co.GasFees, co.GasUsed = co.GasUsed, co.GasFees
	}

	t.BlockRef = BlockId{t.Workchain, t.Shard, t.Seqno}
	t.AccountStateAfter = &AccountState{Hash: t.AccountStateHashAfter}
	t.AccountStateBefore = &AccountState{Hash: t.AccountStateHashBefore}

	if ms1.Cells != nil {
		ac.TotMsgSize = &ms1
	}

	if ms2.Cells != nil {
		bo.MsgSize = &ms2
	}

	if st.StatusChange != nil {
		t.Descr.StoragePh = &st
	}
	if cr.DueFeesCollected != nil || cr.Credit != nil || cr.CreditExtraCurrencies != nil {
		t.Descr.CreditPh = &cr
	}
	if co.IsSkipped != nil {
		t.Descr.ComputePh = &co
	}
	if ac.Success != nil {
		t.Descr.Action = &ac
	}
	if bo.Type != nil {
		t.Descr.Bounce = &bo
	}
	if sp.CurShardPfxLen != nil {
		t.Descr.SplitInfo = &sp
	}

	return &t, nil
}

func (mc *MessageContent) TryDecodeBody() error {
	if mc.Body == nil {
		return errors.New("empty MessageContent")
	}
	if boc, err := b64.StdEncoding.DecodeString(*mc.Body); err == nil {
		if c, err := cell.FromBOC(boc); err == nil {
			l := c.BeginParse()
			if val, err := l.LoadUInt(32); err == nil && val == 0 {
				str, _ := l.LoadStringSnake()
				mc.Decoded = &DecodedContent{Type: "text_comment", Comment: str}
			}
		}
	}
	return nil
}

func ScanMessageWithContent(row pgx.Row) (*Message, error) {
	var m Message
	var body MessageContent
	var init_state MessageContent

	err := row.Scan(&m.TxHash, &m.TxLt, &m.MsgHash, &m.Direction, &m.TraceId, &m.Source, &m.Destination,
		&m.Value, &m.ValueExtraCurrencies, &m.FwdFee, &m.IhrFee, &m.CreatedLt, &m.CreatedAt, &m.Opcode,
		&m.IhrDisabled, &m.Bounce, &m.Bounced, &m.ImportFee, &m.BodyHash, &m.InitStateHash, &m.MsgHashNorm,
		&m.InMsgTxHash, &m.OutMsgTxHash, &body.Hash, &body.Body, &init_state.Hash, &init_state.Body)
	if body.Hash != nil {
		body.TryDecodeBody()
		m.MessageContent = &body
	}
	if init_state.Hash != nil {
		m.InitState = &init_state
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func ScanMessageContent(row pgx.Row) (*MessageContent, error) {
	var mc MessageContent
	err := row.Scan(&mc.Hash, &mc.Body)
	mc.TryDecodeBody()
	if err != nil {
		return nil, err
	}
	return &mc, nil
}

func ScanAccountState(row pgx.Row) (*AccountState, error) {
	var acst AccountState
	err := row.Scan(&acst.Hash, &acst.Account, &acst.Balance, &acst.BalanceExtraCurrencies,
		&acst.AccountStatus, &acst.FrozenHash, &acst.DataHash, &acst.CodeHash)
	if err != nil {
		return nil, err
	}
	return &acst, nil
}

func ScanAccountStateFull(row pgx.Row) (*AccountStateFull, error) {
	var acst AccountStateFull
	err := row.Scan(&acst.AccountAddress, &acst.Hash, &acst.Balance, &acst.BalanceExtraCurrencies,
		&acst.AccountStatus, &acst.FrozenHash, &acst.LastTransactionHash, &acst.LastTransactionLt,
		&acst.DataHash, &acst.CodeHash, &acst.DataBoc, &acst.CodeBoc)
	if err != nil {
		return nil, err
	}
	trimQuotes(acst.CodeBoc)
	trimQuotes(acst.DataBoc)
	return &acst, nil
}

func trimQuotes(s *string) {
	if s != nil {
		*s = strings.Trim(*s, "'")
	}
}

func ScanAccountBalance(row pgx.Row) (*AccountBalance, error) {
	var acst AccountBalance
	err := row.Scan(&acst.Account, &acst.Balance)
	if err != nil {
		return nil, err
	}
	return &acst, nil
}

func ScanNFTCollection(row pgx.Row) (*NFTCollection, error) {
	var res NFTCollection
	err := row.Scan(&res.Address, &res.NextItemIndex, &res.OwnerAddress, &res.CollectionContent,
		&res.DataHash, &res.CodeHash, &res.LastTransactionLt)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanNFTItem(row pgx.Row) (*NFTItem, error) {
	var res NFTItem
	err := row.Scan(&res.Address, &res.Init, &res.Index, &res.CollectionAddress,
		&res.OwnerAddress, &res.Content, &res.LastTransactionLt, &res.CodeHash, &res.DataHash)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanNFTItemWithCollection(row pgx.Row) (*NFTItem, error) {
	var res NFTItem
	var col NFTCollectionNullable

	err := row.Scan(&res.Address, &res.Init, &res.Index, &res.CollectionAddress,
		&res.OwnerAddress, &res.Content, &res.LastTransactionLt, &res.CodeHash, &res.DataHash,
		&col.Address, &col.NextItemIndex, &col.OwnerAddress, &col.CollectionContent,
		&col.DataHash, &col.CodeHash, &col.LastTransactionLt)
	if col.Address != nil {
		res.Collection = new(NFTCollection)
		res.Collection.Address = *col.Address
		res.Collection.NextItemIndex = *col.NextItemIndex
		res.Collection.OwnerAddress = col.OwnerAddress
		res.Collection.CollectionContent = col.CollectionContent
		res.Collection.DataHash = *col.DataHash
		res.Collection.CodeHash = *col.CodeHash
		res.Collection.LastTransactionLt = *col.LastTransactionLt
	}
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanNFTTransfer(row pgx.Row) (*NFTTransfer, error) {
	var res NFTTransfer
	err := row.Scan(&res.TransactionHash, &res.TransactionLt, &res.TransactionNow, &res.TransactionAborted,
		&res.QueryId, &res.NftItemAddress, &res.NftItemIndex, &res.NftCollectionAddress,
		&res.OldOwner, &res.NewOwner, &res.ResponseDestination, &res.CustomPayload,
		&res.ForwardAmount, &res.ForwardPayload, &res.TraceId)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanJettonMaster(row pgx.Row) (*JettonMaster, error) {
	var res JettonMaster
	err := row.Scan(&res.Address, &res.TotalSupply, &res.Mintable, &res.AdminAddress,
		&res.JettonContent, &res.JettonWalletCodeHash, &res.CodeHash, &res.DataHash,
		&res.LastTransactionLt)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanJettonWallet(row pgx.Row) (*JettonWallet, error) {
	var res JettonWallet
	var mintless_into JettonWalletMintlessInfo
	err := row.Scan(&res.Address, &res.Balance, &res.Owner, &res.Jetton, &res.LastTransactionLt,
		&res.CodeHash, &res.DataHash, &mintless_into.IsClaimed, &mintless_into.Amount,
		&mintless_into.StartFrom, &mintless_into.ExpireAt, &mintless_into.CustomPayloadApiUri)
	if mintless_into.IsClaimed != nil && !*mintless_into.IsClaimed && mintless_into.Amount != nil {
		res.MintlessInfo = &mintless_into
	}
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanJettonTransfer(row pgx.Row) (*JettonTransfer, error) {
	var res JettonTransfer
	err := row.Scan(&res.TransactionHash, &res.TransactionLt, &res.TransactionNow, &res.TransactionAborted,
		&res.QueryId, &res.Amount, &res.Source, &res.Destination, &res.SourceWallet, &res.JettonMaster,
		&res.ResponseDestination, &res.CustomPayload, &res.ForwardTonAmount, &res.ForwardPayload, &res.TraceId)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanJettonBurn(row pgx.Row) (*JettonBurn, error) {
	var res JettonBurn
	err := row.Scan(&res.TransactionHash, &res.TransactionLt, &res.TransactionNow, &res.TransactionAborted,
		&res.QueryId, &res.Owner, &res.JettonWallet, &res.JettonMaster, &res.Amount,
		&res.ResponseDestination, &res.CustomPayload, &res.TraceId)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanRawAction(row pgx.Row) (*RawAction, error) {
	var act RawAction
	err := row.Scan(&act.TraceId, &act.ActionId, &act.StartLt, &act.EndLt, &act.StartUtime, &act.EndUtime,
		&act.TraceEndLt, &act.TraceEndUtime, &act.TraceMcSeqnoEnd,
		&act.Source, &act.SourceSecondary, &act.Destination, &act.DestinationSecondary, &act.Asset, &act.AssetSecondary,
		&act.Asset2, &act.Asset2Secondary, &act.Opcode, &act.TxHashes, &act.Type, &act.TonTransferContent,
		&act.TonTransferEncrypted, &act.Value, &act.Amount, &act.JettonTransferResponseDestination, &act.JettonTransferForwardAmount,
		&act.JettonTransferQueryId, &act.JettonTransferCustomPayload, &act.JettonTransferForwardPayload,
		&act.JettonTransferComment, &act.JettonTransferIsEncryptedComment, &act.NFTTransferIsPurchase, &act.NFTTransferPrice,
		&act.NFTTransferQueryId, &act.NFTTransferCustomPayload, &act.NFTTransferForwardPayload, &act.NFTTransferForwardAmount, &act.NFTTransferResponseDestination,
		&act.NFTTransferNFTItemIndex, &act.JettonSwapDex, &act.JettonSwapSender, &act.JettonSwapDexIncomingTransferAmount, &act.JettonSwapDexIncomingTransferAsset,
		&act.JettonSwapDexIncomingTransferSource, &act.JettonSwapDexIncomingTransferDestination, &act.JettonSwapDexIncomingTransferSourceJettonWallet,
		&act.JettonSwapDexIncomingTransferDestinationJettonWallet, &act.JettonSwapDexOutgoingTransferAmount, &act.JettonSwapDexOutgoingTransferAsset,
		&act.JettonSwapDexOutgoingTransferSource, &act.JettonSwapDexOutgoingTransferDestination, &act.JettonSwapDexOutgoingTransferSourceJettonWallet,
		&act.JettonSwapDexOutgoingTransferDestinationJettonWallet, &act.JettonSwapPeerSwaps, &act.ChangeDNSRecordKey, &act.ChangeDNSRecordValueSchema,
		&act.ChangeDNSRecordValue, &act.ChangeDNSRecordFlags, &act.NFTMintNFTItemIndex,
		&act.DexWithdrawLiquidityDataDex,
		&act.DexWithdrawLiquidityDataAmount1,
		&act.DexWithdrawLiquidityDataAmount2,
		&act.DexWithdrawLiquidityDataAsset1Out,
		&act.DexWithdrawLiquidityDataAsset2Out,
		&act.DexWithdrawLiquidityDataUserJettonWallet1,
		&act.DexWithdrawLiquidityDataUserJettonWallet2,
		&act.DexWithdrawLiquidityDataDexJettonWallet1,
		&act.DexWithdrawLiquidityDataDexJettonWallet2,
		&act.DexWithdrawLiquidityDataLpTokensBurnt,
		&act.DexDepositLiquidityDataDex,
		&act.DexDepositLiquidityDataAmount1,
		&act.DexDepositLiquidityDataAmount2,
		&act.DexDepositLiquidityDataAsset1,
		&act.DexDepositLiquidityDataAsset2,
		&act.DexDepositLiquidityDataUserJettonWallet1,
		&act.DexDepositLiquidityDataUserJettonWallet2,
		&act.DexDepositLiquidityDataLpTokensMinted,
		&act.StakingDataProvider,
		&act.StakingDataTsNft,
		&act.StakingDataTokensBurnt,
		&act.StakingDataTokensMinted,
		&act.Success,
		&act.TraceExternalHash,
		&act.ExtraCurrencies,
		&act.MultisigCreateOrderQueryId,
		&act.MultisigCreateOrderOrderSeqno,
		&act.MultisigCreateOrderIsCreatedBySigner,
		&act.MultisigCreateOrderIsSignedByCreator,
		&act.MultisigCreateOrderCreatorIndex,
		&act.MultisigCreateOrderExpirationDate,
		&act.MultisigCreateOrderOrderBoc,
		&act.MultisigApproveSignerIndex,
		&act.MultisigApproveExitCode,
		&act.MultisigExecuteQueryId,
		&act.MultisigExecuteOrderSeqno,
		&act.MultisigExecuteExpirationDate,
		&act.MultisigExecuteApprovalsNum,
		&act.MultisigExecuteSignersHash,
		&act.MultisigExecuteOrderBoc,
		&act.VestingSendMessageQueryId,
		&act.VestingSendMessageMessageBoc,
		&act.VestingAddWhitelistQueryId,
		&act.VestingAddWhitelistAccountsAdded,
		&act.EvaaSupplySenderJettonWallet,
		&act.EvaaSupplyRecipientJettonWallet,
		&act.EvaaSupplyMasterJettonWallet,
		&act.EvaaSupplyMaster,
		&act.EvaaSupplyAssetId,
		&act.EvaaSupplyIsTon,
		&act.EvaaWithdrawRecipientJettonWallet,
		&act.EvaaWithdrawMasterJettonWallet,
		&act.EvaaWithdrawMaster,
		&act.EvaaWithdrawFailReason,
		&act.EvaaWithdrawAssetId,
		&act.EvaaLiquidateFailReason,
		&act.EvaaLiquidateDebtAmount,
		&act.EvaaLiquidateAssetId,
		&act.JvaultClaimClaimedJettons,
		&act.JvaultClaimClaimedAmounts,
		&act.JvaultStakePeriod,
		&act.JvaultStakeMintedStakeJettons,
		&act.JvaultStakeStakeWallet)

	if err != nil {
		return nil, err
	}
	return &act, nil
}

func ScanTrace(row pgx.Row) (*Trace, error) {
	var trace Trace
	err := row.Scan(&trace.TraceId, &trace.ExternalHash, &trace.McSeqnoStart, &trace.McSeqnoEnd,
		&trace.StartLt, &trace.StartUtime, &trace.EndLt, &trace.EndUtime,
		&trace.TraceMeta.TraceState, &trace.TraceMeta.Messages, &trace.TraceMeta.Transactions,
		&trace.TraceMeta.PendingMessages, &trace.TraceMeta.ClassificationState)

	if err != nil {
		return nil, err
	}
	return &trace, nil
}
