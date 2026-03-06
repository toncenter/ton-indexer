package parse

import (
	"fmt"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

var EVAA_TON_ASSET_ID string = "0x1a4219fe5e60d63af2a3cc7dce6fec69b45c6b5718497a6148e7c232ac87bd8a"

var EVAA_ASSET_ID_MAP_MAINNET = map[string]string{
	"0xb387968236197958ca4ac55e9b5be38e688c7631af84c86756431f49a878ef33": "0:729C13B6DF2C07CBF0A06AB63D34AF454F3D320EC1BCD8FB5C6D24D0806A17C2",
	"0x83d916c68510802104d1f75aa6ce30eb1e477aede0d380eee2188e0e56581fc6": "0:7E30FC2B7751BA58A3642F3FD59D5E96A810DDD78D8A310BFE8353BEF10500DF",
	"0x495668e908644f30322b997de8faaafc21f05aa52f8982f042dac1fe0b4d09d0": "0:CD872FA7C5816052ACDF5332260443FAEC9AACC8C21CCA4D92E7F47034D11892",
	"0x3313e2f57ba870af34480350c789b0987d15b43a53172bfce294de21e7d724e7": "0:BDF3FA8098D129B54B4F73B5BAC5D1E1FD91EB054169C3916DFC8CCD536D1000",
	"0xca9006bd3fb03d355daeeff93b24be90afaa6e3ca0073ff5720f8a852c933278": "0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE",
	"0xe025e6a575f174da6d62577c1fe204ccaa5d1e47c55c6b20c67daee56c357b60": "0:3E5FFCA8DDFCF36C36C9FF46F31562AAB51B9914845AD6C26CBDE649D58A5588",
	"0x9c77a4d798a8f500dcfb877a07227c4ca9d6782504cdc7ce2ad0051e5641c032": "0:8D636010DD90D8C0902AC7F9F397D8BD5E177F131EE2CCA24CE894F15D19CEEA",
	"0x6bfa124cc1343d14ba57ded299f6f514f5f26777099e3c378922ce4081ecbf91": "0:AEA78C710AE94270DC263A870CF47B4360F53CC5ED38E3DB502E9E9AFB904B11",
	"0xe08b1f092e7240fb61bf8249d71aa635102acdd50122138105c35dd2896b5331": "0:FE72F474373E97032441BDB873F9A6D3AD10BAB08E6DBC7BEFA5E42B695F5400",
	"0x70930360b81e5f4ef2e06d2bf64978b44d4b87fb85d78788e001cdf8812d13d1": "0:AFC49CB8786F21C87045B19EDE78FC6B46C51048513F8E9A6D44060199C1BF0C",
	"0x8be3365cabaa6a0f90d2e64f03fa78268c135fe0b0758b576b447e9b2068d75d": "0:2F956143C461769579BAEF2E32CC2D7BC18283F40D20BB03E432CD603AC33FFC",
	"0xb372eaca5210612d285f3fdd30e87a0c3a443f1bb7578e16da72563241997ca1": "0:1F1798F724C2296652E6002BFB51BED11FB5A689532E5788AF7203581EF367A8",
	"0xb8a51f42ccbb912d973e58a22dda26decc7dfc83718c325c24432045ad78dc5a": "0:E8CB571401B1AADB9DEFC1FA0F72A4A5A0E0D5016903067891D763C91541E72B",
	"0xc585bac25948a5feea8f2a9e052eb45995882b15dfb784b37cd271cc163f3aea": "0:5EE20B5240CC4CE51A4C60BAF8C9D358EF617C26463E89C6571B534972C8EEF1",
	"0xd9496f9b279a4e26c49eb65d5c47c79b6dfdea16c9baefa9158a7dfbbbcac04a": "0:086FA2A675F74347B08DD4606A549B8FDB98829CB282BC1949D3B12FBAED9DCC",
	"0x4a4bae807cc87355de2ead44b21a739d9d35e9bba9a217f5cc9c0108197ad137": "0:D0E545323C7ACB7102653C073377F7E3C67F122EB94D430A250739F109D4A57D",
}
var EVAA_ASSET_ID_MAP_TESTNET = map[string]string{
	"0xb387968236197958ca4ac55e9b5be38e688c7631af84c86756431f49a878ef33": "0:5EE20B5240CC4CE51A4C60BAF8C9D358EF617C26463E89C6571B534972C8EEF1",
	"0x83d916c68510802104d1f75aa6ce30eb1e477aede0d380eee2188e0e56581fc6": "0:DA639C946AD6271DE8BBDC706A457FCEC7D2CAD979D1ED7B8263D3050B620D6D",
	"0x495668e908644f30322b997de8faaafc21f05aa52f8982f042dac1fe0b4d09d0": "0:B70EEC37760F24F7CC5FE52EC19BB60E595279416F219B788BA857C97DAFE0F6",
	"0xb8a51f42ccbb912d973e58a22dda26decc7dfc83718c325c24432045ad78dc5a": "0:E8CB571401B1AADB9DEFC1FA0F72A4A5A0E0D5016903067891D763C91541E72B",
	"0xc585bac25948a5feea8f2a9e052eb45995882b15dfb784b37cd271cc163f3aea": "0:5EE20B5240CC4CE51A4C60BAF8C9D358EF617C26463E89C6571B534972C8EEF1",
}

func ParseEvaaAssetId(assetId string) (*string, bool) {
	if assetId == EVAA_TON_ASSET_ID {
		return nil, true
	}
	target_asset_map := &EVAA_ASSET_ID_MAP_MAINNET
	if isTestnet {
		target_asset_map = &EVAA_ASSET_ID_MAP_TESTNET
	}
	if address, ok := (*target_asset_map)[assetId]; ok {
		return &address, true
	}
	return nil, false
}

func CollectAddressesFromAction(addr_list *map[string]bool, raw_action *models.RawAction) bool {
	success := true

	if v := raw_action.Source; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.SourceSecondary; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.Destination; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DestinationSecondary; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.Asset; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.AssetSecondary; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.Asset2; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.Asset2Secondary; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonTransferResponseDestination; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.NFTTransferResponseDestination; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapSender; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexIncomingTransferAsset; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexIncomingTransferSource; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexIncomingTransferDestination; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexIncomingTransferSourceJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexIncomingTransferDestinationJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexOutgoingTransferAsset; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexOutgoingTransferSource; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexOutgoingTransferDestination; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexOutgoingTransferSourceJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexOutgoingTransferDestinationJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DexDepositLiquidityDataAsset1; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DexDepositLiquidityDataAsset2; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DexDepositLiquidityDataTargetAsset1; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DexDepositLiquidityDataTargetAsset2; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DexWithdrawLiquidityDataAsset1Out; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DexWithdrawLiquidityDataAsset2Out; v != nil {
		(*addr_list)[(string)(*v)] = true
	}

	// Multisig fields
	if v := raw_action.DestinationSecondary; v != nil {
		(*addr_list)[(string)(*v)] = true
	}

	// EVAA fields
	if v := raw_action.EvaaSupplySenderJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaSupplyRecipientJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaSupplyMasterJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaSupplyMaster; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaWithdrawRecipientJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaWithdrawMasterJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaWithdrawMaster; v != nil {
		(*addr_list)[(string)(*v)] = true
	}

	// JVault fields
	if v := raw_action.JvaultStakeStakeWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	for _, v := range raw_action.JvaultClaimClaimedJettons {
		(*addr_list)[(string)(v)] = true
	}

	if v := raw_action.EvaaLiquidateAssetId; v != nil {
		if master, ok := ParseEvaaAssetId(*v); ok && master != nil {
			(*addr_list)[(string)(*master)] = true
		}
	}
	// Tonco fields
	if v := raw_action.DexDepositLiquidityDataNFTAddress; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DexWithdrawLiquidityDataBurnedNFTAddress; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.ToncoDeployPoolJetton1Minter; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.ToncoDeployPoolJetton0Minter; v != nil {
		(*addr_list)[(string)(*v)] = true
	}

	// Vesting fields
	for _, v := range raw_action.VestingAddWhitelistAccountsAdded {
		(*addr_list)[(string)(v)] = true
	}

	return success
}

func CollectAddressesFromTransactions(addr_list *map[string]bool, tx *models.Transaction) bool {
	success := true

	(*addr_list)[(string)(tx.Account)] = true
	if tx.InMsg != nil {
		if v := tx.InMsg.Source; v != nil {
			(*addr_list)[(string)(*v)] = true
		}
	}
	for idx := range tx.OutMsgs {
		if v := tx.OutMsgs[idx].Destination; v != nil {
			(*addr_list)[(string)(*v)] = true
		}
	}
	return success
}

var isTestnet bool

func SetIsTestnet(v bool) { isTestnet = v }

func ParseRawAction(raw *models.RawAction) (*models.Action, error) {
	var act models.Action

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
	act.Finality = raw.Finality
	act.Type = raw.Type
	act.TraceExternalHash = raw.TraceExternalHash
	act.TraceExternalHashNorm = raw.TraceExternalHashNorm
	act.Accounts = raw.Accounts

	switch act.Type {
	case "call_contract":
		var details models.ActionDetailsCallContract
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
		var details models.ActionDetailsContractDeploy
		details.OpCode = raw.Opcode
		details.Source = raw.Source
		details.Destination = raw.Destination
		details.Value = raw.Value
		act.Details = &details
	case "ton_transfer":
		var details models.ActionDetailsTonTransfer
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
		var details models.ActionDetailsAuctionBid
		details.Bidder = raw.Source
		details.Auction = raw.Destination
		details.Amount = raw.Value
		details.NftItem = raw.AssetSecondary
		details.NftCollection = raw.Asset
		details.NftItemIndex = raw.NFTTransferNFTItemIndex
		act.Details = &details
	case "change_dns":
		var details models.ActionDetailsChangeDns
		details.Key = raw.ChangeDNSRecordKey
		details.Value.SumType = raw.ChangeDNSRecordValueSchema
		details.NFTCollection = raw.Asset
		if raw.ChangeDNSRecordValueSchema != nil {
			switch *raw.ChangeDNSRecordValueSchema {
			case "DNSNextResolver":
				details.Value.DnsNextResolverAddress = raw.ChangeDNSRecordValue
			case "DNSAdnlAddress":
				details.Value.DnsAdnlAddress = raw.ChangeDNSRecordValue
			case "DNSSmcAddress":
				details.Value.DnsSmcAddress = raw.ChangeDNSRecordValue
			case "DNSStorageAddress":
				details.Value.DnsStorageAddress = raw.ChangeDNSRecordValue
			case "DNSText":
				details.Value.DnsStorageAddress = raw.ChangeDNSRecordValue
			}
		}
		details.Value.Flags = raw.ChangeDNSRecordFlags
		details.Asset = raw.Destination
		details.Source = raw.Source
		act.Details = &details
	case "dex_deposit_liquidity":
		var details models.ActionDetailsDexDepositLiquidity
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
		details.TargetAmount1 = raw.DexDepositLiquidityDataTargetAmount1
		details.TargetAsset1 = raw.DexDepositLiquidityDataTargetAsset1
		details.TargetAmount2 = raw.DexDepositLiquidityDataTargetAmount2
		details.TargetAsset2 = raw.DexDepositLiquidityDataTargetAsset2
		details.TickLower = raw.DexDepositLiquidityDataTickLower
		details.TickUpper = raw.DexDepositLiquidityDataTickUpper
		details.NftIndex = raw.DexDepositLiquidityDataNFTIndex
		details.NftAddress = raw.DexDepositLiquidityDataNFTAddress
		details.VaultExcesses = []models.ActionDetailsLiquidityVaultExcess{}
		for _, excess := range raw.DexDepositLiquidityDataVaultExcesses {
			details.VaultExcesses = append(details.VaultExcesses, models.ActionDetailsLiquidityVaultExcess(excess))
		}
		act.Details = &details
	case "dex_withdraw_liquidity":
		var details models.ActionDetailsDexWithdrawLiquidity
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
		details.BurntNftIndex = raw.DexWithdrawLiquidityDataBurnedNFTIndex
		details.BurntNftAddress = raw.DexWithdrawLiquidityDataBurnedNFTAddress
		details.TickUpper = raw.DexWithdrawLiquidityDataTickUpper
		details.TickLower = raw.DexWithdrawLiquidityDataTickLower
		act.Details = &details
	case "delete_dns":
		var details models.ActionDetailsDeleteDns
		details.Key = raw.ChangeDNSRecordKey
		details.Asset = raw.Destination
		details.Source = raw.Source
		details.NFTCollection = raw.Asset
		act.Details = &details
	case "renew_dns":
		var details models.ActionDetailsRenewDns
		details.Asset = raw.Destination
		details.Source = raw.Source
		details.NFTCollection = raw.Asset
		act.Details = &details
	case "election_deposit":
		var details models.ActionDetailsElectionDeposit
		details.StakeHolder = raw.Source
		details.Amount = raw.Amount
		act.Details = &details
	case "election_recover":
		var details models.ActionDetailsElectionRecover
		details.StakeHolder = raw.Source
		details.Amount = raw.Amount
		act.Details = &details
	case "jetton_burn":
		var details models.ActionDetailsJettonBurn
		details.Owner = raw.Source
		details.OwnerJettonWallet = raw.SourceSecondary
		details.Asset = raw.Asset
		details.Amount = raw.Amount
		act.Details = &details
	case "jetton_swap":
		var details models.ActionDetailsJettonSwap
		details.Dex = raw.JettonSwapDex
		details.Sender = raw.Source
		details.AssetIn = raw.Asset
		details.AssetOut = raw.Asset2
		details.DexIncomingTransfer = &models.ActionDetailsJettonSwapTransfer{}
		details.DexOutgoingTransfer = &models.ActionDetailsJettonSwapTransfer{}
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

		details.PeerSwaps = []models.ActionDetailsJettonSwapPeerSwap{}
		for _, peer := range raw.JettonSwapPeerSwaps {
			details.PeerSwaps = append(details.PeerSwaps, models.ActionDetailsJettonSwapPeerSwap(peer))
		}
		// MinOutAmount common field in swaps but for now supported only in tonco swaps, thats why it should appear only for tonco for now
		if details.Dex != nil && *details.Dex == "tonco" {
			act.Details = &models.ActionDetailsToncoJettonSwap{
				Dex:                 details.Dex,
				Sender:              details.Sender,
				AssetIn:             details.AssetIn,
				AssetOut:            details.AssetOut,
				DexIncomingTransfer: details.DexIncomingTransfer,
				DexOutgoingTransfer: details.DexOutgoingTransfer,
				PeerSwaps:           details.PeerSwaps,
				MinOutAmount:        raw.JettonSwapMinOutAmount,
			}
		} else {
			act.Details = &details
		}
	case "jetton_transfer":
		var details models.ActionDetailsJettonTransfer
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
		var details models.ActionDetailsJettonMint
		details.Asset = raw.Asset
		details.Amount = raw.Amount
		details.TonAmount = raw.Value
		details.Receiver = raw.Destination
		details.ReceiverJettonWallet = raw.DestinationSecondary
		act.Details = &details

	case "nft_mint":
		var details models.ActionDetailsNftMint
		details.Owner = raw.Source
		details.NftCollection = raw.Asset
		details.NftItem = raw.AssetSecondary
		details.NftItemIndex = raw.NFTMintNFTItemIndex
		act.Details = &details
	case "nft_transfer", "nft_purchase":
		// TODO: asset = collection, asset_secondary = item, payload, forward_amount, response_dest
		var details models.ActionDetailsNftTransfer
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

		if (raw.NFTTransferIsPurchase != nil && *raw.NFTTransferIsPurchase) || raw.Type == "nft_purchase" {
			if found, marketplaceName := GetMarketplaceName(raw.NFTTransferMarketplaceAddress, raw.Asset); found {
				details.Marketplace = &marketplaceName
			}
		}
		details.RealOldOwner = raw.NFTTransferRealPrevOwner
		details.MarketplaceAddress = raw.NFTTransferMarketplaceAddress
		details.PayoutAmount = raw.NFTTransferPayoutAmount
		details.PayoutComment = raw.NFTTransferPayoutComment
		details.PayoutCommentEncoded = raw.NFTTransferPayoutCommentEncoded
		details.PayoutCommentEncrypted = raw.NFTTransferPayoutCommentEncrypted
		details.RoyaltyAmount = raw.NFTTransferRoyaltyAmount
		if raw.NFTTransferForwardPayload != nil {
			comment, isEncrypted, err := ParseCommentFromPayload(*raw.NFTTransferForwardPayload)
			if err == nil {
				details.Comment = comment
				details.IsEncryptedComment = &isEncrypted
			}
		}
		if raw.Type == "nft_purchase" {
			details.PayoutAddress = details.RealOldOwner
		} else {
			details.PayoutAddress = raw.NFTListingMarketplaceFeeAddress
			details.RoyaltyAddress = raw.NFTListingRoyaltyAddress
		}

		act.Details = &details
	case "dns_purchase":
		var details models.ActionDetailsDnsPurchase
		details.NftCollection = raw.Asset
		details.NftItem = raw.AssetSecondary
		details.NftItemIndex = raw.NFTTransferNFTItemIndex
		details.NewOwner = raw.Destination
		details.Price = raw.NFTTransferPrice
		details.QueryId = raw.NFTTransferQueryId
		details.PayoutAmount = raw.NFTTransferPayoutAmount
		act.Details = &details
	case "nft_put_on_sale":
		var details models.ActionDetailsNftPutOnSale
		details.NftCollection = raw.Asset
		details.NftItem = raw.AssetSecondary
		details.NftItemIndex = raw.NFTListingNFTItemIndex
		details.Owner = raw.Source
		details.ListingAddress = raw.SourceSecondary
		details.SaleAddress = raw.Destination
		details.MarketplaceAddress = raw.DestinationSecondary
		if found, marketplaceName := GetMarketplaceName(raw.NFTTransferMarketplaceAddress, raw.Asset); found {
			details.Marketplace = &marketplaceName
		}
		details.FullPrice = raw.NFTListingFullPrice
		details.MarketplaceFee = raw.NFTListingMarketplaceFee
		details.RoyaltyAmount = raw.NFTListingRoyaltyAmount
		details.MarketplaceFeeAddress = raw.NFTListingMarketplaceFeeAddress
		details.RoyaltyAddress = raw.NFTListingRoyaltyAddress
		act.Details = &details
	case "nft_put_on_auction", "teleitem_start_auction":
		var details models.ActionDetailsNftPutOnAuction
		details.NftCollection = raw.Asset
		details.NftItem = raw.AssetSecondary
		details.NftItemIndex = raw.NFTListingNFTItemIndex
		details.Owner = raw.Source
		details.ListingAddress = raw.SourceSecondary
		details.AuctionAddress = raw.Destination
		details.MarketplaceAddress = raw.DestinationSecondary
		if found, marketplaceName := GetMarketplaceName(raw.DestinationSecondary, raw.Asset); found {
			details.Marketplace = &marketplaceName
		}
		details.MarketplaceFeeFactor = raw.NFTListingMarketplaceFeeFactor
		details.MarketplaceFeeBase = raw.NFTListingMarketplaceFeeBase
		details.RoyaltyFeeBase = raw.NFTListingRoyaltyFeeBase
		details.MaxBid = raw.NFTListingMaxBid
		details.MinBid = raw.NFTListingMinBid
		details.MarketplaceFeeAddress = raw.NFTListingMarketplaceFeeAddress
		details.RoyaltyAddress = raw.NFTListingRoyaltyAddress
		act.Details = &details
	case "tick_tock":
		var details models.ActionDetailsTickTock
		act.Details = &details
	case "stake_deposit":
		var details models.ActionDetailsStakeDeposit
		details.StakeHolder = raw.Source
		details.Amount = raw.Amount
		details.Pool = raw.Destination
		details.Provider = raw.StakingDataProvider
		details.TokensMinted = raw.StakingDataTokensMinted
		details.Asset = raw.Asset
		if details.Provider != nil && *details.Provider == "ethena" {
			details.SourceAsset = raw.Asset2
		}
		act.Details = &details
	case "stake_withdrawal":
		var details models.ActionDetailsWithdrawStake
		details.StakeHolder = raw.Source
		details.Amount = raw.Amount
		details.Pool = raw.Destination
		details.Provider = raw.StakingDataProvider
		details.PayoutNft = raw.StakingDataTsNft
		details.TokensBurnt = raw.StakingDataTokensBurnt
		details.Asset = raw.Asset
		act.Details = &details
	case "stake_withdrawal_request":
		var details models.ActionDetailsWithdrawStakeRequest
		details.StakeHolder = raw.Source
		details.Pool = raw.Destination
		details.Provider = raw.StakingDataProvider
		details.PayoutNft = raw.StakingDataTsNft
		details.Asset = raw.Asset
		if details.Provider != nil && *details.Provider == "tonstakers" {
			details.TokensBurnt = raw.Amount
		} else if details.Provider != nil && *details.Provider == "ethena" {
			details.TokensMinted = raw.StakingDataTokensMinted
		}
		act.Details = &details
	case "subscribe":
		var details models.ActionDetailsSubscribe
		details.Subscriber = raw.Source
		details.Beneficiary = raw.Destination
		details.Subscription = raw.DestinationSecondary
		details.Amount = raw.Amount
		act.Details = &details
	case "unsubscribe":
		var details models.ActionDetailsUnsubscribe
		details.Subscriber = raw.Source
		details.Beneficiary = raw.Destination
		details.Subscription = raw.DestinationSecondary
		details.Amount = raw.Amount
		act.Details = &details
	case "multisig_create_order":
		act.Details = models.ActionDetailsMultisigCreateOrder{
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
		act.Details = models.ActionDetailsMultisigApprove{
			SignerIndex: raw.MultisigApproveSignerIndex,
			ExitCode:    raw.MultisigApproveExitCode,
			Source:      raw.Source,
			Destination: raw.Destination,
		}
	case "multisig_execute":
		act.Details = models.ActionDetailsMultisigExecute{
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
		act.Details = models.ActionDetailsVestingSendMessage{
			QueryId:     raw.VestingSendMessageQueryId,
			MessageBoc:  raw.VestingSendMessageMessageBoc,
			Source:      raw.Source,
			Vesting:     raw.Destination,
			Destination: raw.DestinationSecondary,
			Amount:      raw.Amount,
		}
	case "vesting_add_whitelist":
		act.Details = models.ActionDetailsVestingAddWhitelist{
			QueryId:       raw.VestingAddWhitelistQueryId,
			AccountsAdded: raw.VestingAddWhitelistAccountsAdded,
			Source:        raw.Source,
			Vesting:       raw.Destination,
		}
	case "evaa_supply":
		act.Details = models.ActionDetailsEvaaSupply{
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
		act.Details = models.ActionDetailsEvaaWithdraw{
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
		var asset *string
		var knownAsset bool
		if raw.EvaaLiquidateAssetId != nil {
			asset, knownAsset = ParseEvaaAssetId(*raw.EvaaLiquidateAssetId)
		}
		act.Details = models.ActionDetailsEvaaLiquidate{
			FailReason:       raw.EvaaLiquidateFailReason,
			DebtAmount:       raw.EvaaLiquidateDebtAmount,
			Source:           raw.Source,
			Borrower:         raw.Destination,
			BorrowerContract: raw.DestinationSecondary,
			Collateral:       raw.Asset,
			AssetId:          raw.EvaaLiquidateAssetId,
			Amount:           raw.Amount,
			Asset:            (*models.AccountAddress)(asset),
			IsKnownAsset:     knownAsset,
		}
	case "jvault_claim":
		claimedRewards := make([]models.JettonAmountPair, 0)
		if len(raw.JvaultClaimClaimedJettons) == len(raw.JvaultClaimClaimedAmounts) {
			for i := range raw.JvaultClaimClaimedJettons {
				claimedRewards = append(claimedRewards, models.JettonAmountPair{
					Jetton: &raw.JvaultClaimClaimedJettons[i],
					Amount: &raw.JvaultClaimClaimedAmounts[i],
				})
			}
		}
		act.Details = models.ActionDetailsJvaultClaim{
			ClaimedRewards: claimedRewards,
			Source:         raw.Source,
			StakeWallet:    raw.SourceSecondary,
			Pool:           raw.Destination,
		}
	case "jvault_stake":
		act.Details = models.ActionDetailsJvaultStake{
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
		act.Details = models.ActionDetailsJvaultUnstake{
			Source:       raw.Source,
			StakeWallet:  raw.SourceSecondary,
			Pool:         raw.Destination,
			Amount:       raw.Amount,
			ExitCode:     (*int64)(raw.Opcode),
			Asset:        raw.Asset,
			StakingAsset: raw.Asset2,
		}
	case "jvault_unstake_request":
		act.Details = models.ActionDetailsJvaultUnstakeRequest{
			Source:       raw.Source,
			StakeWallet:  raw.SourceSecondary,
			Pool:         raw.Destination,
			Amount:       raw.Amount,
			ExitCode:     (*int64)(raw.Opcode),
			Asset:        raw.Asset,
			StakingAsset: raw.Asset2,
		}
	case "nft_discovery":
		act.Details = models.ActionDetailsNftDiscovery{
			Source:        raw.Source,
			NftItem:       raw.AssetSecondary,
			NftCollection: raw.Asset,
			NftItemIndex:  raw.NFTTransferNFTItemIndex,
		}
	case "tgbtc_mint", "tgbtc_mint_fallback":
		var details models.ActionDetailsTgbtcMint
		details.Source = raw.Source
		details.Destination = raw.Destination
		details.Amount = raw.Amount
		details.Asset = raw.Asset
		details.BitcoinTxId = (*string)(raw.AssetSecondary)
		details.DestinationWallet = raw.DestinationSecondary
		act.Details = &details
	case "tgbtc_burn", "tgbtc_burn_fallback":
		var details models.ActionDetailsTgbtcBurn
		details.Source = raw.Source
		details.SourceWallet = raw.SourceSecondary
		details.Destination = raw.Destination
		details.Amount = raw.Amount
		details.Asset = raw.Asset
		act.Details = &details
	case "tgbtc_new_key", "tgbtc_new_key_fallback":
		var details models.ActionDetailsTgbtcNewKey
		details.Source = raw.Source
		details.Pubkey = (*string)(raw.SourceSecondary)
		details.Coordinator = raw.Destination
		details.Pegout = raw.DestinationSecondary
		details.Amount = raw.Amount
		details.Asset = raw.Asset
		act.Details = &details
	case "tgbtc_dkg_log_fallback":
		var details models.ActionDetailsDkgLogFallback
		details.Coordinator = raw.Source
		details.Pubkey = (*string)(raw.Asset)
		details.Timestamp = raw.Value
		act.Details = &details
	case "tonco_deploy_pool":
		act.Details = models.ActionDetailsToncoDeployPool{
			Source:              raw.Source,
			Pool:                raw.DestinationSecondary,
			Router:              raw.Destination,
			RouterJettonWallet1: raw.ToncoDeployPoolJetton0RouterWallet,
			RouterJettonWallet2: raw.ToncoDeployPoolJetton1RouterWallet,
			JettonMinter1:       raw.ToncoDeployPoolJetton0Minter,
			JettonMinter2:       raw.ToncoDeployPoolJetton1Minter,
			TickSpacing:         raw.ToncoDeployPoolTickSpacing,
			InitialPriceX96:     raw.ToncoDeployPoolInitialPriceX96,
			ProtocolFee:         raw.ToncoDeployPoolProtocolFee,
			LpFeeBase:           raw.ToncoDeployPoolLpFeeBase,
			LpFeeCurrent:        raw.ToncoDeployPoolLpFeeCurrent,
			PoolActive:          raw.ToncoDeployPoolPoolActive,
		}
	case "coffee_create_pool":
		act.Details = models.ActionDetailsCoffeeCreatePool{
			Source:              raw.Source,
			SourceJettonWallet:  raw.SourceSecondary,
			Initiator1:          raw.CoffeeCreatePoolInitiator1,
			Initiator2:          raw.CoffeeCreatePoolInitiator2,
			ProvidedAsset:       raw.CoffeeCreatePoolProvidedAsset,
			Amount:              raw.Amount,
			Pool:                raw.Destination,
			Asset1:              raw.Asset,
			Asset2:              raw.Asset2,
			Amount1:             raw.CoffeeCreatePoolAmount1,
			Amount2:             raw.CoffeeCreatePoolAmount2,
			LpTokensMinted:      raw.CoffeeCreatePoolLpTokensMinted,
			PoolCreatorContract: raw.CoffeeCreatePoolPoolCreatorContract,
		}
	case "coffee_create_pool_creator":
		act.Details = models.ActionDetailsCoffeeCreatePoolCreator{
			Source:              raw.Source,
			SourceJettonWallet:  raw.SourceSecondary,
			DepositRecipient:    raw.Destination,
			PoolCreatorContract: raw.DestinationSecondary,
			ProvidedAsset:       raw.CoffeeCreatePoolProvidedAsset,
			Asset1:              raw.Asset,
			Asset2:              raw.Asset2,
			Amount:              raw.Amount,
		}
	case "coffee_staking_deposit":
		act.Details = models.ActionDetailsCoffeeStakingDeposit{
			Source:             raw.Source,
			SourceJettonWallet: raw.SourceSecondary,
			Pool:               raw.Destination,
			PoolJettonWallet:   raw.DestinationSecondary,
			Asset:              raw.Asset,
			Amount:             raw.Amount,
			MintedItemAddress:  raw.CoffeeStakingDepositMintedItemAddress,
			MintedItemIndex:    raw.CoffeeStakingDepositMintedItemIndex,
		}
	case "coffee_staking_withdraw":
		act.Details = models.ActionDetailsCoffeeStakingWithdraw{
			Source:             raw.Source,
			SourceJettonWallet: raw.SourceSecondary,
			Pool:               raw.Destination,
			PoolJettonWallet:   raw.DestinationSecondary,
			Asset:              raw.Asset,
			Amount:             raw.Amount,
			NftAddress:         raw.CoffeeStakingWithdrawNftAddress,
			NftIndex:           raw.CoffeeStakingWithdrawNftIndex,
			Points:             raw.CoffeeStakingWithdrawPoints,
		}
	case "coffee_staking_claim_rewards":
		act.Details = models.ActionDetailsCoffeeStakingClaimRewards{
			Pool:                  raw.Source,
			PoolJettonWallet:      raw.SourceSecondary,
			Recipient:             raw.Destination,
			RecipientJettonWallet: raw.DestinationSecondary,
			Asset:                 raw.Asset,
			Amount:                raw.Amount,
		}
	case "coffee_mev_protect_hold_funds":
		act.Details = models.ActionDetailsCoffeeMevProtectHoldFunds{
			Source:                  raw.Source,
			SourceJettonWallet:      raw.SourceSecondary,
			MevContract:             raw.Destination,
			MevContractJettonWallet: raw.DestinationSecondary,
			Asset:                   raw.Asset,
			Amount:                  raw.Amount,
		}
	case "coffee_create_vault":
		act.Details = models.ActionDetailsCoffeeCreateVault{
			Source: raw.Source,
			Vault:  raw.Destination,
			Asset:  raw.Asset,
			Value:  raw.Value,
		}
	case "auction_outbid":
		var details models.ActionDetailsAuctionOutbid
		details.AuctionAddress = raw.Source
		details.Bidder = raw.Destination
		details.NewBidder = raw.SourceSecondary
		details.NftItem = raw.AssetSecondary
		details.NftCollection = raw.Asset
		details.Amount = raw.Amount
		details.Comment = raw.TonTransferContent
		if found, marketplaceName := GetMarketplaceName(raw.NFTTransferMarketplaceAddress, raw.Asset); found {
			details.Marketplace = &marketplaceName
		}
		act.Details = &details
	case "nft_cancel_sale":
		var details models.ActionDetailsNftCancelSale
		details.Owner = raw.Source
		details.SaleAddress = raw.Destination
		details.NftItem = raw.AssetSecondary
		details.NftCollection = raw.Asset
		details.MarketplaceAddress = raw.NFTTransferMarketplaceAddress
		if found, marketplaceName := GetMarketplaceName(raw.NFTTransferMarketplaceAddress, raw.Asset); found {
			details.Marketplace = &marketplaceName
		}
		act.Details = &details
	case "nft_cancel_auction", "teleitem_cancel_auction":
		var details models.ActionDetailsNftCancelAuction
		details.Owner = raw.Source
		details.AuctionAddress = raw.Destination
		details.NftItem = raw.AssetSecondary
		details.NftCollection = raw.Asset
		details.MarketplaceAddress = raw.NFTTransferMarketplaceAddress
		if found, marketplaceName := GetMarketplaceName(raw.NFTTransferMarketplaceAddress, raw.Asset); found {
			details.Marketplace = &marketplaceName
		}
		act.Details = &details
	case "nft_finish_auction":
		var details models.ActionDetailsNftFinishAuction
		details.Owner = raw.Source
		details.AuctionAddress = raw.Destination
		details.NftItem = raw.AssetSecondary
		details.NftCollection = raw.Asset
		details.MarketplaceAddress = raw.NFTTransferMarketplaceAddress
		if found, marketplaceName := GetMarketplaceName(raw.NFTTransferMarketplaceAddress, raw.Asset); found {
			details.Marketplace = &marketplaceName
		}
		act.Details = &details
	case "dns_release":
		var details models.ActionDetailsDnsRelease
		details.QueryId = raw.NFTTransferQueryId
		details.Source = raw.Source
		details.NftItem = raw.Destination
		details.NftCollection = raw.Asset
		details.NftItemIndex = raw.NFTTransferNFTItemIndex
		details.Value = raw.Value
		act.Details = &details
	case "nft_update_sale":
		var details models.ActionDetailsNftUpdateSale
		details.Source = raw.Source
		details.SaleContract = raw.Destination
		details.NftAddress = raw.AssetSecondary
		details.MarketplaceAddress = raw.NFTTransferMarketplaceAddress
		details.FullPrice = raw.NFTListingFullPrice
		details.MarketplaceFee = raw.NFTListingMarketplaceFee
		details.RoyaltyAmount = raw.NFTListingRoyaltyAmount
		if found, marketplaceName := GetMarketplaceName(raw.NFTTransferMarketplaceAddress, raw.Asset); found {
			details.Marketplace = &marketplaceName
		}
		act.Details = &details
	case "layerzero_send":
		details := models.ActionDetailsLayerZeroSend{}
		details.Sender = raw.Source
		details.LayerzeroSend = models.ActionDetailsLayerZeroSendDetails{
			SendRequestId: raw.LayerzeroSendSendRequestId,
			MsglibManager: raw.LayerzeroSendMsglibManager,
			Msglib:        raw.LayerzeroSendMsglib,
			Uln:           raw.LayerzeroSendUln,
			NativeFee:     raw.LayerzeroSendNativeFee,
			ZroFee:        raw.LayerzeroSendZroFee,
			Endpoint:      raw.LayerzeroSendEndpoint,
			Channel:       raw.LayerzeroSendChannel,
		}
		details.LayerzeroPacket = models.ActionDetailsLayerZeroPacket{
			SrcOapp: raw.LayerzeroPacketSrcOapp,
			DstOapp: raw.LayerzeroPacketDstOapp,
			SrcEid:  raw.LayerzeroPacketSrcEid,
			DstEid:  raw.LayerzeroPacketDstEid,
			Nonce:   raw.LayerzeroPacketNonce,
			Guid:    raw.LayerzeroPacketGuid,
			Message: raw.LayerzeroPacketMessage,
		}
		act.Details = details
	case "layerzero_send_tokens":
		detailsTokens := models.ActionDetailsLayerZeroSendTokens{}
		detailsTokens.Sender = raw.Source
		detailsTokens.SenderWallet = raw.SourceSecondary
		detailsTokens.Oapp = raw.Destination
		detailsTokens.OappWallet = raw.DestinationSecondary
		detailsTokens.Asset = raw.Asset
		detailsTokens.Amount = raw.Amount
		detailsTokens.LayerzeroSend = models.ActionDetailsLayerZeroSendDetails{
			SendRequestId: raw.LayerzeroSendSendRequestId,
			MsglibManager: raw.LayerzeroSendMsglibManager,
			Msglib:        raw.LayerzeroSendMsglib,
			Uln:           raw.LayerzeroSendUln,
			NativeFee:     raw.LayerzeroSendNativeFee,
			ZroFee:        raw.LayerzeroSendZroFee,
			Endpoint:      raw.LayerzeroSendEndpoint,
			Channel:       raw.LayerzeroSendChannel,
		}
		detailsTokens.LayerzeroPacket = models.ActionDetailsLayerZeroPacket{
			SrcOapp: raw.LayerzeroPacketSrcOapp,
			DstOapp: raw.LayerzeroPacketDstOapp,
			SrcEid:  raw.LayerzeroPacketSrcEid,
			DstEid:  raw.LayerzeroPacketDstEid,
			Nonce:   raw.LayerzeroPacketNonce,
			Guid:    raw.LayerzeroPacketGuid,
			Message: raw.LayerzeroPacketMessage,
		}
		act.Details = detailsTokens
	case "layerzero_receive":
		details := models.ActionDetailsLayerZeroReceive{}
		details.Sender = raw.Source
		details.Oapp = raw.Destination
		details.Channel = raw.DestinationSecondary
		details.LayerzeroPacket = models.ActionDetailsLayerZeroPacket{
			SrcOapp: raw.LayerzeroPacketSrcOapp,
			DstOapp: raw.LayerzeroPacketDstOapp,
			SrcEid:  raw.LayerzeroPacketSrcEid,
			DstEid:  raw.LayerzeroPacketDstEid,
			Nonce:   raw.LayerzeroPacketNonce,
			Guid:    raw.LayerzeroPacketGuid,
			Message: raw.LayerzeroPacketMessage,
		}
		act.Details = details
	case "layerzero_commit_packet":
		details := models.ActionDetailsLayerZeroCommitPacket{}
		details.Sender = raw.Source
		details.Endpoint = raw.SourceSecondary
		details.Uln = raw.Destination
		details.UlnConnection = raw.DestinationSecondary
		details.Channel = raw.Asset
		details.MsglibConnection = raw.AssetSecondary
		details.LayerzeroPacket = models.ActionDetailsLayerZeroPacket{
			SrcOapp: raw.LayerzeroPacketSrcOapp,
			DstOapp: raw.LayerzeroPacketDstOapp,
			SrcEid:  raw.LayerzeroPacketSrcEid,
			DstEid:  raw.LayerzeroPacketDstEid,
			Nonce:   raw.LayerzeroPacketNonce,
			Guid:    raw.LayerzeroPacketGuid,
			Message: raw.LayerzeroPacketMessage,
		}
		act.Details = details
	case "layerzero_dvn_verify":
		details := models.ActionDetailsLayerZeroDvnVerify{
			Initiator:     raw.Source,
			Nonce:         raw.LayerzeroDvnVerifyNonce,
			Status:        raw.LayerzeroDvnVerifyStatus,
			Dvn:           raw.LayerzeroDvnVerifyDvn,
			Proxy:         raw.LayerzeroDvnVerifyProxy,
			Uln:           raw.LayerzeroDvnVerifyUln,
			UlnConnection: raw.LayerzeroDvnVerifyUlnConnection,
		}
		act.Details = details
	case "cocoon_worker_payout":
		act.Details = models.ActionDetailsCocoonWorkerPayout{
			PayoutType:   raw.CocoonWorkerPayoutPayoutType,
			QueryId:      raw.CocoonWorkerPayoutQueryId,
			NewTokens:    raw.CocoonWorkerPayoutNewTokens,
			WorkerState:  raw.CocoonWorkerPayoutWorkerState,
			WorkerTokens: raw.CocoonWorkerPayoutWorkerTokens,
			Source:       raw.Source,
			Destination:  raw.Destination,
			Amount:       raw.Amount,
		}
	case "cocoon_proxy_payout":
		act.Details = models.ActionDetailsCocoonProxyPayout{
			QueryId:     raw.CocoonProxyPayoutQueryId,
			Source:      raw.Source,
			Destination: raw.Destination,
		}
	case "cocoon_proxy_charge":
		act.Details = models.ActionDetailsCocoonProxyCharge{
			QueryId:         raw.CocoonProxyChargeQueryId,
			NewTokensUsed:   raw.CocoonProxyChargeNewTokensUsed,
			ExpectedAddress: raw.CocoonProxyChargeExpectedAddress,
			Source:          raw.Source,
			Destination:     raw.Destination,
		}
	case "cocoon_client_top_up":
		act.Details = models.ActionDetailsCocoonClientTopUp{
			QueryId:     raw.CocoonClientTopUpQueryId,
			Source:      raw.Source,
			Destination: raw.Destination,
			Amount:      raw.Amount,
		}
	case "cocoon_register_proxy":
		act.Details = models.ActionDetailsCocoonRegisterProxy{
			QueryId:     raw.CocoonRegisterProxyQueryId,
			Destination: raw.Destination,
		}
	case "cocoon_unregister_proxy":
		act.Details = models.ActionDetailsCocoonUnregisterProxy{
			QueryId:     raw.CocoonUnregisterProxyQueryId,
			Seqno:       raw.CocoonUnregisterProxySeqno,
			Destination: raw.Destination,
		}
	case "cocoon_client_register":
		act.Details = models.ActionDetailsCocoonClientRegister{
			QueryId:     raw.CocoonClientRegisterQueryId,
			Nonce:       raw.CocoonClientRegisterNonce,
			Source:      raw.Source,
			Destination: raw.Destination,
		}
	case "cocoon_client_change_secret_hash":
		act.Details = models.ActionDetailsCocoonClientChangeSecretHash{
			QueryId:       raw.CocoonClientChangeSecretHashQueryId,
			NewSecretHash: raw.CocoonClientChangeSecretHashNewSecretHash,
			Source:        raw.Source,
			Destination:   raw.Destination,
		}
	case "cocoon_client_request_refund":
		act.Details = models.ActionDetailsCocoonClientRequestRefund{
			QueryId:     raw.CocoonClientRequestRefundQueryId,
			ViaWallet:   raw.CocoonClientRequestRefundViaWallet,
			Source:      raw.Source,
			Destination: raw.Destination,
		}
	case "cocoon_grant_refund":
		act.Details = models.ActionDetailsCocoonGrantRefund{
			QueryId:         raw.CocoonGrantRefundQueryId,
			NewTokensUsed:   raw.CocoonGrantRefundNewTokensUsed,
			ExpectedAddress: raw.CocoonGrantRefundExpectedAddress,
			Source:          raw.Source,
			Destination:     raw.Destination,
			Amount:          raw.Amount,
		}
	case "cocoon_client_increase_stake":
		act.Details = models.ActionDetailsCocoonClientIncreaseStake{
			QueryId:     raw.CocoonClientIncreaseStakeQueryId,
			NewStake:    raw.CocoonClientIncreaseStakeNewStake,
			Source:      raw.Source,
			Destination: raw.Destination,
			Amount:      raw.Amount,
		}
	case "cocoon_client_withdraw":
		act.Details = models.ActionDetailsCocoonClientWithdraw{
			QueryId:        raw.CocoonClientWithdrawQueryId,
			WithdrawAmount: raw.CocoonClientWithdrawWithdrawAmount,
			Source:         raw.Source,
			Destination:    raw.Destination,
			Amount:         raw.Amount,
		}
	default:
		details := map[string]string{}
		details["error"] = fmt.Sprintf("unsupported action type: '%s'", act.Type)
		act.Details = &details
		act.RawAction = raw
	}
	act.AncestorType = raw.AncestorType
	return &act, nil
}
