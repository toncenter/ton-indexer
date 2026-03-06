package models

import "github.com/toncenter/ton-indexer/ton-index-go/index/emulated"

type RawAction struct {
	TraceId                                              *HashType
	ActionId                                             HashType
	StartLt                                              int64
	EndLt                                                int64
	StartUtime                                           int64
	EndUtime                                             int64
	TraceEndLt                                           int64
	TraceEndUtime                                        int64
	TraceMcSeqnoEnd                                      int32
	Source                                               *AccountAddress
	SourceSecondary                                      *AccountAddress
	Destination                                          *AccountAddress
	DestinationSecondary                                 *AccountAddress
	Asset                                                *AccountAddress
	AssetSecondary                                       *AccountAddress
	Asset2                                               *AccountAddress
	Asset2Secondary                                      *AccountAddress
	Opcode                                               *OpcodeType
	TxHashes                                             []HashType
	Type                                                 string
	TonTransferContent                                   *string
	TonTransferEncrypted                                 *bool
	Value                                                *string
	Amount                                               *string
	JettonTransferResponseDestination                    *AccountAddress
	JettonTransferForwardAmount                          *string
	JettonTransferQueryId                                *string
	JettonTransferCustomPayload                          *string
	JettonTransferForwardPayload                         *string
	JettonTransferComment                                *string
	JettonTransferIsEncryptedComment                     *bool
	NFTTransferIsPurchase                                *bool
	NFTTransferPrice                                     *string
	NFTTransferQueryId                                   *string
	NFTTransferCustomPayload                             *string
	NFTTransferForwardPayload                            *string
	NFTTransferForwardAmount                             *string
	NFTTransferResponseDestination                       *AccountAddress
	NFTTransferNFTItemIndex                              *string
	NFTTransferMarketplace                               *string
	NFTTransferRealPrevOwner                             *AccountAddress
	NFTTransferMarketplaceAddress                        *AccountAddress
	NFTTransferPayoutAmount                              *string
	NFTTransferPayoutCommentEncrypted                    *bool
	NFTTransferPayoutCommentEncoded                      *bool
	NFTTransferPayoutComment                             *string
	NFTTransferRoyaltyAmount                             *string
	NFTListingNFTItemIndex                               *string
	NFTListingFullPrice                                  *string
	NFTListingMarketplaceFee                             *string
	NFTListingRoyaltyAmount                              *string
	NFTListingMarketplaceFeeFactor                       *string
	NFTListingMarketplaceFeeBase                         *string
	NFTListingRoyaltyFeeBase                             *string
	NFTListingMaxBid                                     *string
	NFTListingMinBid                                     *string
	NFTListingMarketplaceFeeAddress                      *AccountAddress
	NFTListingRoyaltyAddress                             *AccountAddress
	NFTListingMarketplace                                *string
	JettonSwapDex                                        *string
	JettonSwapSender                                     *AccountAddress
	JettonSwapDexIncomingTransferAmount                  *string
	JettonSwapDexIncomingTransferAsset                   *AccountAddress
	JettonSwapDexIncomingTransferSource                  *AccountAddress
	JettonSwapDexIncomingTransferDestination             *AccountAddress
	JettonSwapDexIncomingTransferSourceJettonWallet      *AccountAddress
	JettonSwapDexIncomingTransferDestinationJettonWallet *AccountAddress
	JettonSwapDexOutgoingTransferAmount                  *string
	JettonSwapDexOutgoingTransferAsset                   *AccountAddress
	JettonSwapDexOutgoingTransferSource                  *AccountAddress
	JettonSwapDexOutgoingTransferDestination             *AccountAddress
	JettonSwapDexOutgoingTransferSourceJettonWallet      *AccountAddress
	JettonSwapDexOutgoingTransferDestinationJettonWallet *AccountAddress
	JettonSwapPeerSwaps                                  []RawActionJettonSwapPeerSwap
	JettonSwapMinOutAmount                               *string
	ChangeDNSRecordKey                                   *string
	ChangeDNSRecordValueSchema                           *string
	ChangeDNSRecordValue                                 *string
	ChangeDNSRecordFlags                                 *int64
	NFTMintNFTItemIndex                                  *string
	DexWithdrawLiquidityDataDex                          *string
	DexWithdrawLiquidityDataAmount1                      *string
	DexWithdrawLiquidityDataAmount2                      *string
	DexWithdrawLiquidityDataAsset1Out                    *AccountAddress
	DexWithdrawLiquidityDataAsset2Out                    *AccountAddress
	DexWithdrawLiquidityDataUserJettonWallet1            *AccountAddress
	DexWithdrawLiquidityDataUserJettonWallet2            *AccountAddress
	DexWithdrawLiquidityDataDexJettonWallet1             *AccountAddress
	DexWithdrawLiquidityDataDexJettonWallet2             *AccountAddress
	DexWithdrawLiquidityDataLpTokensBurnt                *string
	DexWithdrawLiquidityDataBurnedNFTIndex               *string
	DexWithdrawLiquidityDataBurnedNFTAddress             *AccountAddress
	DexWithdrawLiquidityDataTickLower                    *string
	DexWithdrawLiquidityDataTickUpper                    *string
	DexDepositLiquidityDataDex                           *string
	DexDepositLiquidityDataAmount1                       *string
	DexDepositLiquidityDataAmount2                       *string
	DexDepositLiquidityDataAsset1                        *AccountAddress
	DexDepositLiquidityDataAsset2                        *AccountAddress
	DexDepositLiquidityDataUserJettonWallet1             *AccountAddress
	DexDepositLiquidityDataUserJettonWallet2             *AccountAddress
	DexDepositLiquidityDataLpTokensMinted                *string
	DexDepositLiquidityDataTargetAsset1                  *AccountAddress
	DexDepositLiquidityDataTargetAsset2                  *AccountAddress
	DexDepositLiquidityDataTargetAmount1                 *string
	DexDepositLiquidityDataTargetAmount2                 *string
	DexDepositLiquidityDataVaultExcesses                 []RawActionVaultExcessEntry
	DexDepositLiquidityDataTickLower                     *string
	DexDepositLiquidityDataTickUpper                     *string
	DexDepositLiquidityDataNFTIndex                      *string
	DexDepositLiquidityDataNFTAddress                    *AccountAddress
	StakingDataProvider                                  *string
	StakingDataTsNft                                     *AccountAddress
	StakingDataTokensBurnt                               *string
	StakingDataTokensMinted                              *string
	Success                                              *bool
	Finality                                             emulated.FinalityState
	TraceExternalHash                                    *HashType
	TraceExternalHashNorm                                *HashType
	ExtraCurrencies                                      map[string]string
	MultisigCreateOrderQueryId                           *string
	MultisigCreateOrderOrderSeqno                        *string
	MultisigCreateOrderIsCreatedBySigner                 *bool
	MultisigCreateOrderIsSignedByCreator                 *bool
	MultisigCreateOrderCreatorIndex                      *int64
	MultisigCreateOrderExpirationDate                    *int64
	MultisigCreateOrderOrderBoc                          *string
	MultisigApproveSignerIndex                           *int64
	MultisigApproveExitCode                              *int32
	MultisigExecuteQueryId                               *string
	MultisigExecuteOrderSeqno                            *string
	MultisigExecuteExpirationDate                        *int64
	MultisigExecuteApprovalsNum                          *int64
	MultisigExecuteSignersHash                           *string
	MultisigExecuteOrderBoc                              *string
	VestingSendMessageQueryId                            *string
	VestingSendMessageMessageBoc                         *string
	VestingAddWhitelistQueryId                           *string
	VestingAddWhitelistAccountsAdded                     []AccountAddress
	EvaaSupplySenderJettonWallet                         *AccountAddress
	EvaaSupplyRecipientJettonWallet                      *AccountAddress
	EvaaSupplyMasterJettonWallet                         *AccountAddress
	EvaaSupplyMaster                                     *AccountAddress
	EvaaSupplyAssetId                                    *string
	EvaaSupplyIsTon                                      *bool
	EvaaWithdrawRecipientJettonWallet                    *AccountAddress
	EvaaWithdrawMasterJettonWallet                       *AccountAddress
	EvaaWithdrawMaster                                   *AccountAddress
	EvaaWithdrawFailReason                               *string
	EvaaWithdrawAssetId                                  *string
	EvaaLiquidateFailReason                              *string
	EvaaLiquidateDebtAmount                              *string
	EvaaLiquidateAssetId                                 *string
	JvaultClaimClaimedJettons                            []AccountAddress
	JvaultClaimClaimedAmounts                            []string
	JvaultStakePeriod                                    *int64
	JvaultStakeMintedStakeJettons                        *string
	JvaultStakeStakeWallet                               *AccountAddress
	ToncoDeployPoolJetton0RouterWallet                   *AccountAddress
	ToncoDeployPoolJetton1RouterWallet                   *AccountAddress
	ToncoDeployPoolJetton0Minter                         *AccountAddress
	ToncoDeployPoolJetton1Minter                         *AccountAddress
	ToncoDeployPoolTickSpacing                           *string
	ToncoDeployPoolInitialPriceX96                       *string
	ToncoDeployPoolProtocolFee                           *string
	ToncoDeployPoolLpFeeBase                             *string
	ToncoDeployPoolLpFeeCurrent                          *string
	ToncoDeployPoolPoolActive                            *bool
	CoffeeCreatePoolAmount1                              *string
	CoffeeCreatePoolAmount2                              *string
	CoffeeCreatePoolInitiator1                           *AccountAddress
	CoffeeCreatePoolInitiator2                           *AccountAddress
	CoffeeCreatePoolProvidedAsset                        *AccountAddress
	CoffeeCreatePoolLpTokensMinted                       *string
	CoffeeCreatePoolPoolCreatorContract                  *AccountAddress
	CoffeeStakingDepositMintedItemAddress                *AccountAddress
	CoffeeStakingDepositMintedItemIndex                  *string
	CoffeeStakingWithdrawNftAddress                      *AccountAddress
	CoffeeStakingWithdrawNftIndex                        *string
	CoffeeStakingWithdrawPoints                          *string
	LayerzeroSendSendRequestId                           *uint64
	LayerzeroSendMsglibManager                           *string
	LayerzeroSendMsglib                                  *string
	LayerzeroSendUln                                     *AccountAddress
	LayerzeroSendNativeFee                               *uint64
	LayerzeroSendZroFee                                  *uint64
	LayerzeroSendEndpoint                                *AccountAddress
	LayerzeroSendChannel                                 *AccountAddress
	LayerzeroPacketSrcOapp                               *string
	LayerzeroPacketDstOapp                               *string
	LayerzeroPacketSrcEid                                *int32
	LayerzeroPacketDstEid                                *int32
	LayerzeroPacketNonce                                 *int64
	LayerzeroPacketGuid                                  *string
	LayerzeroPacketMessage                               *string
	LayerzeroDvnVerifyNonce                              *int64
	LayerzeroDvnVerifyStatus                             *string
	LayerzeroDvnVerifyDvn                                *AccountAddress
	LayerzeroDvnVerifyProxy                              *AccountAddress
	LayerzeroDvnVerifyUln                                *AccountAddress
	LayerzeroDvnVerifyUlnConnection                      *AccountAddress

	// COCOON fields
	CocoonWorkerPayoutPayoutType              *string
	CocoonWorkerPayoutQueryId                 *string
	CocoonWorkerPayoutNewTokens               *string
	CocoonWorkerPayoutWorkerState             *int64
	CocoonWorkerPayoutWorkerTokens            *string
	CocoonProxyPayoutQueryId                  *string
	CocoonProxyChargeQueryId                  *string
	CocoonProxyChargeNewTokensUsed            *string
	CocoonProxyChargeExpectedAddress          *string
	CocoonClientTopUpQueryId                  *string
	CocoonRegisterProxyQueryId                *string
	CocoonUnregisterProxyQueryId              *string
	CocoonUnregisterProxySeqno                *int64
	CocoonClientRegisterQueryId               *string
	CocoonClientRegisterNonce                 *string
	CocoonClientChangeSecretHashQueryId       *string
	CocoonClientChangeSecretHashNewSecretHash *string
	CocoonClientRequestRefundQueryId          *string
	CocoonClientRequestRefundViaWallet        *bool
	CocoonGrantRefundQueryId                  *string
	CocoonGrantRefundNewTokensUsed            *string
	CocoonGrantRefundExpectedAddress          *string
	CocoonClientIncreaseStakeQueryId          *string
	CocoonClientIncreaseStakeNewStake         *string
	CocoonClientWithdrawQueryId               *string
	CocoonClientWithdrawWithdrawAmount        *string

	AncestorType []string
	Accounts     []string
} // @name RawAction

type ActionDetailsCallContract struct {
	OpCode          *OpcodeType        `json:"opcode,omitempty"`
	Source          *AccountAddress    `json:"source,omitempty"`
	Destination     *AccountAddress    `json:"destination,omitempty"`
	Value           *string            `json:"value,omitempty"`
	ExtraCurrencies *map[string]string `json:"extra_currencies,omitempty"`
}

type ActionDetailsContractDeploy struct {
	OpCode      *OpcodeType     `json:"opcode,omitempty"`
	Source      *AccountAddress `json:"source,omitempty"`
	Destination *AccountAddress `json:"destination,omitempty"`
	Value       *string         `json:"value,omitempty"`
}

type ActionDetailsTonTransfer struct {
	Source          *AccountAddress    `json:"source"`
	Destination     *AccountAddress    `json:"destination"`
	Value           *string            `json:"value"`
	ExtraCurrencies *map[string]string `json:"value_extra_currencies,omitempty"`
	Comment         *string            `json:"comment"`
	Encrypted       *bool              `json:"encrypted"`
}

type ActionDetailsAuctionBid struct {
	Amount        *string         `json:"amount"`
	Bidder        *AccountAddress `json:"bidder"`
	Auction       *AccountAddress `json:"auction"`
	NftItem       *AccountAddress `json:"nft_item"`
	NftCollection *AccountAddress `json:"nft_collection"`
	NftItemIndex  *string         `json:"nft_item_index"`
}

type ActionDetailsChangeDnsValue struct {
	SumType                *string `json:"sum_type"`
	DnsSmcAddress          *string `json:"dns_smc_address"`
	DnsAdnlAddress         *string `json:"dns_adnl_address"`
	DnsText                *string `json:"dns_text"`
	DnsNextResolverAddress *string `json:"dns_next_resolver_address"`
	DnsStorageAddress      *string `json:"dns_storage_address"`
	Flags                  *int64  `json:"flags"`
}

type ActionDetailsChangeDns struct {
	Key           *string                     `json:"key"`
	Value         ActionDetailsChangeDnsValue `json:"value"`
	Source        *AccountAddress             `json:"source"`
	Asset         *AccountAddress             `json:"asset"`
	NFTCollection *AccountAddress             `json:"nft_collection"`
}

type ActionDetailsDeleteDns struct {
	Key           *string         `json:"hash"`
	Source        *AccountAddress `json:"source"`
	Asset         *AccountAddress `json:"asset"`
	NFTCollection *AccountAddress `json:"nft_collection"`
}

type ActionDetailsRenewDns struct {
	Source        *AccountAddress `json:"source"`
	Asset         *AccountAddress `json:"asset"`
	NFTCollection *AccountAddress `json:"nft_collection"`
}

type ActionDetailsElectionDeposit struct {
	StakeHolder *AccountAddress `json:"stake_holder"`
	Amount      *string         `json:"amount,omitempty"`
}

type ActionDetailsElectionRecover struct {
	StakeHolder *AccountAddress `json:"stake_holder"`
	Amount      *string         `json:"amount,omitempty"`
}

type ActionDetailsJettonBurn struct {
	Owner             *AccountAddress `json:"owner"`
	OwnerJettonWallet *AccountAddress `json:"owner_jetton_wallet"`
	Asset             *AccountAddress `json:"asset"`
	Amount            *string         `json:"amount"`
}

type ActionDetailsJettonSwapTransfer struct {
	Asset                   *AccountAddress `json:"asset"`
	Source                  *AccountAddress `json:"source"`
	Destination             *AccountAddress `json:"destination"`
	SourceJettonWallet      *AccountAddress `json:"source_jetton_wallet"`
	DestinationJettonWallet *AccountAddress `json:"destination_jetton_wallet"`
	Amount                  *string         `json:"amount"`
}

type ActionDetailsJettonSwapPeerSwap struct {
	AssetIn   *AccountAddress `json:"asset_in"`
	AmountIn  *string         `json:"amount_in"`
	AssetOut  *AccountAddress `json:"asset_out"`
	AmountOut *string         `json:"amount_out"`
}

type ActionDetailsJettonSwap struct {
	Dex                 *string                           `json:"dex"`
	Sender              *AccountAddress                   `json:"sender"`
	AssetIn             *AccountAddress                   `json:"asset_in"`
	AssetOut            *AccountAddress                   `json:"asset_out"`
	DexIncomingTransfer *ActionDetailsJettonSwapTransfer  `json:"dex_incoming_transfer"`
	DexOutgoingTransfer *ActionDetailsJettonSwapTransfer  `json:"dex_outgoing_transfer"`
	PeerSwaps           []ActionDetailsJettonSwapPeerSwap `json:"peer_swaps"`
}

type ActionDetailsToncoJettonSwap struct {
	Dex                 *string                           `json:"dex"`
	Sender              *AccountAddress                   `json:"sender"`
	AssetIn             *AccountAddress                   `json:"asset_in"`
	AssetOut            *AccountAddress                   `json:"asset_out"`
	DexIncomingTransfer *ActionDetailsJettonSwapTransfer  `json:"dex_incoming_transfer"`
	DexOutgoingTransfer *ActionDetailsJettonSwapTransfer  `json:"dex_outgoing_transfer"`
	PeerSwaps           []ActionDetailsJettonSwapPeerSwap `json:"peer_swaps"`
	MinOutAmount        *string                           `json:"min_out_amount"`
}

type ActionDetailsLayerZeroSendDetails struct {
	SendRequestId *uint64         `json:"send_request_id"`
	MsglibManager *string         `json:"msglib_manager"`
	Msglib        *string         `json:"msglib"`
	Uln           *AccountAddress `json:"uln"`
	NativeFee     *uint64         `json:"native_fee"`
	ZroFee        *uint64         `json:"zro_fee"`
	Endpoint      *AccountAddress `json:"endpoint"`
	Channel       *AccountAddress `json:"channel"`
}

type ActionDetailsLayerZeroPacket struct {
	SrcOapp *string `json:"src_oapp"`
	DstOapp *string `json:"dst_oapp"`
	SrcEid  *int32  `json:"src_eid"`
	DstEid  *int32  `json:"dst_eid"`
	Nonce   *int64  `json:"nonce"`
	Guid    *string `json:"guid"`
	Message *string `json:"message"`
}

type ActionDetailsLayerZeroSend struct {
	Sender          *AccountAddress                   `json:"initiator"`
	LayerzeroSend   ActionDetailsLayerZeroSendDetails `json:"layerzero_send_data"`
	LayerzeroPacket ActionDetailsLayerZeroPacket      `json:"layerzero_packet_data"`
}

type ActionDetailsLayerZeroReceive struct {
	Sender          *AccountAddress              `json:"sender"`
	Oapp            *AccountAddress              `json:"oapp"`
	Channel         *AccountAddress              `json:"channel"`
	LayerzeroPacket ActionDetailsLayerZeroPacket `json:"layerzero_packet_data"`
}

type ActionDetailsLayerZeroCommitPacket struct {
	Sender           *AccountAddress              `json:"sender"`
	Endpoint         *AccountAddress              `json:"endpoint"`
	Uln              *AccountAddress              `json:"uln"`
	UlnConnection    *AccountAddress              `json:"uln_connection"`
	Channel          *AccountAddress              `json:"channel"`
	MsglibConnection *AccountAddress              `json:"msglib_connection"`
	LayerzeroPacket  ActionDetailsLayerZeroPacket `json:"layerzero_packet_data"`
}

type ActionDetailsLayerZeroDvnVerify struct {
	Initiator     *AccountAddress `json:"initiator"`
	Nonce         *int64          `json:"nonce"`
	Status        *string         `json:"status"`
	Dvn           *AccountAddress `json:"dvn"`
	Proxy         *AccountAddress `json:"proxy"`
	Uln           *AccountAddress `json:"uln"`
	UlnConnection *AccountAddress `json:"uln_connection"`
}

type ActionDetailsLayerZeroSendTokens struct {
	Sender          *AccountAddress                   `json:"sender"`
	SenderWallet    *AccountAddress                   `json:"sender_wallet"`
	Oapp            *AccountAddress                   `json:"oapp"`
	OappWallet      *AccountAddress                   `json:"oapp_wallet"`
	Asset           *AccountAddress                   `json:"asset"`
	Amount          *string                           `json:"amount"`
	LayerzeroSend   ActionDetailsLayerZeroSendDetails `json:"layerzero_send_data"`
	LayerzeroPacket ActionDetailsLayerZeroPacket      `json:"layerzero_packet_data"`
}

type ActionDetailsJettonTransfer struct {
	Asset                *AccountAddress `json:"asset"`
	Sender               *AccountAddress `json:"sender"`
	Receiver             *AccountAddress `json:"receiver"`
	SenderJettonWallet   *AccountAddress `json:"sender_jetton_wallet"`
	ReceiverJettonWallet *AccountAddress `json:"receiver_jetton_wallet"`
	Amount               *string         `json:"amount"`
	Comment              *string         `json:"comment"`
	IsEncryptedComment   *bool           `json:"is_encrypted_comment"`
	QueryId              *string         `json:"query_id"`
	ResponseDestination  *AccountAddress `json:"response_destination"`
	CustomPayload        *string         `json:"custom_payload"`
	ForwardPayload       *string         `json:"forward_payload"`
	ForwardAmount        *string         `json:"forward_amount"`
}

type ActionDetailsJettonMint struct {
	Asset                *AccountAddress `json:"asset"`
	Receiver             *AccountAddress `json:"receiver"`
	ReceiverJettonWallet *AccountAddress `json:"receiver_jetton_wallet"`
	Amount               *string         `json:"amount"`
	TonAmount            *string         `json:"ton_amount"`
}

type ActionDetailsNftMint struct {
	Owner         *AccountAddress `json:"owner,omitempty"`
	NftItem       *AccountAddress `json:"nft_item"`
	NftCollection *AccountAddress `json:"nft_collection"`
	NftItemIndex  *string         `json:"nft_item_index"`
}

type ActionDetailsNftTransfer struct {
	NftCollection          *AccountAddress `json:"nft_collection"`
	NftItem                *AccountAddress `json:"nft_item"`
	NftItemIndex           *string         `json:"nft_item_index"`
	OldOwner               *AccountAddress `json:"old_owner,omitempty"`
	NewOwner               *AccountAddress `json:"new_owner"`
	IsPurchase             *bool           `json:"is_purchase"`
	Price                  *string         `json:"price,omitempty"`
	QueryId                *string         `json:"query_id"`
	ResponseDestination    *AccountAddress `json:"response_destination"`
	CustomPayload          *string         `json:"custom_payload"`
	ForwardPayload         *string         `json:"forward_payload"`
	ForwardAmount          *string         `json:"forward_amount"`
	Comment                *string         `json:"comment"`
	IsEncryptedComment     *bool           `json:"is_encrypted_comment"`
	Marketplace            *string         `json:"marketplace"`
	RealOldOwner           *AccountAddress `json:"real_old_owner"`
	MarketplaceAddress     *AccountAddress `json:"marketplace_address"`
	PayoutAmount           *string         `json:"payout_amount"`
	PayoutAddress          *AccountAddress `json:"payout_address"`
	PayoutComment          *string         `json:"payout_comment"`
	PayoutCommentEncrypted *bool           `json:"payout_comment_encrypted"`
	PayoutCommentEncoded   *bool           `json:"payout_comment_encoded"`
	RoyaltyAmount          *string         `json:"royalty_amount"`
	RoyaltyAddress         *AccountAddress `json:"royalty_address"`
}

type ActionDetailsDnsPurchase struct {
	NftCollection *AccountAddress `json:"nft_collection"`
	NftItem       *AccountAddress `json:"nft_item"`
	NftItemIndex  *string         `json:"nft_item_index"`
	NewOwner      *AccountAddress `json:"new_owner"`
	Price         *string         `json:"price"`
	QueryId       *string         `json:"query_id"`
	PayoutAmount  *string         `json:"payout_amount"`
}

type ActionDetailsNftPutOnSale struct {
	NftCollection         *AccountAddress `json:"nft_collection"`
	NftItem               *AccountAddress `json:"nft_item"`
	NftItemIndex          *string         `json:"nft_item_index"`
	Owner                 *AccountAddress `json:"owner"`
	ListingAddress        *AccountAddress `json:"listing_address"`
	SaleAddress           *AccountAddress `json:"sale_address"`
	MarketplaceAddress    *AccountAddress `json:"marketplace_address"`
	FullPrice             *string         `json:"full_price"`
	MarketplaceFee        *string         `json:"marketplace_fee"`
	RoyaltyAmount         *string         `json:"royalty_amount"`
	MarketplaceFeeAddress *AccountAddress `json:"marketplace_fee_address"`
	RoyaltyAddress        *AccountAddress `json:"royalty_address"`
	Marketplace           *string         `json:"marketplace"`
}

type ActionDetailsNftPutOnAuction struct {
	NftCollection         *AccountAddress `json:"nft_collection"`
	NftItem               *AccountAddress `json:"nft_item"`
	NftItemIndex          *string         `json:"nft_item_index"`
	Owner                 *AccountAddress `json:"owner"`
	ListingAddress        *AccountAddress `json:"listing_address"`
	AuctionAddress        *AccountAddress `json:"auction_address"`
	MarketplaceAddress    *AccountAddress `json:"marketplace_address"`
	MarketplaceFeeFactor  *string         `json:"marketplace_fee_factor"`
	MarketplaceFeeBase    *string         `json:"marketplace_fee_base"`
	RoyaltyFeeBase        *string         `json:"royalty_fee_base"`
	MaxBid                *string         `json:"max_bid"`
	MinBid                *string         `json:"min_bid"`
	MarketplaceFeeAddress *AccountAddress `json:"marketplace_fee_address"`
	RoyaltyAddress        *AccountAddress `json:"royalty_address"`
	Marketplace           *string         `json:"marketplace"`
}

type ActionDetailsTickTock struct {
	Account *AccountAddress `json:"account,omitempty"`
}

type ActionDetailsSubscribe struct {
	Subscriber   *AccountAddress `json:"subscriber"`
	Beneficiary  *AccountAddress `json:"beneficiary,omitempty"`
	Subscription *AccountAddress `json:"subscription"`
	Amount       *string         `json:"amount"`
}

type ActionDetailsUnsubscribe struct {
	Subscriber   *AccountAddress `json:"subscriber"`
	Beneficiary  *AccountAddress `json:"beneficiary,omitempty"`
	Subscription *AccountAddress `json:"subscription"`
	Amount       *string         `json:"amount,omitempty"`
}

type ActionDetailsWtonMint struct {
	Amount   *string         `json:"amount"`
	Receiver *AccountAddress `json:"receiver"`
}
type ActionDetailsLiquidityVaultExcess struct {
	Asset  *AccountAddress `json:"asset"`
	Amount *string         `json:"amount"`
}
type ActionDetailsDexDepositLiquidity struct {
	Dex                  *string                             `json:"dex"`
	Amount1              *string                             `json:"amount_1"`
	Amount2              *string                             `json:"amount_2"`
	Asset1               *AccountAddress                     `json:"asset_1"`
	Asset2               *AccountAddress                     `json:"asset_2"`
	UserJettonWallet1    *AccountAddress                     `json:"user_jetton_wallet_1"`
	UserJettonWallet2    *AccountAddress                     `json:"user_jetton_wallet_2"`
	Source               *AccountAddress                     `json:"source"`
	Pool                 *AccountAddress                     `json:"pool"`
	DestinationLiquidity *AccountAddress                     `json:"destination_liquidity"`
	LpTokensMinted       *string                             `json:"lp_tokens_minted"`
	TargetAsset1         *AccountAddress                     `json:"target_asset_1"`
	TargetAsset2         *AccountAddress                     `json:"target_asset_2"`
	TargetAmount1        *string                             `json:"target_amount_1"`
	TargetAmount2        *string                             `json:"target_amount_2"`
	VaultExcesses        []ActionDetailsLiquidityVaultExcess `json:"vault_excesses"`
	TickLower            *string                             `json:"tick_lower"`
	TickUpper            *string                             `json:"tick_upper"`
	NftIndex             *string                             `json:"nft_index"`
	NftAddress           *AccountAddress                     `json:"nft_address"`
}

type ActionDetailsDexWithdrawLiquidity struct {
	Dex                  *string         `json:"dex"`
	Amount1              *string         `json:"amount_1"`
	Amount2              *string         `json:"amount_2"`
	Asset1               *AccountAddress `json:"asset_1"`
	Asset2               *AccountAddress `json:"asset_2"`
	UserJettonWallet1    *AccountAddress `json:"user_jetton_wallet_1"`
	UserJettonWallet2    *AccountAddress `json:"user_jetton_wallet_2"`
	LpTokensBurnt        *string         `json:"lp_tokens_burnt"`
	IsRefund             *bool           `json:"is_refund"`
	Source               *AccountAddress `json:"source"`
	Pool                 *AccountAddress `json:"pool"`
	DestinationLiquidity *AccountAddress `json:"destination_liquidity"`
	BurntNftIndex        *string         `json:"burnt_nft_index"`
	BurntNftAddress      *AccountAddress `json:"burnt_nft_address"`
	TickLower            *string         `json:"tick_lower"`
	TickUpper            *string         `json:"tick_upper"`
}

type ActionDetailsToncoDeployPool struct {
	Source              *AccountAddress `json:"source"`
	Pool                *AccountAddress `json:"pool"`
	Router              *AccountAddress `json:"router"`
	RouterJettonWallet1 *AccountAddress `json:"router_jetton_wallet_1"`
	RouterJettonWallet2 *AccountAddress `json:"router_jetton_wallet_2"`
	JettonMinter1       *AccountAddress `json:"jetton_minter_1"`
	JettonMinter2       *AccountAddress `json:"jetton_minter_2"`
	TickSpacing         *string         `json:"tick_spacing"`
	InitialPriceX96     *string         `json:"initial_price_x96"`
	ProtocolFee         *string         `json:"protocol_fee"`
	LpFeeBase           *string         `json:"lp_fee_base"`
	LpFeeCurrent        *string         `json:"lp_fee_current"`
	PoolActive          *bool           `json:"pool_active"`
}

type ActionDetailsStakeDeposit struct {
	Provider     *string         `json:"provider"`
	StakeHolder  *AccountAddress `json:"stake_holder"`
	Pool         *AccountAddress `json:"pool"`
	Amount       *string         `json:"amount"`
	TokensMinted *string         `json:"tokens_minted"`
	Asset        *AccountAddress `json:"asset"`
	SourceAsset  *AccountAddress `json:"source_asset,omitempty"`
}

type ActionDetailsWithdrawStake struct {
	Provider    *string         `json:"provider"`
	StakeHolder *AccountAddress `json:"stake_holder"`
	Pool        *AccountAddress `json:"pool"`
	Amount      *string         `json:"amount"`
	PayoutNft   *AccountAddress `json:"payout_nft"`
	TokensBurnt *string         `json:"tokens_burnt"`
	Asset       *AccountAddress `json:"asset"`
}

type ActionDetailsWithdrawStakeRequest struct {
	Provider     *string         `json:"provider"`
	StakeHolder  *AccountAddress `json:"stake_holder"`
	Pool         *AccountAddress `json:"pool"`
	PayoutNft    *AccountAddress `json:"payout_nft"`
	Asset        *AccountAddress `json:"asset"`
	TokensBurnt  *string         `json:"tokens_burnt"`
	TokensMinted *string         `json:"tokens_minted,omitempty"`
}

type Action struct {
	TraceId               *HashType              `json:"trace_id"`
	ActionId              HashType               `json:"action_id"`
	StartLt               int64                  `json:"start_lt,string"`
	EndLt                 int64                  `json:"end_lt,string"`
	StartUtime            int64                  `json:"start_utime"`
	EndUtime              int64                  `json:"end_utime"`
	TraceEndLt            int64                  `json:"trace_end_lt,string"`
	TraceEndUtime         int64                  `json:"trace_end_utime"`
	TraceMcSeqnoEnd       int32                  `json:"trace_mc_seqno_end"`
	TxHashes              []HashType             `json:"transactions"`
	Success               *bool                  `json:"success"`
	Type                  string                 `json:"type"`
	Details               interface{}            `json:"details"`
	RawAction             *RawAction             `json:"raw_action,omitempty" swaggerignore:"true"`
	TraceExternalHash     *HashType              `json:"trace_external_hash,omitempty"`
	TraceExternalHashNorm *HashType              `json:"trace_external_hash_norm,omitempty"`
	AncestorType          []string               `json:"-"`
	Accounts              []string               `json:"accounts,omitempty"`
	Transactions          []*Transaction         `json:"transactions_full,omitempty"`
	Finality              emulated.FinalityState `json:"finality"`
} // @name Action

// Multisig action details structs
type ActionDetailsMultisigCreateOrder struct {
	QueryId           *string         `json:"query_id"`
	OrderSeqno        *string         `json:"order_seqno"`
	IsCreatedBySigner *bool           `json:"is_created_by_signer"`
	IsSignedByCreator *bool           `json:"is_signed_by_creator"`
	CreatorIndex      *int64          `json:"creator_index"`
	ExpirationDate    *int64          `json:"expiration_date"`
	OrderBoc          *string         `json:"order_boc"`
	Source            *AccountAddress `json:"source"`
	Destination       *AccountAddress `json:"destination"`
	DestinationOrder  *AccountAddress `json:"destination_order"`
}

type ActionDetailsMultisigApprove struct {
	SignerIndex *int64          `json:"signer_index"`
	ExitCode    *int32          `json:"exit_code"`
	Source      *AccountAddress `json:"source"`
	Destination *AccountAddress `json:"destination"`
}

type ActionDetailsMultisigExecute struct {
	QueryId        *string         `json:"query_id"`
	OrderSeqno     *string         `json:"order_seqno"`
	ExpirationDate *int64          `json:"expiration_date"`
	ApprovalsNum   *int64          `json:"approvals_num"`
	SignersHash    *string         `json:"signers_hash"`
	OrderBoc       *string         `json:"order_boc"`
	Source         *AccountAddress `json:"source"`
	Destination    *AccountAddress `json:"destination"`
}

// Vesting action details structs
type ActionDetailsVestingSendMessage struct {
	QueryId     *string         `json:"query_id"`
	MessageBoc  *string         `json:"message_boc"`
	Source      *AccountAddress `json:"source"`
	Vesting     *AccountAddress `json:"vesting"`
	Destination *AccountAddress `json:"destination"`
	Amount      *string         `json:"amount"`
}

type ActionDetailsVestingAddWhitelist struct {
	QueryId       *string          `json:"query_id"`
	AccountsAdded []AccountAddress `json:"accounts_added"`
	Source        *AccountAddress  `json:"source"`
	Vesting       *AccountAddress  `json:"vesting"`
}

// EVAA action details structs
type ActionDetailsEvaaSupply struct {
	SenderJettonWallet    *AccountAddress `json:"sender_jetton_wallet"`
	RecipientJettonWallet *AccountAddress `json:"recipient_jetton_wallet"`
	MasterJettonWallet    *AccountAddress `json:"master_jetton_wallet"`
	Master                *AccountAddress `json:"master"`
	AssetId               *string         `json:"asset_id"`
	IsTon                 *bool           `json:"is_ton"`
	Source                *AccountAddress `json:"source"`
	SourceWallet          *AccountAddress `json:"source_wallet"`
	Recipient             *AccountAddress `json:"recipient"`
	RecipientContract     *AccountAddress `json:"recipient_contract"`
	Asset                 *AccountAddress `json:"asset"`
	Amount                *string         `json:"amount"`
}

type ActionDetailsEvaaWithdraw struct {
	RecipientJettonWallet *AccountAddress `json:"recipient_jetton_wallet"`
	MasterJettonWallet    *AccountAddress `json:"master_jetton_wallet"`
	Master                *AccountAddress `json:"master"`
	FailReason            *string         `json:"fail_reason"`
	AssetId               *string         `json:"asset_id"`
	Source                *AccountAddress `json:"source"`
	Recipient             *AccountAddress `json:"recipient"`
	OwnerContract         *AccountAddress `json:"owner_contract"`
	Asset                 *AccountAddress `json:"asset"`
	Amount                *string         `json:"amount"`
}

type ActionDetailsEvaaLiquidate struct {
	FailReason       *string         `json:"fail_reason"`
	DebtAmount       *string         `json:"debt_amount"`
	Source           *AccountAddress `json:"source"`
	Borrower         *AccountAddress `json:"borrower"`
	BorrowerContract *AccountAddress `json:"borrower_contract"`
	Collateral       *AccountAddress `json:"collateral"`
	AssetId          *string         `json:"asset_id"`
	Asset            *AccountAddress `json:"asset"`
	IsKnownAsset     bool            `json:"is_known_asset"`
	Amount           *string         `json:"amount"`
}

// JVault action details structs
type JettonAmountPair struct {
	Jetton *AccountAddress `json:"jetton"`
	Amount *string         `json:"amount"`
}

type ActionDetailsJvaultClaim struct {
	ClaimedRewards []JettonAmountPair `json:"claimed_rewards"`
	Source         *AccountAddress    `json:"source"`
	StakeWallet    *AccountAddress    `json:"stake_wallet"`
	Pool           *AccountAddress    `json:"pool"`
}

type ActionDetailsJvaultStake struct {
	Period             *int64          `json:"period"`
	MintedStakeJettons *string         `json:"minted_stake_jettons"`
	StakeWallet        *AccountAddress `json:"stake_wallet"`
	Source             *AccountAddress `json:"source"`
	SourceJettonWallet *AccountAddress `json:"source_jetton_wallet"`
	Asset              *AccountAddress `json:"asset"`
	Pool               *AccountAddress `json:"pool"`
	Amount             *string         `json:"amount"`
}

type ActionDetailsJvaultUnstake struct {
	Source       *AccountAddress `json:"source"`
	StakeWallet  *AccountAddress `json:"stake_wallet"`
	Pool         *AccountAddress `json:"pool"`
	Amount       *string         `json:"amount"`
	ExitCode     *int64          `json:"exit_code"`
	Asset        *AccountAddress `json:"asset"`
	StakingAsset *AccountAddress `json:"staking_asset"`
}

type ActionDetailsJvaultUnstakeRequest struct {
	Source       *AccountAddress `json:"source"`
	StakeWallet  *AccountAddress `json:"stake_wallet"`
	Pool         *AccountAddress `json:"pool"`
	Amount       *string         `json:"amount"`
	ExitCode     *int64          `json:"exit_code"`
	Asset        *AccountAddress `json:"asset"`
	StakingAsset *AccountAddress `json:"staking_asset"`
}

type ActionDetailsNftDiscovery struct {
	Source        *AccountAddress `json:"source"`
	NftItem       *AccountAddress `json:"nft_item"`
	NftCollection *AccountAddress `json:"nft_collection"`
	NftItemIndex  *string         `json:"nft_item_index"`
}

type ActionDetailsTgbtcMint struct {
	Source            *AccountAddress `json:"source"`
	Destination       *AccountAddress `json:"destination"`
	Amount            *string         `json:"amount"`
	Asset             *AccountAddress `json:"asset"`
	BitcoinTxId       *string         `json:"bitcoin_tx_id"`
	DestinationWallet *AccountAddress `json:"destination_wallet"`
}

type ActionDetailsTgbtcBurn struct {
	Source       *AccountAddress `json:"source"`
	SourceWallet *AccountAddress `json:"source_wallet"`
	Destination  *AccountAddress `json:"destination"`
	Amount       *string         `json:"amount"`
	Asset        *AccountAddress `json:"asset"`
}

type ActionDetailsTgbtcNewKey struct {
	Source      *AccountAddress `json:"source"`
	Pubkey      *string         `json:"pubkey"`
	Coordinator *AccountAddress `json:"coordinator"`
	Pegout      *AccountAddress `json:"pegout"`
	Amount      *string         `json:"amount"`
	Asset       *AccountAddress `json:"asset"`
}

type ActionDetailsDkgLogFallback struct {
	Coordinator *AccountAddress `json:"coordinator"`
	Pubkey      *string         `json:"pubkey"`
	Timestamp   *string         `json:"timestamp"`
}

type ActionDetailsCoffeeCreatePool struct {
	Source              *AccountAddress `json:"source"`
	SourceJettonWallet  *AccountAddress `json:"source_jetton_wallet"`
	Initiator1          *AccountAddress `json:"initiator_1"`
	Initiator2          *AccountAddress `json:"initiator_2"`
	Pool                *AccountAddress `json:"pool"`
	PoolCreatorContract *AccountAddress `json:"pool_creator_contract"`
	ProvidedAsset       *AccountAddress `json:"provided_asset"`
	Amount              *string         `json:"amount"`
	Asset1              *AccountAddress `json:"asset_1"`
	Asset2              *AccountAddress `json:"asset_2"`
	Amount1             *string         `json:"amount_1"`
	Amount2             *string         `json:"amount_2"`
	LpTokensMinted      *string         `json:"lp_tokens_minted"`
}

type ActionDetailsCoffeeCreatePoolCreator struct {
	Source              *AccountAddress `json:"source"`
	SourceJettonWallet  *AccountAddress `json:"source_jetton_wallet"`
	DepositRecipient    *AccountAddress `json:"deposit_recipient"`
	PoolCreatorContract *AccountAddress `json:"pool_creator_contract"`
	ProvidedAsset       *AccountAddress `json:"provided_asset"`
	Asset1              *AccountAddress `json:"asset_1"`
	Asset2              *AccountAddress `json:"asset_2"`
	Amount              *string         `json:"amount"`
}

type ActionDetailsCoffeeStakingDeposit struct {
	Source             *AccountAddress `json:"source"`
	SourceJettonWallet *AccountAddress `json:"source_jetton_wallet"`
	Pool               *AccountAddress `json:"pool"`
	PoolJettonWallet   *AccountAddress `json:"pool_jetton_wallet"`
	Asset              *AccountAddress `json:"asset"`
	Amount             *string         `json:"amount"`
	MintedItemAddress  *AccountAddress `json:"minted_item_address"`
	MintedItemIndex    *string         `json:"minted_item_index"`
}

type ActionDetailsCoffeeStakingWithdraw struct {
	Source             *AccountAddress `json:"source"`
	SourceJettonWallet *AccountAddress `json:"source_jetton_wallet"`
	Pool               *AccountAddress `json:"pool"`
	PoolJettonWallet   *AccountAddress `json:"pool_jetton_wallet"`
	Asset              *AccountAddress `json:"asset"`
	Amount             *string         `json:"amount"`
	NftAddress         *AccountAddress `json:"nft_address"`
	NftIndex           *string         `json:"nft_index"`
	Points             *string         `json:"points"`
}

type ActionDetailsCoffeeStakingClaimRewards struct {
	Pool                  *AccountAddress `json:"pool"`
	PoolJettonWallet      *AccountAddress `json:"pool_jetton_wallet"`
	Recipient             *AccountAddress `json:"recipient"`
	RecipientJettonWallet *AccountAddress `json:"recipient_jetton_wallet"`
	Asset                 *AccountAddress `json:"asset"`
	Amount                *string         `json:"amount"`
}

type ActionDetailsCoffeeMevProtectHoldFunds struct {
	Source                  *AccountAddress `json:"source"`
	SourceJettonWallet      *AccountAddress `json:"source_jetton_wallet"`
	MevContract             *AccountAddress `json:"mev_contract"`
	MevContractJettonWallet *AccountAddress `json:"mev_contract_jetton_wallet"`
	Asset                   *AccountAddress `json:"asset"`
	Amount                  *string         `json:"amount"`
}

type ActionDetailsCoffeeCreateVault struct {
	Source *AccountAddress `json:"source"`
	Vault  *AccountAddress `json:"vault"`
	Asset  *AccountAddress `json:"asset"`
	Value  *string         `json:"value"`
}

type ActionDetailsAuctionOutbid struct {
	AuctionAddress *AccountAddress `json:"auction_address"`
	Bidder         *AccountAddress `json:"bidder"`
	NewBidder      *AccountAddress `json:"new_bidder"`
	NftItem        *AccountAddress `json:"nft_item"`
	NftCollection  *AccountAddress `json:"nft_collection"`
	Amount         *string         `json:"amount"`
	Comment        *string         `json:"comment,omitempty"`
	Marketplace    *string         `json:"marketplace,omitempty"`
}

type ActionDetailsNftCancelSale struct {
	Owner              *AccountAddress `json:"owner"`
	NftItem            *AccountAddress `json:"nft_item"`
	NftCollection      *AccountAddress `json:"nft_collection"`
	SaleAddress        *AccountAddress `json:"sale_address"`
	MarketplaceAddress *AccountAddress `json:"marketplace_address"`
	Marketplace        *string         `json:"marketplace"`
}

type ActionDetailsNftCancelAuction struct {
	Owner              *AccountAddress `json:"owner"`
	NftItem            *AccountAddress `json:"nft_item"`
	NftCollection      *AccountAddress `json:"nft_collection"`
	AuctionAddress     *AccountAddress `json:"auction_address"`
	MarketplaceAddress *AccountAddress `json:"marketplace_address"`
	Marketplace        *string         `json:"marketplace"`
}

type ActionDetailsNftFinishAuction struct {
	Owner              *AccountAddress `json:"owner"`
	NftItem            *AccountAddress `json:"nft_item"`
	NftCollection      *AccountAddress `json:"nft_collection"`
	AuctionAddress     *AccountAddress `json:"auction_address"`
	MarketplaceAddress *AccountAddress `json:"marketplace_address"`
	Marketplace        *string         `json:"marketplace"`
}

type ActionDetailsDnsRelease struct {
	QueryId       *string         `json:"query_id"`
	Source        *AccountAddress `json:"source"`
	NftItem       *AccountAddress `json:"nft_item"`
	NftCollection *AccountAddress `json:"nft_collection"`
	NftItemIndex  *string         `json:"nft_item_index"`
	Value         *string         `json:"value"`
}

type ActionDetailsNftUpdateSale struct {
	Source             *AccountAddress `json:"source"`
	SaleContract       *AccountAddress `json:"sale_contract"`
	NftAddress         *AccountAddress `json:"nft_address"`
	MarketplaceAddress *AccountAddress `json:"marketplace_address"`
	Marketplace        *string         `json:"marketplace"`
	FullPrice          *string         `json:"full_price"`
	MarketplaceFee     *string         `json:"marketplace_fee"`
	RoyaltyAmount      *string         `json:"royalty_amount"`
}

// COCOON action details structs
type ActionDetailsCocoonWorkerPayout struct {
	PayoutType   *string         `json:"payout_type"`
	QueryId      *string         `json:"query_id"`
	NewTokens    *string         `json:"new_tokens"`
	WorkerState  *int64          `json:"worker_state"`
	WorkerTokens *string         `json:"worker_tokens"`
	Source       *AccountAddress `json:"source"`
	Destination  *AccountAddress `json:"destination"`
	Amount       *string         `json:"amount"`
}

type ActionDetailsCocoonProxyPayout struct {
	QueryId     *string         `json:"query_id"`
	Source      *AccountAddress `json:"source"`
	Destination *AccountAddress `json:"destination"`
}

type ActionDetailsCocoonProxyCharge struct {
	QueryId         *string         `json:"query_id"`
	NewTokensUsed   *string         `json:"new_tokens_used"`
	ExpectedAddress *string         `json:"expected_address"`
	Source          *AccountAddress `json:"source"`
	Destination     *AccountAddress `json:"destination"`
}

type ActionDetailsCocoonClientTopUp struct {
	QueryId     *string         `json:"query_id"`
	Source      *AccountAddress `json:"source"`
	Destination *AccountAddress `json:"destination"`
	Amount      *string         `json:"amount"`
}

type ActionDetailsCocoonRegisterProxy struct {
	QueryId     *string         `json:"query_id"`
	Destination *AccountAddress `json:"destination"`
}

type ActionDetailsCocoonUnregisterProxy struct {
	QueryId     *string         `json:"query_id"`
	Seqno       *int64          `json:"seqno"`
	Destination *AccountAddress `json:"destination"`
}

type ActionDetailsCocoonClientRegister struct {
	QueryId     *string         `json:"query_id"`
	Nonce       *string         `json:"nonce"`
	Source      *AccountAddress `json:"source"`
	Destination *AccountAddress `json:"destination"`
}

type ActionDetailsCocoonClientChangeSecretHash struct {
	QueryId       *string         `json:"query_id"`
	NewSecretHash *string         `json:"new_secret_hash"`
	Source        *AccountAddress `json:"source"`
	Destination   *AccountAddress `json:"destination"`
}

type ActionDetailsCocoonClientRequestRefund struct {
	QueryId     *string         `json:"query_id"`
	ViaWallet   *bool           `json:"via_wallet"`
	Source      *AccountAddress `json:"source"`
	Destination *AccountAddress `json:"destination"`
}

type ActionDetailsCocoonGrantRefund struct {
	QueryId         *string         `json:"query_id"`
	NewTokensUsed   *string         `json:"new_tokens_used"`
	ExpectedAddress *string         `json:"expected_address"`
	Source          *AccountAddress `json:"source"`
	Destination     *AccountAddress `json:"destination"`
	Amount          *string         `json:"amount"`
}

type ActionDetailsCocoonClientIncreaseStake struct {
	QueryId     *string         `json:"query_id"`
	NewStake    *string         `json:"new_stake"`
	Source      *AccountAddress `json:"source"`
	Destination *AccountAddress `json:"destination"`
	Amount      *string         `json:"amount"`
}

type ActionDetailsCocoonClientWithdraw struct {
	QueryId        *string         `json:"query_id"`
	WithdrawAmount *string         `json:"withdraw_amount"`
	Source         *AccountAddress `json:"source"`
	Destination    *AccountAddress `json:"destination"`
	Amount         *string         `json:"amount"`
}
