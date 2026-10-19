//! The fields of sf.ton.type.v1.Block the module reads, with their tags in
//! proto/sf/ton/type/v1/type.proto: prost skips the others.

pub mod ton {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Block {
        #[prost(message, optional, tag = "1")]
        pub masterchain_block: Option<BlockData>,
        #[prost(message, repeated, tag = "2")]
        pub shard_blocks: Vec<BlockData>,
        #[prost(message, repeated, tag = "3")]
        pub traces: Vec<Trace>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BlockData {
        #[prost(message, optional, tag = "3")]
        pub info: Option<BlockInfo>,
        #[prost(message, optional, tag = "6")]
        pub extra: Option<BlockExtra>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BlockInfo {
        #[prost(uint32, tag = "11")]
        pub seq_no: u32,
        #[prost(uint32, tag = "14")]
        pub gen_utime: u32,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BlockExtra {
        #[prost(message, repeated, tag = "3")]
        pub account_blocks: Vec<AccountBlock>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct AccountBlock {
        #[prost(message, repeated, tag = "2")]
        pub transactions: Vec<Transaction>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Transaction {
        #[prost(uint32, tag = "7")]
        pub outmsg_cnt: u32,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Trace {
        #[prost(uint32, tag = "10")]
        pub transactions: u32,
    }
}

pub mod smoke {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BlockStats {
        #[prost(uint32, tag = "1")]
        pub seqno: u32,
        #[prost(uint32, tag = "2")]
        pub gen_utime: u32,
        #[prost(uint32, tag = "3")]
        pub shard_blocks: u32,
        #[prost(uint64, tag = "4")]
        pub transactions: u64,
        #[prost(uint64, tag = "5")]
        pub out_messages: u64,
        #[prost(uint32, tag = "6")]
        pub traces: u32,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Totals {
        #[prost(uint32, tag = "1")]
        pub seqno: u32,
        #[prost(int64, tag = "2")]
        pub blocks: i64,
        #[prost(int64, tag = "3")]
        pub transactions: i64,
        #[prost(int64, tag = "4")]
        pub traces: i64,
    }
}
