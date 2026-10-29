#pragma once

namespace jetton_history::postgres {

inline constexpr const char* SCHEMA_SQL = R"SQL(
CREATE TABLE IF NOT EXISTS public.jetton_wallet_states (
    account tonaddr NOT NULL,
    mc_seqno bigint NOT NULL,
    gen_utime bigint NOT NULL,
    last_tx_lt bigint NOT NULL,
    existing boolean,
    balance numeric(78, 0),
    nonzero boolean GENERATED ALWAYS AS (existing AND balance > 0) STORED,
    PRIMARY KEY (account, mc_seqno),
    CHECK ((existing IS TRUE AND balance IS NOT NULL AND balance >= 0)
        OR (existing IS NOT TRUE AND balance IS NULL))
);
)SQL";

}  // namespace jetton_history::postgres

namespace nft_history::postgres {

inline constexpr const char* SCHEMA_SQL = R"SQL(
CREATE TABLE IF NOT EXISTS public.nft_item_states (
    account tonaddr NOT NULL,
    mc_seqno bigint NOT NULL,
    gen_utime bigint NOT NULL,
    last_tx_lt bigint NOT NULL,
    existing boolean,
    owner tonaddr,
    PRIMARY KEY (account, mc_seqno),
    CHECK (existing IS TRUE OR owner IS NULL)
);
CREATE INDEX IF NOT EXISTS nft_item_states_owner_account_idx
ON public.nft_item_states (owner, account)
WHERE existing IS TRUE AND owner IS NOT NULL;
)SQL";

}  // namespace nft_history::postgres
