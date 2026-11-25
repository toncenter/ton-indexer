#pragma once

namespace elector::postgres {
inline constexpr const char* SCHEMA_SQL = R"SQL(
CREATE TABLE IF NOT EXISTS public.validator_round_results (
    election_id bigint PRIMARY KEY,
    vset_hash text NOT NULL,
    total_stake numeric(38,0) NOT NULL CHECK (total_stake >= 0),
    bonuses numeric(38,0) NOT NULL CHECK (bonuses >= 0),
    unfreeze_mc_seqno bigint NOT NULL,
    unfreeze_utime bigint NOT NULL,
    unfreeze_tx_lt bigint NOT NULL,
    unfreeze_tx_hash tonhash NOT NULL
);
)SQL";
}
