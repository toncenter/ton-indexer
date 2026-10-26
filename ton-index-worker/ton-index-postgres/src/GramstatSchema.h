#pragma once

namespace gramstat::postgres {

// Shared by migration 1.3.2 and optional Gramstat startup initialization.
inline constexpr const char* SCHEMA_SQL = R"SQL(
CREATE SCHEMA IF NOT EXISTS gramstat;
CREATE TABLE IF NOT EXISTS gramstat.total_supply (
    mc_seqno bigint PRIMARY KEY,
    gen_utime bigint NOT NULL,
    total numeric(38, 0) NOT NULL,
    accounts numeric(38, 0) NOT NULL,
    dispatch_queue numeric(38, 0) NOT NULL,
    unprocessed_messages numeric(38, 0) NOT NULL,
    unrecovered_validator_fees numeric(38, 0) NOT NULL
);
CREATE INDEX IF NOT EXISTS total_supply_gen_utime_idx
    ON gramstat.total_supply (gen_utime, mc_seqno);
CREATE TABLE IF NOT EXISTS gramstat.account_deltas (
    mc_seqno bigint PRIMARY KEY,
    gen_utime bigint NOT NULL,
    delta_uninit bigint NOT NULL,
    delta_active bigint NOT NULL,
    delta_frozen bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS account_deltas_gen_utime_idx
    ON gramstat.account_deltas (gen_utime, mc_seqno);
CREATE TABLE IF NOT EXISTS gramstat.deployments (
    mc_seqno bigint NOT NULL,
    gen_utime bigint NOT NULL,
    account tonaddr NOT NULL,
    lt bigint NOT NULL,
    tx_hash tonhash NOT NULL,
    init_code_hash tonhash,
    out_msgs_count integer NOT NULL,
    PRIMARY KEY (account, lt, tx_hash)
);
CREATE INDEX IF NOT EXISTS deployments_code_account_lt_idx
    ON gramstat.deployments (init_code_hash, account, lt);
CREATE INDEX IF NOT EXISTS deployments_gen_utime_idx
    ON gramstat.deployments (gen_utime);
CREATE INDEX IF NOT EXISTS deployments_mc_seqno_idx
    ON gramstat.deployments (mc_seqno);
)SQL";

}  // namespace gramstat::postgres
