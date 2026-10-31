#pragma once

namespace staking_history::postgres {
inline constexpr const char* SCHEMA_SQL = R"SQL(
CREATE TABLE IF NOT EXISTS public.elector_stake_states (
    mc_seqno bigint PRIMARY KEY,
    gen_utime bigint NOT NULL,
    complete boolean NOT NULL,
    payload jsonb NOT NULL,
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK ((payload->>'complete')::boolean = complete)
);
CREATE INDEX IF NOT EXISTS elector_stake_states_time_idx ON public.elector_stake_states(gen_utime,mc_seqno);
CREATE TABLE IF NOT EXISTS public.elector_frozen_elections (
    mc_seqno bigint NOT NULL REFERENCES public.elector_stake_states(mc_seqno) ON DELETE CASCADE,
    election_id bigint NOT NULL, vset_hash varchar NOT NULL,
    unfreeze_at bigint NOT NULL, stake_held bigint NOT NULL, total_stake numeric(38,0) NOT NULL,
    PRIMARY KEY(mc_seqno,election_id)
);
CREATE INDEX IF NOT EXISTS elector_frozen_elections_origin_idx
ON public.elector_frozen_elections(election_id,vset_hash,mc_seqno);
CREATE OR REPLACE VIEW public.elector_frozen_positions AS
SELECT s.mc_seqno, s.gen_utime, (e->>'election_id')::bigint AS election_id,
       e->>'vset_hash' AS vset_hash, (e->>'unfreeze_at')::bigint AS unfreeze_at,
       (e->>'stake_held')::bigint AS stake_held,
       p->>'pubkey' AS pubkey, (p->>'holder')::tonaddr AS holder,
       (p->>'weight')::numeric(20,0) AS weight, (p->>'stake')::numeric(38,0) AS stake,
       (p->>'banned')::boolean AS banned
FROM public.elector_stake_states s,
     LATERAL jsonb_array_elements(s.payload->'elections') e,
     LATERAL jsonb_array_elements(e->'positions') p
WHERE s.complete;
CREATE TABLE IF NOT EXISTS public.validator_set_states (
    mc_seqno bigint PRIMARY KEY, gen_utime bigint NOT NULL,
    config_hash varchar NOT NULL, config_boc text NOT NULL
);
CREATE TABLE IF NOT EXISTS public.stake_holder_states (
    account tonaddr NOT NULL, mc_seqno bigint NOT NULL, gen_utime bigint NOT NULL,
    payload jsonb NOT NULL, PRIMARY KEY(account,mc_seqno)
);
)SQL";
}
