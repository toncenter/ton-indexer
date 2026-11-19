-- Apply administratively before upgrading the writer/API. Empty maps on legacy
-- rows mean "not prepared"; a certified zero day contains all 18 zero values.
ALTER TABLE gramstat.wallet_days ADD COLUMN IF NOT EXISTS activation_versions Map(String,UInt64) DEFAULT map();
ALTER TABLE gramstat.wallet_activation_stream_state ADD COLUMN IF NOT EXISTS activation_versions Map(String,UInt64) DEFAULT map();
