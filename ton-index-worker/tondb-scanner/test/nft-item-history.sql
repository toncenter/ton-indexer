-- Shared PG/CH fixture: owner changes, init=false, unknown intervals,
-- an ownerless existing NFT, a quiet day, destruction and redeployment.
INSERT INTO blocks (workchain, seqno, gen_utime) VALUES
    (-1, 1, 1788220800), (-1, 2, 1788264000),
    (-1, 3, 1788307200), (-1, 4, 1788350400),
    (-1, 5, 1788393600), (-1, 6, 1788480000),
    (-1, 7, 1788566400), (-1, 8, 1788652800);
INSERT INTO nft_item_states (account, mc_seqno, gen_utime, last_tx_lt, existing, owner) VALUES
    ('0:A', 6, 1788480000, 60, true, '0:Y'),
    ('0:A', 1, 1788220800, 10, true, '0:X'),
    ('0:B', 2, 1788264000, 20, true, '0:X'),
    ('0:uninitialized', 2, 1788264000, 20, false, NULL),
    ('0:A', 2, 1788264000, 20, true, '0:Y'),
    ('0:A', 3, 1788307200, 30, NULL, NULL),
    ('0:B', 4, 1788350400, 40, true, NULL),
    ('0:B', 6, 1788480000, 60, false, NULL),
    ('0:A', 7, 1788566400, 70, false, NULL),
    ('0:B', 7, 1788566400, 70, true, '0:X'),
    ('0:A', 8, 1788652800, 80, true, '0:Y');
