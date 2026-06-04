\pset format unaligned
\pset fieldsep '|'

CREATE TABLE test(id SERIAL, h TONHASH, a TONADDR, b TONBYTES);
INSERT INTO test(h, a, b) VALUES
    ('ANT/iLBgHDmV1FwjM/EWihj4////////AAAAAAAAAAA=', '0:934F64BE8E43994563C6FCAAAA18B772B74E7D314D3D87CAD992F8711D32C635', 'AQIDBAU='),
    ('ANT/iLBgHDlgMYBZXVUAABj4////////AAAAAAAAAAA=', '-1:79DCEFAE9F68AB8F4D8A2CDABCE377A94365881866540B0FF87860851493D2C4', 'aGVsbG8='),
    ('5+p73p1noxIgPdD2SFf2Jk7heaHA8i1lkB9HP53iMY0=', '-1001:0000000000000000011100000000000000000000000000000000aaaaaaaaaaaa', 'AAEC/w==');

SELECT * FROM test;
SELECT
    'ANT_iLBgHDmV1FwjM_EWihj4________AAAAAAAAAAA='::tonhash AS hash_url_padded,
    'ANT_iLBgHDmV1FwjM_EWihj4________AAAAAAAAAAA'::tonhash AS hash_url_unpadded,
    '00d4ff88b0601c3995d45c2333f1168a18f8ffffffffffff0000000000000000'::tonhash AS hash_hex;
SELECT
    'addr_none'::tonaddr AS none_addr,
    'ext$5:A8'::tonaddr AS ext_addr,
    'ext$12:abc'::tonaddr AS ext_addr_nibble,
    'var$7:12:abc'::tonaddr AS var_addr,
    'var$7:7:AC'::tonaddr AS var_partial_addr,
    'var$-2:36:123abcdef'::tonaddr AS var_addr_long;
SELECT
    encode(tonaddr_send('addr_none'::tonaddr), 'hex') AS none_payload,
    encode(tonaddr_send('ext$12:abc'::tonaddr), 'hex') AS ext_payload,
    encode(tonaddr_send('var$7:7:AC'::tonaddr), 'hex') AS var_payload;
SELECT ''::tonhash;
SELECT ''::tonaddr;

CREATE INDEX test_index_1 ON test(h);
CREATE INDEX test_index_2 ON test(id, h);
CREATE INDEX test_index_3 ON test(h, a);
CREATE INDEX test_index_4 ON test(a, h);
CREATE INDEX test_index_5 ON test(a, id);
CREATE INDEX test_index_6 ON test(b);
CREATE INDEX test_index_7 ON test(b, h);
CREATE INDEX test_hash_index_h ON test USING hash(h);
CREATE INDEX test_hash_index_a ON test USING hash(a);
CREATE INDEX test_hash_index_b ON test USING hash(b);

SELECT * FROM test WHERE h = 'ANT/iLBgHDlgMYBZXVUAABj4////////AAAAAAAAAAA=';
SELECT * FROM test WHERE h = 'ANT/iLBgHDlgMYBZXVUAABj4////////AAAAAAAAAAA='::varchar;
SELECT * FROM test WHERE h = 'ANT/iLBgHDlgMYBZXVUAABj4////////AAAAAAAAAAA='::text;
SELECT * FROM test WHERE h = ANY(ARRAY['ANT/iLBgHDlgMYBZXVUAABj4////////AAAAAAAAAAA=']::varchar[]);
CREATE TEMP TABLE worker_ids(id varchar);
INSERT INTO worker_ids VALUES ('ruslixag-thinkpad_3535591');
PREPARE worker_id_eq AS SELECT id = $1 AS worker_id_matches FROM worker_ids;
EXECUTE worker_id_eq('ruslixag-thinkpad_3535591');
DEALLOCATE worker_id_eq;
DROP TABLE worker_ids;
SELECT * FROM test WHERE a = '0:934F64BE8E43994563C6FCAAAA18B772B74E7D314D3D87CAD992F8711D32C635';
SELECT * FROM test WHERE a = '0:934F64BE8E43994563C6FCAAAA18B772B74E7D314D3D87CAD992F8711D32C635'::varchar;
SELECT * FROM test WHERE a = '0:934F64BE8E43994563C6FCAAAA18B772B74E7D314D3D87CAD992F8711D32C635'::text;
CREATE TEMP TABLE plain_addr_strings(id varchar);
INSERT INTO plain_addr_strings VALUES ('not-an-address');
PREPARE plain_addr_eq AS SELECT id = $1 AS plain_addr_matches FROM plain_addr_strings;
EXECUTE plain_addr_eq('not-an-address');
DEALLOCATE plain_addr_eq;
DROP TABLE plain_addr_strings;
SELECT * FROM test WHERE b = 'aGVsbG8=';
SELECT ''::tonbytes IS NULL AS empty_tonbytes_is_null, ''::tonbytes = ''::tonbytes AS empty_tonbytes_eq;

INSERT INTO test(a) VALUES
    ('addr_none'),
    ('ext$12:abc'),
    ('var$-2:36:123abcdef');
SELECT * FROM test WHERE a = 'var$-2:36:123ABCDEF';
SELECT * FROM test;
