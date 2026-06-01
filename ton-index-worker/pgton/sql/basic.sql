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
    'var$7:abc'::tonaddr AS var_addr,
    'var$7:AC_'::tonaddr AS var_partial_addr,
    'var$-2:123abcdef'::tonaddr AS var_addr_long;

CREATE INDEX test_index_1 ON test(h);
CREATE INDEX test_index_2 ON test(id, h);
CREATE INDEX test_index_3 ON test(h, a);
CREATE INDEX test_index_4 ON test(a, h);
CREATE INDEX test_index_5 ON test(a, id);
CREATE INDEX test_index_6 ON test(b);
CREATE INDEX test_index_7 ON test(b, h);

SELECT * FROM test WHERE h = 'ANT/iLBgHDlgMYBZXVUAABj4////////AAAAAAAAAAA=';
SELECT * FROM test WHERE a = '0:934F64BE8E43994563C6FCAAAA18B772B74E7D314D3D87CAD992F8711D32C635';
SELECT * FROM test WHERE b = 'aGVsbG8=';
SELECT ''::tonbytes IS NULL AS empty_tonbytes_is_null, ''::tonbytes = ''::tonbytes AS empty_tonbytes_eq;

INSERT INTO test(a) VALUES
    ('addr_none'),
    ('ext$12:abc'),
    ('var$-2:123abcdef');
SELECT * FROM test WHERE a = 'var$-2:123ABCDEF';
SELECT * FROM test;
