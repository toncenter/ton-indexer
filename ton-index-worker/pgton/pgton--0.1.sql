CREATE TYPE tonhash;

CREATE OR REPLACE FUNCTION tonhash_in(cstring)
   RETURNS tonhash
   AS 'MODULE_PATHNAME', 'tonhash_in'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION tonhash_out(tonhash)
   RETURNS cstring
   AS 'MODULE_PATHNAME', 'tonhash_out'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tonhash_recv(internal)
   RETURNS tonhash
   AS 'MODULE_PATHNAME', 'tonhash_recv'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tonhash_send(tonhash)
   RETURNS bytea
   AS 'MODULE_PATHNAME', 'tonhash_send'
   LANGUAGE C IMMUTABLE STRICT;


CREATE TYPE tonhash (
   input = tonhash_in,
   output = tonhash_out,
   send = tonhash_send,
   receive = tonhash_recv,
   internallength = 32,
   alignment = int4
);

CREATE FUNCTION tonhash_lt(tonhash, tonhash) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonhash_lt' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonhash_le(tonhash, tonhash) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonhash_le' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonhash_eq(tonhash, tonhash) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonhash_eq' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonhash_gt(tonhash, tonhash) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonhash_gt' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonhash_ge(tonhash, tonhash) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonhash_ge' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonhash_cmp(tonhash, tonhash) RETURNS int4
   AS 'MODULE_PATHNAME', 'tonhash_cmp' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonhash_hash(tonhash) RETURNS int4
   AS 'MODULE_PATHNAME', 'tonhash_hash' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonhash_hash_extended(tonhash, int8) RETURNS int8
   AS 'MODULE_PATHNAME', 'tonhash_hash_extended' LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR < (
   leftarg = tonhash, rightarg = tonhash, procedure = tonhash_lt,
   commutator = > , negator = >= ,
   restrict = scalarltsel, join = scalarltjoinsel
);
CREATE OPERATOR <= (
   leftarg = tonhash, rightarg = tonhash, procedure = tonhash_le,
   commutator = >= , negator = > ,
   restrict = scalarlesel, join = scalarlejoinsel
);
CREATE OPERATOR = (
   leftarg = tonhash, rightarg = tonhash, procedure = tonhash_eq,
   commutator = = ,
   hashes,
   restrict = eqsel, join = eqjoinsel
);
CREATE OPERATOR >= (
   leftarg = tonhash, rightarg = tonhash, procedure = tonhash_ge,
   commutator = <= , negator = < ,
   restrict = scalargesel, join = scalargejoinsel
);
CREATE OPERATOR > (
   leftarg = tonhash, rightarg = tonhash, procedure = tonhash_gt,
   commutator = < , negator = <= ,
   restrict = scalargtsel, join = scalargtjoinsel
);

CREATE OPERATOR CLASS tonhash_ops
    DEFAULT FOR TYPE tonhash USING btree AS
        OPERATOR        1       < ,
        OPERATOR        2       <= ,
        OPERATOR        3       = ,
        OPERATOR        4       >= ,
        OPERATOR        5       > ,
        FUNCTION        1       tonhash_cmp(tonhash, tonhash);

CREATE OPERATOR CLASS tonhash_hash_ops
    DEFAULT FOR TYPE tonhash USING hash AS
        OPERATOR        1       = ,
        FUNCTION        1       tonhash_hash(tonhash),
        FUNCTION        2       tonhash_hash_extended(tonhash, int8);

CREATE FUNCTION tonhash_eq(tonhash, text) RETURNS bool
   AS $$ SELECT $1 = $2::tonhash $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonhash_eq(tonhash, character varying) RETURNS bool
   AS $$ SELECT $1 = $2::tonhash $$
   LANGUAGE SQL IMMUTABLE STRICT;

CREATE FUNCTION tonhash_cmp(tonhash, text) RETURNS int4
   AS $$ SELECT tonhash_cmp($1, $2::tonhash) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonhash_cmp(tonhash, character varying) RETURNS int4
   AS $$ SELECT tonhash_cmp($1, $2::tonhash) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonhash_cmp(text, text) RETURNS int4
   AS $$ SELECT tonhash_cmp($1::tonhash, $2::tonhash) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonhash_cmp(text, character varying) RETURNS int4
   AS $$ SELECT tonhash_cmp($1::tonhash, $2::tonhash) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonhash_cmp(character varying, text) RETURNS int4
   AS $$ SELECT tonhash_cmp($1::tonhash, $2::tonhash) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonhash_cmp(character varying, character varying) RETURNS int4
   AS $$ SELECT tonhash_cmp($1::tonhash, $2::tonhash) $$
   LANGUAGE SQL IMMUTABLE STRICT;

CREATE FUNCTION tonhash_hash(text) RETURNS int4
   AS $$ SELECT tonhash_hash($1::tonhash) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonhash_hash(character varying) RETURNS int4
   AS $$ SELECT tonhash_hash($1::tonhash) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonhash_hash_extended(text, int8) RETURNS int8
   AS $$ SELECT tonhash_hash_extended($1::tonhash, $2) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonhash_hash_extended(character varying, int8) RETURNS int8
   AS $$ SELECT tonhash_hash_extended($1::tonhash, $2) $$
   LANGUAGE SQL IMMUTABLE STRICT;

CREATE OPERATOR = (
   leftarg = tonhash, rightarg = text, procedure = tonhash_eq,
   hashes,
   restrict = eqsel, join = eqjoinsel
);
CREATE OPERATOR = (
   leftarg = tonhash, rightarg = character varying, procedure = tonhash_eq,
   hashes,
   restrict = eqsel, join = eqjoinsel
);

ALTER OPERATOR FAMILY tonhash_ops USING btree ADD
        OPERATOR        3       = (tonhash, text),
        OPERATOR        3       = (tonhash, character varying),
        FUNCTION        1       tonhash_cmp(tonhash, text),
        FUNCTION        1       tonhash_cmp(tonhash, character varying),
        FUNCTION        1       tonhash_cmp(text, text),
        FUNCTION        1       tonhash_cmp(text, character varying),
        FUNCTION        1       tonhash_cmp(character varying, text),
        FUNCTION        1       tonhash_cmp(character varying, character varying);

ALTER OPERATOR FAMILY tonhash_hash_ops USING hash ADD
        OPERATOR        1       = (tonhash, text),
        OPERATOR        1       = (tonhash, character varying),
        FUNCTION        1       tonhash_hash(text),
        FUNCTION        1       tonhash_hash(character varying),
        FUNCTION        2       tonhash_hash_extended(text, int8),
        FUNCTION        2       tonhash_hash_extended(character varying, int8);

-- TonAddr type
CREATE TYPE tonaddr;

CREATE OR REPLACE FUNCTION tonaddr_in(cstring)
   RETURNS tonaddr
   AS 'MODULE_PATHNAME', 'tonaddr_in'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION tonaddr_out(tonaddr)
   RETURNS cstring
   AS 'MODULE_PATHNAME', 'tonaddr_out'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tonaddr_recv(internal)
   RETURNS tonaddr
   AS 'MODULE_PATHNAME', 'tonaddr_recv'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tonaddr_send(tonaddr)
   RETURNS bytea
   AS 'MODULE_PATHNAME', 'tonaddr_send'
   LANGUAGE C IMMUTABLE STRICT;


CREATE TYPE tonaddr (
   input = tonaddr_in,
   output = tonaddr_out,
   send = tonaddr_send,
   receive = tonaddr_recv,
   internallength = variable,
   storage = plain,
   alignment = int4
);

CREATE FUNCTION tonaddr_lt(tonaddr, tonaddr) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonaddr_lt' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_le(tonaddr, tonaddr) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonaddr_le' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_eq(tonaddr, tonaddr) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonaddr_eq' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_gt(tonaddr, tonaddr) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonaddr_gt' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_ge(tonaddr, tonaddr) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonaddr_ge' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_cmp(tonaddr, tonaddr) RETURNS int4
   AS 'MODULE_PATHNAME', 'tonaddr_cmp' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_hash(tonaddr) RETURNS int4
   AS 'MODULE_PATHNAME', 'tonaddr_hash' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_hash_extended(tonaddr, int8) RETURNS int8
   AS 'MODULE_PATHNAME', 'tonaddr_hash_extended' LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR < (
   leftarg = tonaddr, rightarg = tonaddr, procedure = tonaddr_lt,
   commutator = > , negator = >= ,
   restrict = scalarltsel, join = scalarltjoinsel
);
CREATE OPERATOR <= (
   leftarg = tonaddr, rightarg = tonaddr, procedure = tonaddr_le,
   commutator = >= , negator = > ,
   restrict = scalarlesel, join = scalarlejoinsel
);
CREATE OPERATOR = (
   leftarg = tonaddr, rightarg = tonaddr, procedure = tonaddr_eq,
   commutator = = ,
   hashes,
   restrict = eqsel, join = eqjoinsel
);
CREATE OPERATOR >= (
   leftarg = tonaddr, rightarg = tonaddr, procedure = tonaddr_ge,
   commutator = <= , negator = < ,
   restrict = scalargesel, join = scalargejoinsel
);
CREATE OPERATOR > (
   leftarg = tonaddr, rightarg = tonaddr, procedure = tonaddr_gt,
   commutator = < , negator = <= ,
   restrict = scalargtsel, join = scalargtjoinsel
);

CREATE OPERATOR CLASS tonaddr_ops
    DEFAULT FOR TYPE tonaddr USING btree AS
        OPERATOR        1       < ,
        OPERATOR        2       <= ,
        OPERATOR        3       = ,
        OPERATOR        4       >= ,
        OPERATOR        5       > ,
        FUNCTION        1       tonaddr_cmp(tonaddr, tonaddr);

CREATE OPERATOR CLASS tonaddr_hash_ops
    DEFAULT FOR TYPE tonaddr USING hash AS
        OPERATOR        1       = ,
        FUNCTION        1       tonaddr_hash(tonaddr),
        FUNCTION        2       tonaddr_hash_extended(tonaddr, int8);

CREATE FUNCTION tonaddr_eq(tonaddr, text) RETURNS bool
   AS $$ SELECT $1 = $2::tonaddr $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_eq(tonaddr, character varying) RETURNS bool
   AS $$ SELECT $1 = $2::tonaddr $$
   LANGUAGE SQL IMMUTABLE STRICT;

CREATE FUNCTION tonaddr_cmp(tonaddr, text) RETURNS int4
   AS $$ SELECT tonaddr_cmp($1, $2::tonaddr) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_cmp(tonaddr, character varying) RETURNS int4
   AS $$ SELECT tonaddr_cmp($1, $2::tonaddr) $$
   LANGUAGE SQL IMMUTABLE STRICT;

CREATE FUNCTION tonaddr_hash(text) RETURNS int4
   AS $$ SELECT tonaddr_hash($1::tonaddr) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_hash(character varying) RETURNS int4
   AS $$ SELECT tonaddr_hash($1::tonaddr) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_hash_extended(text, int8) RETURNS int8
   AS $$ SELECT tonaddr_hash_extended($1::tonaddr, $2) $$
   LANGUAGE SQL IMMUTABLE STRICT;
CREATE FUNCTION tonaddr_hash_extended(character varying, int8) RETURNS int8
   AS $$ SELECT tonaddr_hash_extended($1::tonaddr, $2) $$
   LANGUAGE SQL IMMUTABLE STRICT;

CREATE OPERATOR = (
   leftarg = tonaddr, rightarg = text, procedure = tonaddr_eq,
   hashes,
   restrict = eqsel, join = eqjoinsel
);
CREATE OPERATOR = (
   leftarg = tonaddr, rightarg = character varying, procedure = tonaddr_eq,
   hashes,
   restrict = eqsel, join = eqjoinsel
);

ALTER OPERATOR FAMILY tonaddr_ops USING btree ADD
        OPERATOR        3       = (tonaddr, text),
        OPERATOR        3       = (tonaddr, character varying),
        FUNCTION        1       tonaddr_cmp(tonaddr, text),
        FUNCTION        1       tonaddr_cmp(tonaddr, character varying);

ALTER OPERATOR FAMILY tonaddr_hash_ops USING hash ADD
        OPERATOR        1       = (tonaddr, text),
        OPERATOR        1       = (tonaddr, character varying),
        FUNCTION        1       tonaddr_hash(text),
        FUNCTION        1       tonaddr_hash(character varying),
        FUNCTION        2       tonaddr_hash_extended(text, int8),
        FUNCTION        2       tonaddr_hash_extended(character varying, int8);

-- TonBytes type
CREATE TYPE tonbytes;

CREATE OR REPLACE FUNCTION tonbytes_in(cstring)
   RETURNS tonbytes
   AS 'MODULE_PATHNAME', 'tonbytes_in'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION tonbytes_out(tonbytes)
   RETURNS cstring
   AS 'MODULE_PATHNAME', 'tonbytes_out'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tonbytes_recv(internal)
   RETURNS tonbytes
   AS 'MODULE_PATHNAME', 'tonbytes_recv'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tonbytes_send(tonbytes)
   RETURNS bytea
   AS 'MODULE_PATHNAME', 'tonbytes_send'
   LANGUAGE C IMMUTABLE STRICT;


CREATE TYPE tonbytes (
   input = tonbytes_in,
   output = tonbytes_out,
   send = tonbytes_send,
   receive = tonbytes_recv,
   internallength = variable,
   storage = extended,
   alignment = int4
);

CREATE FUNCTION tonbytes_lt(tonbytes, tonbytes) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonbytes_lt' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonbytes_le(tonbytes, tonbytes) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonbytes_le' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonbytes_eq(tonbytes, tonbytes) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonbytes_eq' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonbytes_gt(tonbytes, tonbytes) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonbytes_gt' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonbytes_ge(tonbytes, tonbytes) RETURNS bool
   AS 'MODULE_PATHNAME', 'tonbytes_ge' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonbytes_cmp(tonbytes, tonbytes) RETURNS int4
   AS 'MODULE_PATHNAME', 'tonbytes_cmp' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonbytes_hash(tonbytes) RETURNS int4
   AS 'MODULE_PATHNAME', 'tonbytes_hash' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION tonbytes_hash_extended(tonbytes, int8) RETURNS int8
   AS 'MODULE_PATHNAME', 'tonbytes_hash_extended' LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR < (
   leftarg = tonbytes, rightarg = tonbytes, procedure = tonbytes_lt,
   commutator = > , negator = >= ,
   restrict = scalarltsel, join = scalarltjoinsel
);
CREATE OPERATOR <= (
   leftarg = tonbytes, rightarg = tonbytes, procedure = tonbytes_le,
   commutator = >= , negator = > ,
   restrict = scalarlesel, join = scalarlejoinsel
);
CREATE OPERATOR = (
   leftarg = tonbytes, rightarg = tonbytes, procedure = tonbytes_eq,
   commutator = = ,
   hashes,
   restrict = eqsel, join = eqjoinsel
);
CREATE OPERATOR >= (
   leftarg = tonbytes, rightarg = tonbytes, procedure = tonbytes_ge,
   commutator = <= , negator = < ,
   restrict = scalargesel, join = scalargejoinsel
);
CREATE OPERATOR > (
   leftarg = tonbytes, rightarg = tonbytes, procedure = tonbytes_gt,
   commutator = < , negator = <= ,
   restrict = scalargtsel, join = scalargtjoinsel
);

CREATE OPERATOR CLASS tonbytes_ops
    DEFAULT FOR TYPE tonbytes USING btree AS
        OPERATOR        1       < ,
        OPERATOR        2       <= ,
        OPERATOR        3       = ,
        OPERATOR        4       >= ,
        OPERATOR        5       > ,
        FUNCTION        1       tonbytes_cmp(tonbytes, tonbytes);

CREATE OPERATOR CLASS tonbytes_hash_ops
    DEFAULT FOR TYPE tonbytes USING hash AS
        OPERATOR        1       = ,
        FUNCTION        1       tonbytes_hash(tonbytes),
        FUNCTION        2       tonbytes_hash_extended(tonbytes, int8);
