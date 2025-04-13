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
   internallength = 36,
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
