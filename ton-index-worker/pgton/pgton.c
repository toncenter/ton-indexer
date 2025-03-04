#include "string.h"
#include "postgres.h"
#include "common/base64.h"
#include "utils/builtins.h"
#include "libpq/pqformat.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

// TonHash type. It stores td::Bits256 for the fair 32 bytes.
typedef struct TonHash {
    char data[32];
} TonHash;

PG_FUNCTION_INFO_V1(tonhash_in);
PG_FUNCTION_INFO_V1(tonhash_out);
PG_FUNCTION_INFO_V1(tonhash_send);
PG_FUNCTION_INFO_V1(tonhash_recv);

PG_FUNCTION_INFO_V1(tonhash_lt);
PG_FUNCTION_INFO_V1(tonhash_le);
PG_FUNCTION_INFO_V1(tonhash_eq);
PG_FUNCTION_INFO_V1(tonhash_gt);
PG_FUNCTION_INFO_V1(tonhash_ge);
PG_FUNCTION_INFO_V1(tonhash_cmp);


Datum tonhash_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    TonHash *result = (TonHash*) palloc(sizeof(TonHash));
    int len = strlen(str);
    if (len == 0) {
        PG_RETURN_NULL();
    }
    if (len != 44) {
        pfree(result);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("invalid length of input for type %s: \"%ld\" != 44", "tonhash", strlen(str))));
    }
    if (pg_b64_decode(str, 44, result->data, 32) < 0) {
        pfree(result);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("failed to decode base64 value for type %s", "tonhash")));
    }
    PG_RETURN_POINTER(result);
}

Datum tonhash_out(PG_FUNCTION_ARGS) {
    TonHash *hash = (TonHash*) PG_GETARG_POINTER(0);
    char *result = (char*) palloc(45);
    result[44] = '\0';
    if (pg_b64_encode(hash->data, 32, result, 44) < 0) {
        pfree(result);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("failed to base64-encode value of type %s", "tonhash")));
    }
    PG_RETURN_CSTRING(result);
}

Datum tonhash_send(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
	TonHash *result = (TonHash*) palloc(sizeof(TonHash));
    
    memcpy(result->data, pq_getmsgbytes(buf, 32), 32);
    PG_RETURN_POINTER(result);
}

Datum tonhash_recv(PG_FUNCTION_ARGS) {
    TonHash *result = (TonHash*) PG_GETARG_POINTER(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendbytes(&buf, result->data, 32);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

static int tonhash_cmp_internal(TonHash *a, TonHash *b) {
    return memcmp(a->data, b->data, 32);
}

Datum tonhash_lt(PG_FUNCTION_ARGS) {
    TonHash *a = (TonHash*) PG_GETARG_POINTER(0);
    TonHash *b = (TonHash*) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(tonhash_cmp_internal(a, b) < 0);
}

Datum tonhash_le(PG_FUNCTION_ARGS) {
    TonHash *a = (TonHash*) PG_GETARG_POINTER(0);
    TonHash *b = (TonHash*) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(tonhash_cmp_internal(a, b) <= 0);
}

Datum tonhash_eq(PG_FUNCTION_ARGS) {
    TonHash *a = (TonHash*) PG_GETARG_POINTER(0);
    TonHash *b = (TonHash*) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(tonhash_cmp_internal(a, b) == 0);
}

Datum tonhash_gt(PG_FUNCTION_ARGS) {
    TonHash *a = (TonHash*) PG_GETARG_POINTER(0);
    TonHash *b = (TonHash*) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(tonhash_cmp_internal(a, b) > 0);
}

Datum tonhash_ge(PG_FUNCTION_ARGS) {
    TonHash *a = (TonHash*) PG_GETARG_POINTER(0);
    TonHash *b = (TonHash*) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(tonhash_cmp_internal(a, b) >= 0);
}

Datum tonhash_cmp(PG_FUNCTION_ARGS) {
    TonHash *a = (TonHash*) PG_GETARG_POINTER(0);
    TonHash *b = (TonHash*) PG_GETARG_POINTER(1);

    PG_RETURN_INT32(tonhash_cmp_internal(a, b));
}

// TonAddr type. It stores TON address in raw format as a struct of 36 bytes.
typedef struct TonAddr {
    int32 workchain;
    char addr[32];
} TonAddr;

PG_FUNCTION_INFO_V1(tonaddr_in);
PG_FUNCTION_INFO_V1(tonaddr_out);
PG_FUNCTION_INFO_V1(tonaddr_send);
PG_FUNCTION_INFO_V1(tonaddr_recv);

PG_FUNCTION_INFO_V1(tonaddr_lt);
PG_FUNCTION_INFO_V1(tonaddr_le);
PG_FUNCTION_INFO_V1(tonaddr_eq);
PG_FUNCTION_INFO_V1(tonaddr_gt);
PG_FUNCTION_INFO_V1(tonaddr_ge);
PG_FUNCTION_INFO_V1(tonaddr_cmp);


Datum tonaddr_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    TonAddr *result = (TonAddr*) palloc(sizeof(TonAddr));

    int pos, len = strlen(str);
    if (len == 0) {
        PG_RETURN_NULL();
    }
    if (strncmp(str, "addr_none", 9) == 0) {
        result->workchain = 123456;
        PG_RETURN_POINTER(result);
    }
    if (strncmp(str, "addr_extern", 11) == 0) {
        result->workchain = 123457;
        PG_RETURN_POINTER(result);
    }
    
    if (sscanf(str, "%d:%n", &result->workchain, &pos) != 1) {
        pfree(result);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("invalid workchain for type %s: \"%s\"", "tonaddr", str)));
    }

    if (len - pos != 64) {
        pfree(result);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("wrong address length for type %s: \"%d\" != 64", "tonaddr", len - pos)));
    }
    if (hex_decode(str + pos, 64, result->addr) < 0) {
        pfree(result);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("failed to decode hex value for type %s", "tonaddr")));
    }
    PG_RETURN_POINTER(result);
}

Datum tonaddr_out(PG_FUNCTION_ARGS) {
    TonAddr *addr = (TonAddr*) PG_GETARG_POINTER(0);

    if (addr->workchain == 123456) {
        char *result = psprintf("addr_none");
        PG_RETURN_CSTRING(result);
    }
    if (addr->workchain == 123457) {
        char *result = psprintf("addr_extern");
        PG_RETURN_CSTRING(result);
    }

    char workchain[20];
    memset(workchain, '\0', 20);
    sprintf(workchain, "%d:", addr->workchain);

    int len = strlen(workchain);
    int full_len = len + 64;

    char *result = (char*) palloc(full_len + 1);
    result[full_len] = '\0';
    memcpy(result, workchain, len);
    if(hex_encode(addr->addr, 32, result + len) < 0) {
        pfree(result);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("failed to hex-encode value of type %s", "tonaddr")));
    }
    for(int i = 0; i < full_len; ++i) {
        if (result[i] >= 'a' && result[i] <= 'z') {
            result[i] += 'A' - 'a';
        }
    }
    PG_RETURN_CSTRING(result);
}

Datum tonaddr_send(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
	TonAddr *result = (TonAddr*) palloc(sizeof(TonAddr));
    
    result->workchain = (int32) pq_getmsgint(buf, 4);
    memcpy(result->addr, pq_getmsgbytes(buf, 32), 32);
    PG_RETURN_POINTER(result);
}

Datum tonaddr_recv(PG_FUNCTION_ARGS) {
    TonAddr *result = (TonAddr*) PG_GETARG_POINTER(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint32(&buf, (uint32) result->workchain);
    pq_sendbytes(&buf, result->addr, 32);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

static int tonaddr_cmp_internal(TonAddr *a, TonAddr *b) {
    if (a->workchain < b->workchain) {
        return -1;
    }
    if (a->workchain > b->workchain) {
        return 1;
    }
    if (a->workchain == 123456 || a->workchain == 123457) {
        return 0;
    }
    return memcmp(a->addr, b->addr, 32);
}

Datum tonaddr_lt(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_POINTER(0);
    TonAddr *b = (TonAddr*) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(tonaddr_cmp_internal(a, b) < 0);
}

Datum tonaddr_le(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_POINTER(0);
    TonAddr *b = (TonAddr*) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(tonaddr_cmp_internal(a, b) <= 0);
}

Datum tonaddr_eq(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_POINTER(0);
    TonAddr *b = (TonAddr*) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(tonaddr_cmp_internal(a, b) == 0);
}

Datum tonaddr_gt(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_POINTER(0);
    TonAddr *b = (TonAddr*) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(tonaddr_cmp_internal(a, b) > 0);
}

Datum tonaddr_ge(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_POINTER(0);
    TonAddr *b = (TonAddr*) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(tonaddr_cmp_internal(a, b) >= 0);
}

Datum tonaddr_cmp(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_POINTER(0);
    TonAddr *b = (TonAddr*) PG_GETARG_POINTER(1);

    PG_RETURN_INT32(tonaddr_cmp_internal(a, b));
}
