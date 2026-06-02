#include "postgres.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "common/base64.h"
#include "common/hashfn.h"
#include "fmgr.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "varatt.h"

PG_MODULE_MAGIC;

#define TON_HASH_SIZE 32
#define TON_HASH_HEX_SIZE (TON_HASH_SIZE * 2)
#define TON_ADDR_SIZE 32
#define TON_ADDR_HEX_SIZE (TON_ADDR_SIZE * 2)
#define TON_ADDR_MAX_BITS 511

static int
ton_compare_bytes(const char *a, int a_len, const char *b, int b_len) {
    int prefix_len = Min(a_len, b_len);
    int result = memcmp(a, b, prefix_len);

    if (result != 0) {
        return result;
    }
    if (a_len < b_len) {
        return -1;
    }
    if (a_len > b_len) {
        return 1;
    }
    return 0;
}

// TonHash type. It stores td::Bits256 for the fair 32 bytes.
typedef struct TonHash {
    char data[TON_HASH_SIZE];
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
PG_FUNCTION_INFO_V1(tonhash_hash);
PG_FUNCTION_INFO_V1(tonhash_hash_extended);


Datum tonhash_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    int len = strlen(str);
    TonHash *result;

    if (len == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("empty input is not allowed for type %s", "tonhash")));
    }
    result = (TonHash*) palloc(sizeof(TonHash));

    if (len == TON_HASH_HEX_SIZE) {
        if (hex_decode(str, TON_HASH_HEX_SIZE, result->data) != TON_HASH_SIZE) {
            pfree(result);
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("failed to decode 32-byte hex value for type %s", "tonhash")));
        }
        PG_RETURN_POINTER(result);
    }

    if (len == pg_b64_enc_len(TON_HASH_SIZE) || len == pg_b64_enc_len(TON_HASH_SIZE) - 1) {
        int normalized_len = pg_b64_enc_len(TON_HASH_SIZE);
        char *normalized = (char*) palloc(normalized_len);
        for (int i = 0; i < len; ++i) {
            if (str[i] == '-') {
                normalized[i] = '+';
            } else if (str[i] == '_') {
                normalized[i] = '/';
            } else {
                normalized[i] = str[i];
            }
        }
        if (len < normalized_len) {
            normalized[len] = '=';
        }
        if (pg_b64_decode(normalized, normalized_len, (uint8*) result->data, TON_HASH_SIZE) != TON_HASH_SIZE) {
            pfree(normalized);
            pfree(result);
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("failed to decode 32-byte base64 value for type %s", "tonhash")));
        }
        pfree(normalized);
        PG_RETURN_POINTER(result);
    }

    pfree(result);
    ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
        errmsg("invalid length of input for type %s: \"%d\" is not %d, %d or %d",
               "tonhash", len, pg_b64_enc_len(TON_HASH_SIZE) - 1,
               pg_b64_enc_len(TON_HASH_SIZE), TON_HASH_HEX_SIZE)));
}

Datum tonhash_out(PG_FUNCTION_ARGS) {
    TonHash *hash = (TonHash*) PG_GETARG_POINTER(0);
    int len = pg_b64_enc_len(TON_HASH_SIZE);
    char *result = (char*) palloc(len + 1);
    int written = pg_b64_encode((uint8*) hash->data, TON_HASH_SIZE, result, len);

    if (written < 0) {
        pfree(result);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("failed to base64-encode value of type %s", "tonhash")));
    }
    result[written] = '\0';
    PG_RETURN_CSTRING(result);
}

Datum tonhash_recv(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    TonHash *result = (TonHash*) palloc(sizeof(TonHash));
    
    memcpy(result->data, pq_getmsgbytes(buf, TON_HASH_SIZE), TON_HASH_SIZE);
    PG_RETURN_POINTER(result);
}

Datum tonhash_send(PG_FUNCTION_ARGS) {
    TonHash *result = (TonHash*) PG_GETARG_POINTER(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendbytes(&buf, result->data, TON_HASH_SIZE);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

static int tonhash_cmp_internal(TonHash *a, TonHash *b) {
    return memcmp(a->data, b->data, TON_HASH_SIZE);
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

Datum tonhash_hash(PG_FUNCTION_ARGS) {
    TonHash *hash = (TonHash*) PG_GETARG_POINTER(0);

    return hash_any((const unsigned char*) hash->data, TON_HASH_SIZE);
}

Datum tonhash_hash_extended(PG_FUNCTION_ARGS) {
    TonHash *hash = (TonHash*) PG_GETARG_POINTER(0);
    uint64 seed = (uint64) PG_GETARG_INT64(1);

    return hash_any_extended((const unsigned char*) hash->data, TON_HASH_SIZE, seed);
}

// TonAddr type. It stores TON raw addresses as a compact varlena payload.
typedef struct TonAddr {
    int32 vl_len_;
    char data[FLEXIBLE_ARRAY_MEMBER];
} TonAddr;

#define TONADDR_KIND_NONE 0
#define TONADDR_KIND_EXT 1
#define TONADDR_KIND_STD 2
#define TONADDR_KIND_VAR 3
#define TONADDR_KIND_INVALID -1

#define TONADDR_NONE_TAG ((uint32) 0x80000000U)
#define TONADDR_VAR_TAG_PREFIX 0x8001
#define TONADDR_EXT_TAG_PREFIX 0x8002

#define TONADDR_TAG_SIZE 4
#define TONADDR_TAG_PREFIX_OFFSET 0
#define TONADDR_TAG_BITS_OFFSET 2
#define TONADDR_STD_WORKCHAIN_OFFSET 0
#define TONADDR_STD_ADDR_OFFSET 4
#define TONADDR_EXT_ADDR_OFFSET 4
#define TONADDR_VAR_WORKCHAIN_OFFSET 4
#define TONADDR_VAR_ADDR_OFFSET 8

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
PG_FUNCTION_INFO_V1(tonaddr_hash);
PG_FUNCTION_INFO_V1(tonaddr_hash_extended);


static int
tonaddr_hex_value(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

static int
tonaddr_hex_len_from_bits(int bit_len) {
    return (bit_len + 3) / 4;
}

static int
tonaddr_data_len_from_hex_len(int hex_len) {
    return (hex_len + 1) / 2;
}

static int
tonaddr_data_len_from_bits(int bit_len) {
    return tonaddr_data_len_from_hex_len(tonaddr_hex_len_from_bits(bit_len));
}

static bool
tonaddr_decode_hex_nibbles(const char *src, int hex_len, uint8 *dst) {
    int data_len = tonaddr_data_len_from_hex_len(hex_len);

    memset(dst, 0, data_len);
    for (int i = 0; i < hex_len; ++i) {
        int value = tonaddr_hex_value(src[i]);
        if (value < 0) {
            return false;
        }
        if ((i & 1) == 0) {
            dst[i / 2] = (uint8) (value << 4);
        } else {
            dst[i / 2] |= (uint8) value;
        }
    }
    return true;
}

static void
tonaddr_encode_hex_nibbles(const uint8 *src, int hex_len, char *dst) {
    static const char digits[] = "0123456789ABCDEF";

    for (int i = 0; i < hex_len; ++i) {
        uint8 byte = src[i / 2];
        dst[i] = digits[(i & 1) == 0 ? (byte >> 4) : (byte & 0x0f)];
    }
}

static bool
tonaddr_text_unused_bits_are_zero(const char *hex, int bit_len) {
    int unused_bits = (4 - (bit_len & 3)) & 3;
    int hex_len = tonaddr_hex_len_from_bits(bit_len);
    int last_nibble;

    if (unused_bits == 0 || hex_len == 0) {
        return true;
    }
    last_nibble = tonaddr_hex_value(hex[hex_len - 1]);
    return last_nibble >= 0 && (last_nibble & ((1 << unused_bits) - 1)) == 0;
}

static bool
tonaddr_data_unused_bits_are_zero(const uint8 *data, int bit_len) {
    int hex_len = tonaddr_hex_len_from_bits(bit_len);
    int unused_bits = (4 - (bit_len & 3)) & 3;
    int last_nibble;

    if (hex_len == 0) {
        return true;
    }

    if ((hex_len & 1) != 0 && (data[hex_len / 2] & 0x0f) != 0) {
        return false;
    }

    if (unused_bits == 0) {
        return true;
    }

    if ((hex_len & 1) == 0) {
        last_nibble = data[(hex_len - 1) / 2] & 0x0f;
    } else {
        last_nibble = data[(hex_len - 1) / 2] >> 4;
    }
    return (last_nibble & ((1 << unused_bits) - 1)) == 0;
}

static void
tonaddr_write_i32(uint8 *dst, int32 value) {
    uint32 raw = (uint32) value;

    dst[0] = (uint8) (raw >> 24);
    dst[1] = (uint8) (raw >> 16);
    dst[2] = (uint8) (raw >> 8);
    dst[3] = (uint8) raw;
}

static int32
tonaddr_read_i32(const uint8 *src) {
    uint32 raw = ((uint32) src[0] << 24) |
                 ((uint32) src[1] << 16) |
                 ((uint32) src[2] << 8) |
                 (uint32) src[3];

    return (int32) raw;
}

static void
tonaddr_write_u32(uint8 *dst, uint32 value) {
    dst[0] = (uint8) (value >> 24);
    dst[1] = (uint8) (value >> 16);
    dst[2] = (uint8) (value >> 8);
    dst[3] = (uint8) value;
}

static uint32
tonaddr_read_u32(const uint8 *src) {
    return ((uint32) src[0] << 24) |
           ((uint32) src[1] << 16) |
           ((uint32) src[2] << 8) |
           (uint32) src[3];
}

static void
tonaddr_write_u16(uint8 *dst, int value) {
    dst[0] = (uint8) (value >> 8);
    dst[1] = (uint8) value;
}

static int
tonaddr_read_u16(const uint8 *src) {
    return ((int) src[0] << 8) | (int) src[1];
}

static int
tonaddr_payload_kind(const uint8 *data, int len) {
    uint32 tag;
    uint32 tag_prefix;

    if (len < TONADDR_TAG_SIZE) {
        return TONADDR_KIND_INVALID;
    }
    tag = tonaddr_read_u32(data);
    if (tag == TONADDR_NONE_TAG) {
        return TONADDR_KIND_NONE;
    }

    tag_prefix = tag >> 16;
    if (tag_prefix == TONADDR_EXT_TAG_PREFIX) {
        return TONADDR_KIND_EXT;
    }
    if (tag_prefix == TONADDR_VAR_TAG_PREFIX) {
        return TONADDR_KIND_VAR;
    }
    return TONADDR_KIND_STD;
}

static bool
tonaddr_std_workchain_is_reserved(int32 workchain) {
    uint32 tag = (uint32) workchain;
    uint32 tag_prefix = tag >> 16;

    return tag == TONADDR_NONE_TAG ||
           tag_prefix == TONADDR_EXT_TAG_PREFIX ||
           tag_prefix == TONADDR_VAR_TAG_PREFIX;
}

static void
tonaddr_write_tagged_len(uint8 *dst, int tag_prefix, int bit_len) {
    tonaddr_write_u16(dst + TONADDR_TAG_PREFIX_OFFSET, tag_prefix);
    tonaddr_write_u16(dst + TONADDR_TAG_BITS_OFFSET, bit_len);
}

static int
tonaddr_read_tagged_len(const uint8 *src) {
    return tonaddr_read_u16(src + TONADDR_TAG_BITS_OFFSET);
}

static TonAddr *
tonaddr_alloc(int payload_len) {
    TonAddr *result = (TonAddr*) palloc(VARHDRSZ + payload_len);

    SET_VARSIZE(result, VARHDRSZ + payload_len);
    return result;
}

static TonAddr *
tonaddr_make_none(void) {
    TonAddr *result = tonaddr_alloc(TONADDR_TAG_SIZE);
    uint8 *data = (uint8*) VARDATA(result);

    tonaddr_write_u32(data, TONADDR_NONE_TAG);
    return result;
}

static bool
tonaddr_validate_payload(const uint8 *data, int len) {
    int bit_len;
    int expected_len;

    if (len < TONADDR_TAG_SIZE) {
        return false;
    }
    switch (tonaddr_payload_kind(data, len)) {
        case TONADDR_KIND_NONE:
            return len == TONADDR_TAG_SIZE;
        case TONADDR_KIND_EXT:
            if (len < TONADDR_EXT_ADDR_OFFSET) {
                return false;
            }
            bit_len = tonaddr_read_tagged_len(data);
            if (bit_len < 0 || bit_len > TON_ADDR_MAX_BITS) {
                return false;
            }
            expected_len = TONADDR_EXT_ADDR_OFFSET + tonaddr_data_len_from_bits(bit_len);
            return len == expected_len &&
                   tonaddr_data_unused_bits_are_zero(data + TONADDR_EXT_ADDR_OFFSET, bit_len);
        case TONADDR_KIND_STD:
            if (tonaddr_std_workchain_is_reserved(tonaddr_read_i32(data + TONADDR_STD_WORKCHAIN_OFFSET))) {
                return false;
            }
            return len == TONADDR_STD_ADDR_OFFSET + TON_ADDR_SIZE;
        case TONADDR_KIND_VAR:
            if (len < TONADDR_VAR_ADDR_OFFSET) {
                return false;
            }
            bit_len = tonaddr_read_tagged_len(data);
            if (bit_len < 0 || bit_len > TON_ADDR_MAX_BITS) {
                return false;
            }
            expected_len = TONADDR_VAR_ADDR_OFFSET + tonaddr_data_len_from_bits(bit_len);
            return len == expected_len &&
                   tonaddr_data_unused_bits_are_zero(data + TONADDR_VAR_ADDR_OFFSET, bit_len);
        default:
            return false;
    }
}

static void
tonaddr_validate_payload_or_error(const uint8 *data, int len) {
    if (!tonaddr_validate_payload(data, len)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
            errmsg("invalid binary representation for type %s", "tonaddr")));
    }
}

Datum tonaddr_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    int len = strlen(str);
    char *addr_start = NULL;
    char *end = NULL;
    long workchain;

    if (len == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("empty input is not allowed for type %s", "tonaddr")));
    }
    if (strcmp(str, "addr_none") == 0) {
        PG_RETURN_POINTER(tonaddr_make_none());
    }

    if (strncmp(str, "ext$", 4) == 0) {
        long bit_len;
        int hex_len;
        int payload_len;
        TonAddr *result;
        uint8 *data;

        errno = 0;
        bit_len = strtol(str + 4, &addr_start, 10);
        if (addr_start == str + 4 || *addr_start != ':' || errno == ERANGE ||
            bit_len < 0 || bit_len > TON_ADDR_MAX_BITS) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid external address length for type %s: \"%s\"", "tonaddr", str)));
        }
        addr_start++;
        hex_len = tonaddr_hex_len_from_bits((int) bit_len);
        if (len - (addr_start - str) != hex_len) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("wrong external address hex length for type %s: \"%ld\" != %d",
                       "tonaddr", len - (addr_start - str), hex_len)));
        }
        if (!tonaddr_text_unused_bits_are_zero(addr_start, (int) bit_len)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("unused bits must be zero for type %s", "tonaddr")));
        }

        payload_len = TONADDR_EXT_ADDR_OFFSET + tonaddr_data_len_from_bits((int) bit_len);
        result = tonaddr_alloc(payload_len);
        data = (uint8*) VARDATA(result);
        tonaddr_write_tagged_len(data, TONADDR_EXT_TAG_PREFIX, (int) bit_len);
        if (!tonaddr_decode_hex_nibbles(addr_start, hex_len, data + TONADDR_EXT_ADDR_OFFSET)) {
            pfree(result);
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("failed to decode hex value for type %s", "tonaddr")));
        }
        PG_RETURN_POINTER(result);
    }

    if (strncmp(str, "var$", 4) == 0) {
        int hex_len;
        int payload_len;
        long bit_len;
        char *len_start = NULL;
        TonAddr *result;
        uint8 *data;

        errno = 0;
        workchain = strtol(str + 4, &addr_start, 10);
        if (addr_start == str + 4 || *addr_start != ':' || errno == ERANGE ||
            workchain < PG_INT32_MIN || workchain > PG_INT32_MAX) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid variable address workchain for type %s: \"%s\"", "tonaddr", str)));
        }
        addr_start++;
        errno = 0;
        bit_len = strtol(addr_start, &len_start, 10);
        if (len_start == addr_start || *len_start != ':' || errno == ERANGE ||
            bit_len < 0 || bit_len > TON_ADDR_MAX_BITS) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid variable address length for type %s: \"%s\"", "tonaddr", str)));
        }
        addr_start = len_start + 1;
        hex_len = tonaddr_hex_len_from_bits((int) bit_len);
        if (len - (addr_start - str) != hex_len) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("wrong variable address hex length for type %s: \"%ld\" != %d",
                       "tonaddr", len - (addr_start - str), hex_len)));
        }
        if (!tonaddr_text_unused_bits_are_zero(addr_start, (int) bit_len)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("unused bits must be zero for type %s", "tonaddr")));
        }

        payload_len = TONADDR_VAR_ADDR_OFFSET + tonaddr_data_len_from_bits((int) bit_len);
        result = tonaddr_alloc(payload_len);
        data = (uint8*) VARDATA(result);
        tonaddr_write_tagged_len(data, TONADDR_VAR_TAG_PREFIX, (int) bit_len);
        tonaddr_write_i32(data + TONADDR_VAR_WORKCHAIN_OFFSET, (int32) workchain);
        if (!tonaddr_decode_hex_nibbles(addr_start, hex_len, data + TONADDR_VAR_ADDR_OFFSET)) {
            pfree(result);
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("failed to decode hex value for type %s", "tonaddr")));
        }
        PG_RETURN_POINTER(result);
    }

    errno = 0;
    workchain = strtol(str, &end, 10);
    if (end == str || *end != ':' || errno == ERANGE ||
        workchain < PG_INT32_MIN || workchain > PG_INT32_MAX) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("invalid workchain for type %s: \"%s\"", "tonaddr", str)));
    }
    addr_start = end + 1;

    if (len - (addr_start - str) != TON_ADDR_HEX_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("wrong address length for type %s: \"%ld\" != %d", "tonaddr", len - (addr_start - str), TON_ADDR_HEX_SIZE)));
    }
    if (tonaddr_std_workchain_is_reserved((int32) workchain)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("reserved workchain id for type %s: \"%ld\"", "tonaddr", workchain)));
    }

    TonAddr *result = tonaddr_alloc(TONADDR_STD_ADDR_OFFSET + TON_ADDR_SIZE);
    uint8 *data = (uint8*) VARDATA(result);
    tonaddr_write_i32(data + TONADDR_STD_WORKCHAIN_OFFSET, (int32) workchain);
    if (hex_decode(addr_start, TON_ADDR_HEX_SIZE, (char*) data + TONADDR_STD_ADDR_OFFSET) != TON_ADDR_SIZE) {
        pfree(result);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("failed to decode 32-byte hex value for type %s", "tonaddr")));
    }
    PG_RETURN_POINTER(result);
}

Datum tonaddr_out(PG_FUNCTION_ARGS) {
    TonAddr *addr = (TonAddr*) PG_GETARG_VARLENA_PP(0);
    const uint8 *data = (const uint8*) VARDATA_ANY(addr);
    int payload_len = VARSIZE_ANY_EXHDR(addr);
    char workchain[20];
    int workchain_len;
    int hex_len;
    int full_len;
    char *result;
    int kind;

    tonaddr_validate_payload_or_error(data, payload_len);
    kind = tonaddr_payload_kind(data, payload_len);
    if (kind == TONADDR_KIND_NONE) {
        result = psprintf("addr_none");
        PG_FREE_IF_COPY(addr, 0);
        PG_RETURN_CSTRING(result);
    }

    if (kind == TONADDR_KIND_EXT) {
        int bit_len = tonaddr_read_tagged_len(data);
        char bit_len_text[20];
        int bit_len_text_len = snprintf(bit_len_text, sizeof(bit_len_text), "%d", bit_len);

        hex_len = tonaddr_hex_len_from_bits(bit_len);
        full_len = 4 + bit_len_text_len + 1 + hex_len;
        result = (char*) palloc(full_len + 1);
        memcpy(result, "ext$", 4);
        memcpy(result + 4, bit_len_text, bit_len_text_len);
        result[4 + bit_len_text_len] = ':';
        tonaddr_encode_hex_nibbles(data + TONADDR_EXT_ADDR_OFFSET, hex_len,
                                   result + 4 + bit_len_text_len + 1);
        result[full_len] = '\0';
        PG_FREE_IF_COPY(addr, 0);
        PG_RETURN_CSTRING(result);
    }

    if (kind == TONADDR_KIND_VAR) {
        int32 workchain_id = tonaddr_read_i32(data + TONADDR_VAR_WORKCHAIN_OFFSET);
        int bit_len = tonaddr_read_tagged_len(data);
        char bit_len_text[20];
        int bit_len_text_len = snprintf(bit_len_text, sizeof(bit_len_text), "%d", bit_len);

        workchain_len = snprintf(workchain, sizeof(workchain), "%d", workchain_id);
        hex_len = tonaddr_hex_len_from_bits(bit_len);
        full_len = 4 + workchain_len + 1 + bit_len_text_len + 1 + hex_len;
        result = (char*) palloc(full_len + 1);
        memcpy(result, "var$", 4);
        memcpy(result + 4, workchain, workchain_len);
        result[4 + workchain_len] = ':';
        memcpy(result + 4 + workchain_len + 1, bit_len_text, bit_len_text_len);
        result[4 + workchain_len + 1 + bit_len_text_len] = ':';
        tonaddr_encode_hex_nibbles(data + TONADDR_VAR_ADDR_OFFSET, hex_len,
                                   result + 4 + workchain_len + 1 + bit_len_text_len + 1);
        result[full_len] = '\0';
        PG_FREE_IF_COPY(addr, 0);
        PG_RETURN_CSTRING(result);
    }

    workchain_len = snprintf(workchain, sizeof(workchain), "%d:", tonaddr_read_i32(data + TONADDR_STD_WORKCHAIN_OFFSET));
    full_len = workchain_len + TON_ADDR_HEX_SIZE;
    result = (char*) palloc(full_len + 1);
    result[full_len] = '\0';
    memcpy(result, workchain, workchain_len);
    tonaddr_encode_hex_nibbles(data + TONADDR_STD_ADDR_OFFSET, TON_ADDR_HEX_SIZE, result + workchain_len);
    PG_FREE_IF_COPY(addr, 0);
    PG_RETURN_CSTRING(result);
}

Datum tonaddr_recv(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    int payload_len = buf->len - buf->cursor;
    TonAddr *result = tonaddr_alloc(payload_len);

    memcpy(VARDATA(result), pq_getmsgbytes(buf, payload_len), payload_len);
    tonaddr_validate_payload_or_error((const uint8*) VARDATA(result), payload_len);
    PG_RETURN_POINTER(result);
}

Datum tonaddr_send(PG_FUNCTION_ARGS) {
    TonAddr *result = (TonAddr*) PG_GETARG_VARLENA_PP(0);
    const uint8 *data = (const uint8*) VARDATA_ANY(result);
    int payload_len = VARSIZE_ANY_EXHDR(result);
    StringInfoData buf;

    tonaddr_validate_payload_or_error(data, payload_len);
    pq_begintypsend(&buf);
    pq_sendbytes(&buf, (const char*) data, payload_len);
    PG_FREE_IF_COPY(result, 0);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

static int tonaddr_cmp_internal(TonAddr *a, TonAddr *b) {
    const uint8 *adata = (const uint8*) VARDATA_ANY(a);
    const uint8 *bdata = (const uint8*) VARDATA_ANY(b);
    int alen = VARSIZE_ANY_EXHDR(a);
    int blen = VARSIZE_ANY_EXHDR(b);
    int akind = tonaddr_payload_kind(adata, alen);
    int bkind = tonaddr_payload_kind(bdata, blen);
    int32 aworkchain;
    int32 bworkchain;
    int abit_len;
    int bbit_len;
    int data_len;

    if (akind < bkind) {
        return -1;
    }
    if (akind > bkind) {
        return 1;
    }
    switch (akind) {
        case TONADDR_KIND_NONE:
            return 0;
        case TONADDR_KIND_EXT:
            abit_len = tonaddr_read_tagged_len(adata);
            bbit_len = tonaddr_read_tagged_len(bdata);
            if (abit_len < bbit_len) {
                return -1;
            }
            if (abit_len > bbit_len) {
                return 1;
            }
            data_len = tonaddr_data_len_from_bits(abit_len);
            return memcmp(adata + TONADDR_EXT_ADDR_OFFSET, bdata + TONADDR_EXT_ADDR_OFFSET, data_len);
        case TONADDR_KIND_STD:
            aworkchain = tonaddr_read_i32(adata + TONADDR_STD_WORKCHAIN_OFFSET);
            bworkchain = tonaddr_read_i32(bdata + TONADDR_STD_WORKCHAIN_OFFSET);
            if (aworkchain < bworkchain) {
                return -1;
            }
            if (aworkchain > bworkchain) {
                return 1;
            }
            return memcmp(adata + TONADDR_STD_ADDR_OFFSET, bdata + TONADDR_STD_ADDR_OFFSET, TON_ADDR_SIZE);
        case TONADDR_KIND_VAR:
            aworkchain = tonaddr_read_i32(adata + TONADDR_VAR_WORKCHAIN_OFFSET);
            bworkchain = tonaddr_read_i32(bdata + TONADDR_VAR_WORKCHAIN_OFFSET);
            if (aworkchain < bworkchain) {
                return -1;
            }
            if (aworkchain > bworkchain) {
                return 1;
            }
            abit_len = tonaddr_read_tagged_len(adata);
            bbit_len = tonaddr_read_tagged_len(bdata);
            if (abit_len < bbit_len) {
                return -1;
            }
            if (abit_len > bbit_len) {
                return 1;
            }
            data_len = tonaddr_data_len_from_bits(abit_len);
            return memcmp(adata + TONADDR_VAR_ADDR_OFFSET, bdata + TONADDR_VAR_ADDR_OFFSET, data_len);
        default:
            return ton_compare_bytes((const char*) adata, alen,
                                     (const char*) bdata, blen);
    }
}

Datum tonaddr_lt(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_VARLENA_PP(0);
    TonAddr *b = (TonAddr*) PG_GETARG_VARLENA_PP(1);
    bool result = tonaddr_cmp_internal(a, b) < 0;

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_BOOL(result);
}

Datum tonaddr_le(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_VARLENA_PP(0);
    TonAddr *b = (TonAddr*) PG_GETARG_VARLENA_PP(1);
    bool result = tonaddr_cmp_internal(a, b) <= 0;

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_BOOL(result);
}

Datum tonaddr_eq(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_VARLENA_PP(0);
    TonAddr *b = (TonAddr*) PG_GETARG_VARLENA_PP(1);
    bool result = tonaddr_cmp_internal(a, b) == 0;

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_BOOL(result);
}

Datum tonaddr_gt(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_VARLENA_PP(0);
    TonAddr *b = (TonAddr*) PG_GETARG_VARLENA_PP(1);
    bool result = tonaddr_cmp_internal(a, b) > 0;

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_BOOL(result);
}

Datum tonaddr_ge(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_VARLENA_PP(0);
    TonAddr *b = (TonAddr*) PG_GETARG_VARLENA_PP(1);
    bool result = tonaddr_cmp_internal(a, b) >= 0;

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_BOOL(result);
}

Datum tonaddr_cmp(PG_FUNCTION_ARGS) {
    TonAddr *a = (TonAddr*) PG_GETARG_VARLENA_PP(0);
    TonAddr *b = (TonAddr*) PG_GETARG_VARLENA_PP(1);
    int result = tonaddr_cmp_internal(a, b);

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_INT32(result);
}

Datum tonaddr_hash(PG_FUNCTION_ARGS) {
    TonAddr *addr = (TonAddr*) PG_GETARG_VARLENA_PP(0);
    const uint8 *data = (const uint8*) VARDATA_ANY(addr);
    int payload_len = VARSIZE_ANY_EXHDR(addr);
    Datum result;

    tonaddr_validate_payload_or_error(data, payload_len);
    result = hash_any((const unsigned char*) data, payload_len);
    PG_FREE_IF_COPY(addr, 0);
    return result;
}

Datum tonaddr_hash_extended(PG_FUNCTION_ARGS) {
    TonAddr *addr = (TonAddr*) PG_GETARG_VARLENA_PP(0);
    const uint8 *data = (const uint8*) VARDATA_ANY(addr);
    int payload_len = VARSIZE_ANY_EXHDR(addr);
    uint64 seed = (uint64) PG_GETARG_INT64(1);
    Datum result;

    tonaddr_validate_payload_or_error(data, payload_len);
    result = hash_any_extended((const unsigned char*) data, payload_len, seed);
    PG_FREE_IF_COPY(addr, 0);
    return result;
}

// TonBytes type. It stores arbitrary bytes and exposes them as base64 text.
typedef struct TonBytes {
    int32 vl_len_;
    char data[FLEXIBLE_ARRAY_MEMBER];
} TonBytes;

PG_FUNCTION_INFO_V1(tonbytes_in);
PG_FUNCTION_INFO_V1(tonbytes_out);
PG_FUNCTION_INFO_V1(tonbytes_send);
PG_FUNCTION_INFO_V1(tonbytes_recv);

PG_FUNCTION_INFO_V1(tonbytes_lt);
PG_FUNCTION_INFO_V1(tonbytes_le);
PG_FUNCTION_INFO_V1(tonbytes_eq);
PG_FUNCTION_INFO_V1(tonbytes_gt);
PG_FUNCTION_INFO_V1(tonbytes_ge);
PG_FUNCTION_INFO_V1(tonbytes_cmp);
PG_FUNCTION_INFO_V1(tonbytes_hash);
PG_FUNCTION_INFO_V1(tonbytes_hash_extended);

Datum tonbytes_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    int len = strlen(str);
    int max_decoded_len = pg_b64_dec_len(len);
    TonBytes *result = (TonBytes*) palloc(VARHDRSZ + max_decoded_len);
    int decoded_len = pg_b64_decode(str, len, (uint8*) VARDATA(result), max_decoded_len);

    if (decoded_len < 0) {
        pfree(result);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("failed to decode base64 value for type %s", "tonbytes")));
    }
    SET_VARSIZE(result, VARHDRSZ + decoded_len);
    PG_RETURN_POINTER(result);
}

Datum tonbytes_out(PG_FUNCTION_ARGS) {
    TonBytes *bytes = (TonBytes*) PG_GETARG_VARLENA_PP(0);
    int bytes_len = VARSIZE_ANY_EXHDR(bytes);
    int encoded_len = pg_b64_enc_len(bytes_len);
    char *result = (char*) palloc(encoded_len + 1);
    int written = pg_b64_encode((uint8*) VARDATA_ANY(bytes), bytes_len, result, encoded_len);

    if (written < 0) {
        pfree(result);
        PG_FREE_IF_COPY(bytes, 0);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("failed to base64-encode value of type %s", "tonbytes")));
    }
    result[written] = '\0';
    PG_FREE_IF_COPY(bytes, 0);
    PG_RETURN_CSTRING(result);
}

Datum tonbytes_recv(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    int bytes_len = buf->len - buf->cursor;
    TonBytes *result = (TonBytes*) palloc(VARHDRSZ + bytes_len);

    SET_VARSIZE(result, VARHDRSZ + bytes_len);
    memcpy(VARDATA(result), pq_getmsgbytes(buf, bytes_len), bytes_len);
    PG_RETURN_POINTER(result);
}

Datum tonbytes_send(PG_FUNCTION_ARGS) {
    TonBytes *bytes = (TonBytes*) PG_GETARG_VARLENA_PP(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendbytes(&buf, VARDATA_ANY(bytes), VARSIZE_ANY_EXHDR(bytes));
    PG_FREE_IF_COPY(bytes, 0);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

static int tonbytes_cmp_internal(TonBytes *a, TonBytes *b) {
    return ton_compare_bytes(VARDATA_ANY(a), VARSIZE_ANY_EXHDR(a),
                             VARDATA_ANY(b), VARSIZE_ANY_EXHDR(b));
}

Datum tonbytes_lt(PG_FUNCTION_ARGS) {
    TonBytes *a = (TonBytes*) PG_GETARG_VARLENA_PP(0);
    TonBytes *b = (TonBytes*) PG_GETARG_VARLENA_PP(1);
    bool result = tonbytes_cmp_internal(a, b) < 0;

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_BOOL(result);
}

Datum tonbytes_le(PG_FUNCTION_ARGS) {
    TonBytes *a = (TonBytes*) PG_GETARG_VARLENA_PP(0);
    TonBytes *b = (TonBytes*) PG_GETARG_VARLENA_PP(1);
    bool result = tonbytes_cmp_internal(a, b) <= 0;

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_BOOL(result);
}

Datum tonbytes_eq(PG_FUNCTION_ARGS) {
    TonBytes *a = (TonBytes*) PG_GETARG_VARLENA_PP(0);
    TonBytes *b = (TonBytes*) PG_GETARG_VARLENA_PP(1);
    bool result = tonbytes_cmp_internal(a, b) == 0;

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_BOOL(result);
}

Datum tonbytes_gt(PG_FUNCTION_ARGS) {
    TonBytes *a = (TonBytes*) PG_GETARG_VARLENA_PP(0);
    TonBytes *b = (TonBytes*) PG_GETARG_VARLENA_PP(1);
    bool result = tonbytes_cmp_internal(a, b) > 0;

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_BOOL(result);
}

Datum tonbytes_ge(PG_FUNCTION_ARGS) {
    TonBytes *a = (TonBytes*) PG_GETARG_VARLENA_PP(0);
    TonBytes *b = (TonBytes*) PG_GETARG_VARLENA_PP(1);
    bool result = tonbytes_cmp_internal(a, b) >= 0;

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_BOOL(result);
}

Datum tonbytes_cmp(PG_FUNCTION_ARGS) {
    TonBytes *a = (TonBytes*) PG_GETARG_VARLENA_PP(0);
    TonBytes *b = (TonBytes*) PG_GETARG_VARLENA_PP(1);
    int result = tonbytes_cmp_internal(a, b);

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_INT32(result);
}

Datum tonbytes_hash(PG_FUNCTION_ARGS) {
    TonBytes *bytes = (TonBytes*) PG_GETARG_VARLENA_PP(0);
    Datum result;

    result = hash_any((const unsigned char*) VARDATA_ANY(bytes),
                      VARSIZE_ANY_EXHDR(bytes));
    PG_FREE_IF_COPY(bytes, 0);
    return result;
}

Datum tonbytes_hash_extended(PG_FUNCTION_ARGS) {
    TonBytes *bytes = (TonBytes*) PG_GETARG_VARLENA_PP(0);
    uint64 seed = (uint64) PG_GETARG_INT64(1);
    Datum result;

    result = hash_any_extended((const unsigned char*) VARDATA_ANY(bytes),
                               VARSIZE_ANY_EXHDR(bytes), seed);
    PG_FREE_IF_COPY(bytes, 0);
    return result;
}
