// --actions-msgpack-test: write-back writer self-test.
// Assertions run on the DECODED map, never on the bytes: a Go-decoder
// contract gate. Field TYPES matter; map order does not. A type slip
// silently drops whole traces.
#include "EmuActionSerializeTest.h"

#include "ActionBuild.h"
#include "BlockTree.h"
#include "Classify.h"
#include "SchemaTraceLoader.h"
#include "Value.h"
#include "emu/EmuActionSerialize.h"
#include "emu/EmuTypes.h"  // view_finality

#include "td/utils/base64.h"

#include "common/refint.h"
#include "vm/cells/CellBuilder.h"

#include <msgpack.hpp>

#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mch {

namespace {

int g_fail = 0;

void check(const std::string &name, bool ok) {
  std::printf("%s %s\n", ok ? "PASS" : "FAIL", name.c_str());
  if (!ok) {
    g_fail++;
  }
}

// Decoded-payload inspectors
// Owns the bytes AND the unpack zone; msgpack::object points into both.
class Decoded {
 public:
  explicit Decoded(std::string bytes) : bytes_(std::move(bytes)) {
    try {
      handle_ = msgpack::unpack(bytes_.data(), bytes_.size());
      ok_ = true;
    } catch (const std::exception &) {
      ok_ = false;
    }
  }
  bool ok() const { return ok_; }
  std::size_t size() const { return bytes_.size(); }
  const msgpack::object &root() const { return handle_.get(); }

 private:
  std::string bytes_;
  msgpack::object_handle handle_;
  bool ok_{false};
};

const msgpack::object *at(const msgpack::object &map, const char *key) {
  if (map.type != msgpack::type::MAP) {
    return nullptr;
  }
  const std::size_t n = std::strlen(key);
  for (std::uint32_t i = 0; i < map.via.map.size; i++) {
    const msgpack::object &k = map.via.map.ptr[i].key;
    if (k.type == msgpack::type::STR && k.via.str.size == n &&
        std::memcmp(k.via.str.ptr, key, n) == 0) {
      return &map.via.map.ptr[i].val;
    }
  }
  return nullptr;
}

bool absent(const msgpack::object &map, const char *key) { return at(map, key) == nullptr; }

const msgpack::object *elem(const msgpack::object *arr, std::uint32_t i) {
  if (arr == nullptr || arr->type != msgpack::type::ARRAY || i >= arr->via.array.size) {
    return nullptr;
  }
  return &arr->via.array.ptr[i];
}

bool is_str(const msgpack::object *o, const std::string &want) {
  return o != nullptr && o->type == msgpack::type::STR && o->via.str.size == want.size() &&
         std::memcmp(o->via.str.ptr, want.data(), want.size()) == 0;
}

// The type test that matters against ton-index-go: an INTEGER where the Go
// field is *uint64/*int64/*int32, a STR where it is *string. A str into a
// numeric field, or the reverse, aborts the whole trace decode.
bool is_uint(const msgpack::object *o, std::uint64_t want) {
  return o != nullptr && o->type == msgpack::type::POSITIVE_INTEGER && o->via.u64 == want;
}

// A NEGATIVE msgpack integer (a bounded signed field that went below zero).
bool is_int(const msgpack::object *o, std::int64_t want) {
  if (o == nullptr) {
    return false;
  }
  if (o->type == msgpack::type::NEGATIVE_INTEGER) {
    return o->via.i64 == want;
  }
  return o->type == msgpack::type::POSITIVE_INTEGER &&
         o->via.u64 == static_cast<std::uint64_t>(want);
}

bool is_bool(const msgpack::object *o, bool want) {
  return o != nullptr && o->type == msgpack::type::BOOLEAN && o->via.boolean == want;
}

bool is_nil(const msgpack::object *o) { return o != nullptr && o->type == msgpack::type::NIL; }

bool is_map(const msgpack::object *o, std::uint32_t size) {
  return o != nullptr && o->type == msgpack::type::MAP && o->via.map.size == size;
}

// Fixtures
const std::string kAddrA = "0:" + std::string(64, 'A');
const std::string kAddrB = "0:" + std::string(64, 'B');
const std::string kTraceKey = "6JLNRxi7yQKUYYSGxSt8SVLZKvC3vHMqBLHqOKTqDGA=";

EmuTraceView make_view(std::vector<EmuFinality> finalities) {
  EmuTraceView view;
  view.trace_id = kTraceKey;
  for (EmuFinality f : finalities) {
    EmuTxRef node;
    node.finality = f;
    view.nodes.push_back(std::move(node));
  }
  return view;
}

Action base_action() {
  Action a;
  a.action_id = "aQpDaKBb2mZmBBHkxTZ9xxJgqhZP4mZBTvUAn1KoLpA=";
  a.type = "ton_transfer";
  a.success = true;
  a.start_lt = 58291746000001;
  a.end_lt = 58291746000003;
  a.start_utime = 1753900000;
  a.end_utime = 1753900002;
  a.mc_seqno_end = 47110815;
  a.tx_hashes = {"txA", "txB"};
  a.accounts = {kAddrA, kAddrB};
  return a;
}

// One serialize + decode + "row 0 is a map" step, shared by most cases.
const msgpack::object *first_row(const Decoded &d) {
  const msgpack::object *row = elem(&d.root(), 0);
  return (row != nullptr && row->type == msgpack::type::MAP) ? row : nullptr;
}

// Cases

// "No actions" is the one-byte EMPTY ARRAY, never an empty string:
// EmuActionPayload::actions_blob uses empty to mean "no `actions` field written
// at all", which is a different state for ton-index-go (Classified=false).
void test_empty_actions() {
  Decoded d(serialize_actions({}, make_view({EmuFinality::finalized})));
  check("empty/one_byte", d.size() == 1);
  check("empty/decodes_to_empty_array", d.ok() && d.root().type == msgpack::type::ARRAY &&
                                            d.root().via.array.size == 0);
}

// The always-written key set, bounded integers, and deliberate omissions.
void test_fixed_keys() {
  Action a = base_action();
  Decoded d(serialize_actions({a}, make_view({EmuFinality::finalized})));
  const msgpack::object *row = first_row(d);
  check("fixed/one_row", d.ok() && d.root().via.array.size == 1 && row != nullptr);
  if (row == nullptr) {
    return;
  }
  const msgpack::object &m = *row;
  check("fixed/action_id", is_str(at(m, "action_id"), a.action_id));
  check("fixed/type", is_str(at(m, "type"), "ton_transfer"));
  check("fixed/success_bool", is_bool(at(m, "success"), true));
  // Bounded, so natural msgpack integers.
  check("fixed/start_lt_int", is_uint(at(m, "start_lt"), 58291746000001ULL));
  check("fixed/end_lt_int", is_uint(at(m, "end_lt"), 58291746000003ULL));
  check("fixed/start_utime_int", is_uint(at(m, "start_utime"), 1753900000));
  check("fixed/end_utime_int", is_uint(at(m, "end_utime"), 1753900002));
  check("fixed/finality_int", is_uint(at(m, "finality"), 2));
  // Go only surfaces it when it differs from trace_external_hash (models.go:930).
  check("fixed/trace_external_hash_norm", is_str(at(m, "trace_external_hash_norm"), kTraceKey));

  const msgpack::object *th = at(m, "tx_hashes");
  check("fixed/tx_hashes", th != nullptr && th->type == msgpack::type::ARRAY &&
                               th->via.array.size == 2 && is_str(elem(th, 0), "txA") &&
                               is_str(elem(th, 1), "txB"));
  const msgpack::object *acc = at(m, "accounts");
  check("fixed/accounts", acc != nullptr && acc->type == msgpack::type::ARRAY &&
                              acc->via.array.size == 2 && is_str(elem(acc, 0), kAddrA) &&
                              is_str(elem(acc, 1), kAddrB));

  // Go clobbers these right after decode, or has no field at all.
  check("fixed/omits_trace_id", absent(m, "trace_id"));
  check("fixed/omits_trace_external_hash", absent(m, "trace_external_hash"));
  check("fixed/omits_trace_end_lt", absent(m, "trace_end_lt"));
  check("fixed/omits_trace_end_utime", absent(m, "trace_end_utime"));
  check("fixed/omits_trace_mc_seqno_end", absent(m, "trace_mc_seqno_end"));
  check("fixed/omits_trace_start_lt", absent(m, "trace_start_lt"));
  check("fixed/omits_value_extra_currencies", absent(m, "value_extra_currencies"));
  // Go has no mc_seqno_end field, so the wire payload omits it.
  check("fixed/omits_mc_seqno_end", absent(m, "mc_seqno_end"));
  // An unfilled field is ABSENT, not present-as-nil.
  check("fixed/omits_unfilled_source", absent(m, "source"));
  check("fixed/omits_unfilled_opcode", absent(m, "opcode"));
  check("fixed/omits_unfilled_composite", absent(m, "ton_transfer_data"));
  // Child-recursion asymmetry (Python's, not ours): parent_action_id is ASSIGNED
  // on every row so it is always a key, nil on a top-level one; ancestor_type is
  // assigned only on a child row, so here it is absent.
  const msgpack::object *pa = at(m, "parent_action_id");
  check("fixed/parent_action_id_nil", pa != nullptr && pa->type == msgpack::type::NIL);
  check("fixed/omits_empty_ancestor_type", absent(m, "ancestor_type"));
  check("fixed/exact_key_count", m.via.map.size == 12);
}

// A CHILD row: the two fields serialize_blocks hands down when a non-v1_ops
// block's merged children are expanded into rows of their own.
void test_child_row_fields() {
  Action a = base_action();
  a.parent_action_id = "QIoX0flkEEHBMhk2Z6QlnxrXlWXACZBCee6T8YKYSKE=";
  a.ancestor_type = {"jvault_stake", "teleitem_start_auction"};
  Decoded d(serialize_actions({a}, make_view({EmuFinality::finalized})));
  const msgpack::object *row = first_row(d);
  check("child/decodes", d.ok() && row != nullptr);
  if (row == nullptr) {
    return;
  }
  check("child/parent_action_id", is_str(at(*row, "parent_action_id"), a.parent_action_id));
  const msgpack::object *anc = at(*row, "ancestor_type");
  check("child/ancestor_type", anc != nullptr && anc->type == msgpack::type::ARRAY &&
                                   anc->via.array.size == 2 &&
                                   is_str(elem(anc, 0), "jvault_stake") &&
                                   is_str(elem(anc, 1), "teleitem_start_auction"));
  check("child/exact_key_count", row->via.map.size == 13);
}

// The presence rule for the four "present but means None" Value shapes, plus
// the address / amount rendering the directional fields rely on.
void test_null_and_stringify() {
  Action a = base_action();
  a.type = "jetton_transfer";
  a.source = Value::null();  // av_addr(AccountId(None))
  a.destination = Value::make_account_raw(kAddrB);
  a.asset = Value::null();  // av_addr(TON asset)
  a.asset2 = Value::make_asset_jetton(kAddrA);
  a.amount = Value::make_amount_none();  // Amount(None)
  // A jetton amount well above uint64: `value` is in the decimal-string set
  // precisely because the 256-bit carrier cannot be an int64. Plain base-10,
  // no exponent notation.
  a.value = Value::make_amount(
      td::string_to_int256(std::string("340282366920938463463374607431768211456")));
  a.source_secondary = Value::null();
  a.opcode = Value::make_int64(0x0f8a7ea5);

  Decoded d(serialize_actions({a}, make_view({EmuFinality::confirmed})));
  const msgpack::object *row = first_row(d);
  check("null/decodes", d.ok() && row != nullptr);
  if (row == nullptr) {
    return;
  }
  const msgpack::object &m = *row;
  check("null/addr_none_omitted", absent(m, "source"));
  check("null/null_value_omitted", absent(m, "source_secondary"));
  check("null/ton_asset_omitted", absent(m, "asset"));
  check("null/amount_none_omitted", absent(m, "amount"));
  check("null/account_canonical_str", is_str(at(m, "destination"), kAddrB));
  check("null/jetton_asset_str", is_str(at(m, "asset2"), kAddrA));
  check("null/amount_256bit_dec_str",
        is_str(at(m, "value"), "340282366920938463463374607431768211456"));
  // opcode is bounded (Go *uint32), so it naturalizes to an integer.
  check("null/opcode_int", is_uint(at(m, "opcode"), 0x0f8a7ea5));
  check("null/finality_min_confirmed", is_uint(at(m, "finality"), 1));
}

// Pin the natural wire type of each value domain. A string where the consumer
// expects an integer, or the reverse, aborts the whole trace decode.
void test_natural_types() {
  Action a = base_action();
  a.type = "jetton_transfer";

  // Bounded ints: naturalized, whatever their Go declaration (*int64 for
  // period, *int32 for the eids, *string for tick_lower, decodeFlexString
  // renders an integer for the last of those).
  Value::Fields jv;
  jv.emplace_back("period", Value::make_int64(7776000));
  jv.emplace_back("stake_wallet", Value::make_str(kAddrB));
  a.jvault_stake_data = Value::make_dict(std::move(jv));

  Value::Fields lz;
  lz.emplace_back("src_eid", Value::make_int64(30101));
  lz.emplace_back("dst_eid", Value::make_int64(30343));
  lz.emplace_back("nonce", Value::make_int64(1234567890123LL));
  lz.emplace_back("guid", Value::make_str("0xdeadbeef"));
  a.layerzero_packet_data = Value::make_obj(std::move(lz));

  // Value-domain ints: decimal strings, because they can exceed int64.
  // query_id is a uint64 that routinely sits above 2^63, which is exactly the
  // case a signed naturalization would nil out.
  Value::Fields jt;
  jt.emplace_back("query_id",
                  Value::make_int(td::string_to_int256(std::string("18446744073709551615"))));
  jt.emplace_back("forward_amount", Value::make_int(td::make_refint(1000000)));
  jt.emplace_back("is_encrypted_comment", Value::make_bool(true));
  a.jetton_transfer_data = Value::make_dict(std::move(jt));

  // The composite whose keys are BOTH kinds at once: tick_lower/tick_upper are
  // int24 (naturalized, and negative for tick_lower), nft_index and the amounts
  // are value-domain (decimal strings).
  Value::Fields dd;
  dd.emplace_back("dex", Value::make_str("tonco"));
  dd.emplace_back("tick_lower", Value::make_int64(-887220));
  dd.emplace_back("tick_upper", Value::make_int64(887220));
  dd.emplace_back("nft_index", Value::make_int64(4242));
  dd.emplace_back("amount1", Value::make_int(td::make_refint(500000000)));
  dd.emplace_back("lp_tokens_minted",
                  Value::make_int(td::string_to_int256(
                      std::string("115792089237316195423570985008687907853269984665640564039457"))));
  Value::Fields ve;
  ve.emplace_back("asset", Value::make_str(kAddrA));
  ve.emplace_back("amount", Value::make_int(td::make_refint(7)));
  dd.emplace_back("vault_excesses", Value::make_list({Value::make_dict(std::move(ve))}));
  a.dex_deposit_liquidity_data = Value::make_dict(std::move(dd));

  Decoded d(serialize_actions({a}, make_view({EmuFinality::finalized})));
  const msgpack::object *row = first_row(d);
  check("natural/decodes", d.ok() && row != nullptr);
  if (row == nullptr) {
    return;
  }
  const msgpack::object *jvd = at(*row, "jvault_stake_data");
  check("natural/jvault_period_int", is_uint(at(*jvd, "period"), 7776000));
  check("natural/jvault_stake_wallet_str", is_str(at(*jvd, "stake_wallet"), kAddrB));

  const msgpack::object *lzd = at(*row, "layerzero_packet_data");
  check("natural/lz_src_eid_int", is_uint(at(*lzd, "src_eid"), 30101));
  check("natural/lz_dst_eid_int", is_uint(at(*lzd, "dst_eid"), 30343));
  // `nonce` became a decimal-string key when cocoon landed (cocoon_client_register
  // carries a u64 nonce past int64); LayerZero's bounded one rides along, which
  // its Go *int64 decodes losslessly through decodeFlexInt.
  check("natural/lz_nonce_str", is_str(at(*lzd, "nonce"), "1234567890123"));
  check("natural/lz_guid_str", is_str(at(*lzd, "guid"), "0xdeadbeef"));

  const msgpack::object *jtd = at(*row, "jetton_transfer_data");
  // At/above 2^63: a decimal string is the ONLY encoding that survives.
  check("natural/query_id_uint64_max_str", is_str(at(*jtd, "query_id"), "18446744073709551615"));
  check("natural/forward_amount_str", is_str(at(*jtd, "forward_amount"), "1000000"));
  check("natural/bool_untouched", is_bool(at(*jtd, "is_encrypted_comment"), true));

  const msgpack::object *ddd = at(*row, "dex_deposit_liquidity_data");
  check("natural/tick_lower_negative_int", is_int(at(*ddd, "tick_lower"), -887220));
  check("natural/tick_upper_int", is_uint(at(*ddd, "tick_upper"), 887220));
  check("natural/nft_index_str", is_str(at(*ddd, "nft_index"), "4242"));
  check("natural/amount1_str", is_str(at(*ddd, "amount1"), "500000000"));
  check("natural/lp_tokens_minted_256bit_str",
        is_str(at(*ddd, "lp_tokens_minted"),
               "115792089237316195423570985008687907853269984665640564039457"));
  // Nested one level down, inside a list of maps: the key decision is made per
  // key at EVERY depth, so vault_excesses[].amount is a string and its sibling
  // asset stays the address it was.
  const msgpack::object *ex0 = elem(at(*ddd, "vault_excesses"), 0);
  check("natural/vault_excess_amount_str", ex0 != nullptr && is_str(at(*ex0, "amount"), "7"));
  check("natural/vault_excess_asset_str", ex0 != nullptr && is_str(at(*ex0, "asset"), kAddrA));
}

// The []string-valued fields, whose ELEMENTS bypass ton-index-go's flexible
// decoders entirely (msgpack-go's slice fast path never consults a registered
// element decoder). Nothing numeric may ever be written inside one, and
// claimed_amounts is 256-bit on top of that, so it is doubly required to be a
// string.
void test_string_list_fields() {
  Action a = base_action();
  a.type = "jvault_claim";
  Value::Fields jc;
  jc.emplace_back("claimed_jettons",
                  Value::make_list({Value::make_str(kAddrA), Value::make_str(kAddrB)}));
  jc.emplace_back(
      "claimed_amounts",
      Value::make_list({Value::make_int(td::make_refint(1000)),
                        Value::make_int(td::string_to_int256(
                            std::string("340282366920938463463374607431768211456")))}));
  a.jvault_claim_data = Value::make_dict(std::move(jc));

  Decoded d(serialize_actions({a}, make_view({EmuFinality::finalized})));
  const msgpack::object *row = first_row(d);
  check("strlist/decodes", d.ok() && row != nullptr);
  if (row == nullptr) {
    return;
  }
  const msgpack::object *jcd = at(*row, "jvault_claim_data");
  const msgpack::object *amounts = at(*jcd, "claimed_amounts");
  check("strlist/claimed_amounts_arity",
        amounts != nullptr && amounts->type == msgpack::type::ARRAY &&
            amounts->via.array.size == 2);
  // BOTH elements are strings, the small one too. A []string with one int
  // element fails the whole Unmarshal, so "it fits in an int64" is not a reason
  // to naturalize it here.
  check("strlist/claimed_amounts_small_is_str", is_str(elem(amounts, 0), "1000"));
  check("strlist/claimed_amounts_256bit_is_str",
        is_str(elem(amounts, 1), "340282366920938463463374607431768211456"));
  const msgpack::object *jettons = at(*jcd, "claimed_jettons");
  check("strlist/claimed_jettons_str", is_str(elem(jettons, 0), kAddrA));
  // The two fixed []string fields, from base_action().
  check("strlist/tx_hashes_str", is_str(elem(at(*row, "tx_hashes"), 0), "txA"));
  check("strlist/accounts_str", is_str(elem(at(*row, "accounts"), 0), kAddrA));
}

// A bounded key carrying a value that does not fit a SIGNED int64 packs nil and
// counts, rather than truncating silently. Every field that can hold such a
// value is in the decimal-string set by construction, so this firing means the
// key table is wrong, and an absent field is the only failure mode that cannot
// drop the whole trace on the Go side.
void test_out_of_range_bounded_int() {
  Action a = base_action();
  Value::Fields f;
  f.emplace_back("period", Value::make_int(td::string_to_int256(
                               std::string("9223372036854775808"))));  // 2^63
  a.jvault_stake_data = Value::make_dict(std::move(f));

  ActionSerializeStats st;
  Decoded d(serialize_actions({a}, make_view({EmuFinality::finalized}), &st));
  const msgpack::object *row = first_row(d);
  check("range/decodes", d.ok() && row != nullptr);
  if (row == nullptr) {
    return;
  }
  check("range/out_of_int64_is_nil", is_nil(at(*at(*row, "jvault_stake_data"), "period")));
  check("range/counted_unrenderable", st.unrenderable == 1);
}

// Serializing the same rows twice must produce the same bytes: the payload is
// what a downstream diff compares, so any iteration-order nondeterminism in the
// row construction becomes a wire-visible one here.
void test_determinism() {
  Action a = base_action();
  Value::Fields f;
  f.emplace_back("dex", Value::make_str("dedust"));
  f.emplace_back("amount1", Value::make_int(td::make_refint(12345)));
  f.emplace_back("asset1_out", Value::make_str(kAddrB));
  a.dex_withdraw_liquidity_data = Value::make_dict(std::move(f));
  a.opcode = Value::make_int64(0x7362d09c);

  const EmuTraceView view = make_view({EmuFinality::confirmed, EmuFinality::finalized});
  const std::string first = serialize_actions({a}, view);
  const std::string second = serialize_actions({a}, view);
  check("determinism/byte_identical", !first.empty() && first == second);
}

// G2: cross-language wire fixture
//
// ONE row that exercises every wire-type decision this writer makes, built here
// so the C++ assertions below and the Go round-trip test
// (ton-index-go/index/emulated/actions_wire_test.go) are looking at the SAME
// bytes. `--actions-msgpack-out <path>` regenerates the committed fixture; a
// C++-side mirror of Go's expectations is strictly weaker than running Go's own
// decoder over it, because only the real decoder knows its struct field types.
}  // namespace

Action wire_fixture_action() {
  Action a = base_action();
  a.type = "jetton_transfer";
  // A CHILD row: the serializer's recursion output, so Go's *string /[]string
  // pair is decoded from real bytes rather than assumed from the struct tags.
  a.parent_action_id = "QIoX0flkEEHBMhk2Z6QlnxrXlWXACZBCee6T8YKYSKE=";
  a.ancestor_type = {"jvault_stake"};
  a.source = Value::make_account_raw(kAddrA);
  a.destination = Value::make_account_raw(kAddrB);
  a.asset = Value::make_asset_jetton(kAddrA);
  a.opcode = Value::make_int64(0x0f8a7ea5);  // Go *uint32
  // 256-bit: Go *string, and unrepresentable as an int64 either way.
  a.amount = Value::make_int(
      td::string_to_int256(std::string("340282366920938463463374607431768211456")));

  Value::Fields jt;
  // uint64 above 2^63, the case a signed naturalization would nil out.
  jt.emplace_back("query_id",
                  Value::make_int(td::string_to_int256(std::string("18446744073709551615"))));
  jt.emplace_back("response_destination", Value::make_str(kAddrB));
  jt.emplace_back("forward_amount", Value::make_int(td::make_refint(400000000)));
  jt.emplace_back("custom_payload", Value::null());
  jt.emplace_back("forward_payload", Value::null());
  jt.emplace_back("comment", Value::make_str("hello"));
  jt.emplace_back("is_encrypted_comment", Value::make_bool(false));
  a.jetton_transfer_data = Value::make_dict(std::move(jt));

  // Bounded ints against three different Go declarations: *int32, *int32,
  // *int64.
  Value::Fields lz;
  lz.emplace_back("src_oapp", Value::make_str(kAddrA));
  lz.emplace_back("dst_oapp", Value::make_str(kAddrB));
  lz.emplace_back("src_eid", Value::make_int64(30101));
  lz.emplace_back("dst_eid", Value::make_int64(30343));
  lz.emplace_back("nonce", Value::make_int64(1234567890123LL));
  lz.emplace_back("guid", Value::make_str("0xdeadbeef"));
  lz.emplace_back("message", Value::null());
  a.layerzero_packet_data = Value::make_dict(std::move(lz));

  // Go *uint64 keys fed as DECIMAL STRINGS: send_request_id sits above 2^63
  // (the case a signed naturalization would nil out) and the two fees are
  // uint128-domain. The hex ids beside them are plain *string.
  Value::Fields lzs;
  lzs.emplace_back("send_request_id",
                   Value::make_int(td::string_to_int256(std::string("18446744073709551614"))));
  lzs.emplace_back("msglib_manager", Value::make_str("0x150645746e25be54"));
  lzs.emplace_back("msglib", Value::make_str("0x63bdfbf347f883dd"));
  lzs.emplace_back("uln", Value::make_str(kAddrA));
  lzs.emplace_back("native_fee", Value::make_int(td::make_refint(14835102020LL)));
  lzs.emplace_back("zro_fee", Value::make_int(td::make_refint(0)));
  lzs.emplace_back("endpoint", Value::make_str(kAddrB));
  lzs.emplace_back("channel", Value::null());
  a.layerzero_send_data = Value::make_dict(std::move(lzs));

  // A bounded *int64 nonce beside a mapped status STRING (the parser, not
  // the expression language, produces that string).
  Value::Fields lzv;
  lzv.emplace_back("nonce", Value::make_int64(125));
  lzv.emplace_back("status", Value::make_str("succeeded"));
  lzv.emplace_back("dvn", Value::make_str(kAddrA));
  lzv.emplace_back("proxy", Value::make_str(kAddrB));
  lzv.emplace_back("uln", Value::make_str(kAddrA));
  lzv.emplace_back("uln_connection", Value::null());
  a.layerzero_dvn_verify_data = Value::make_dict(std::move(lzv));

  // The Q64.96 price is a uint160, far past int64, so it must go out as a
  // DECIMAL STRING, while the three u16 fee fields and the int24 tick spacing
  // stay natural integers despite all four being Go *string.
  Value::Fields tdp;
  tdp.emplace_back("jetton0_router_wallet", Value::make_str(kAddrA));
  tdp.emplace_back("jetton1_router_wallet", Value::make_str(kAddrB));
  tdp.emplace_back("jetton0_minter", Value::make_str(kAddrA));
  tdp.emplace_back("jetton1_minter", Value::null());
  tdp.emplace_back("tick_spacing", Value::make_int64(60));
  tdp.emplace_back("initial_price_x96",
                   Value::make_int(td::string_to_int256(
                       std::string("2159938633973433351177391787024"))));
  tdp.emplace_back("protocol_fee", Value::make_int64(33268));
  tdp.emplace_back("lp_fee_base", Value::make_int64(15));
  tdp.emplace_back("lp_fee_current", Value::make_int64(15));
  tdp.emplace_back("pool_active", Value::make_bool(true));
  a.tonco_deploy_pool_data = Value::make_dict(std::move(tdp));

  // A Go []string of ADDRESSES: its elements bypass the flexible decoder (the
  // claimed_amounts rule), so nothing numeric may ever be emitted inside one.
  Value::Fields vaw;
  vaw.emplace_back("query_id", Value::make_int(td::make_refint(0)));
  vaw.emplace_back("accounts_added",
                   Value::make_list(std::vector<Value>{Value::make_str(kAddrA),
                                                       Value::make_str(kAddrB)}));
  a.vesting_add_whitelist_data = Value::make_dict(std::move(vaw));

  a.multisig_create_order_data = Value::make_dict({
      {"query_id", Value::make_int(td::string_to_int256(std::string("18446744073709551615")))},
      {"order_seqno", Value::make_int(td::string_to_int256(std::string(
                            "115792089237316195423570985008687907853269984665640564039457584007913129639935")))},
      {"is_created_by_signer", Value::make_bool(true)},
      {"is_signed_by_creator", Value::make_bool(false)},
      {"creator_index", Value::make_int64(2)},
      {"expiration_date", Value::make_int64(1785595662)},
      {"order_boc", Value::make_str("te6ccgEBAQEA")},
  });
  a.multisig_approve_data = Value::make_dict({
      {"signer_index", Value::make_int64(-1)}, {"exit_code", Value::make_int64(0)}});
  a.multisig_execute_data = Value::make_dict({
      {"query_id", Value::make_int64(93734614000003)},
      {"order_seqno", Value::make_int64(899)},
      {"expiration_date", Value::make_int64(1788095979)},
      {"approvals_num", Value::make_int64(1)},
      {"signers_hash", Value::make_str("C32M4/TYDdcDTvMVw09kw5jnsfQJaZGrLCsRJuShiZQ=")},
      {"order_boc", Value::make_str("te6ccgEBBwEA")},
  });

  // Cocoon: four of the twelve composites, chosen for wire SHAPES rather than
  // coverage, between them they carry every type the family emits.
  //  - worker payout: a *string payout_type, two u64 amounts past int64, and a
  //    u2 `worker_state` that must stay a NATURAL integer beside them;
  //  - client register: the `nonce` that forced `nonce` into
  //    decimal_string_key at all (11924145372215500834 > 2^63, from the
  //    5B6Ex8kv fixture);
  //  - request refund: the family's only BOOL;
  //  - unregister proxy: a u32 `seqno`, natural beside a stringified query_id.
  Value::Fields cwp;
  cwp.emplace_back("payout_type", Value::make_str("last"));
  cwp.emplace_back("query_id",
                   Value::make_int(td::string_to_int256(std::string("10553886210694500674"))));
  cwp.emplace_back("new_tokens",
                   Value::make_int(td::string_to_int256(std::string("9229614747703451079"))));
  cwp.emplace_back("worker_state", Value::make_int64(2));
  cwp.emplace_back("worker_tokens", Value::make_int64(73063940556LL));
  a.cocoon_worker_payout_data = Value::make_dict(std::move(cwp));

  Value::Fields ccr;
  ccr.emplace_back("query_id", Value::make_int64(1821011912409218354LL));
  ccr.emplace_back("nonce",
                   Value::make_int(td::string_to_int256(std::string("11924145372215500834"))));
  a.cocoon_client_register_data = Value::make_dict(std::move(ccr));

  Value::Fields crr;
  crr.emplace_back("query_id", Value::make_int64(0));
  crr.emplace_back("via_wallet", Value::make_bool(true));
  a.cocoon_client_request_refund_data = Value::make_dict(std::move(crr));

  Value::Fields cup;
  cup.emplace_back("query_id", Value::make_int64(0));
  cup.emplace_back("seqno", Value::make_int64(1));
  a.cocoon_unregister_proxy_data = Value::make_dict(std::move(cup));

  // *int64 (flags) beside *string keys in one composite.
  Value::Fields dns;
  dns.emplace_back("value_schema", Value::make_str("DNSAdnlAddress"));
  dns.emplace_back("flags", Value::make_int64(7));
  dns.emplace_back("address", Value::null());
  dns.emplace_back("key", Value::make_str("abababab"));
  dns.emplace_back("value", Value::make_str("cdcdcdcd"));
  a.change_dns_record_data = Value::make_dict(std::move(dns));

  // []string, whose ELEMENTS bypass the flexible decoders.
  Value::Fields jc;
  jc.emplace_back("claimed_jettons",
                  Value::make_list({Value::make_str(kAddrA), Value::make_str(kAddrB)}));
  jc.emplace_back("claimed_amounts",
                  Value::make_list({Value::make_int(td::make_refint(1000)),
                                    Value::make_int(td::string_to_int256(std::string(
                                        "340282366920938463463374607431768211456")))}));
  a.jvault_claim_data = Value::make_dict(std::move(jc));

  // NFT indices use the same decimal-string encoding as other wide integers.
  Value::Fields nt;
  nt.emplace_back("is_purchase", Value::make_bool(true));
  nt.emplace_back("price", Value::make_int(td::make_refint(2500000000LL)));
  nt.emplace_back("nft_item_index", Value::make_int64(1037));
  a.nft_transfer_data = Value::make_dict(std::move(nt));

  // Tick bounds: int24, one of them negative, into Go *string.
  Value::Fields dd;
  dd.emplace_back("dex", Value::make_str("tonco"));
  dd.emplace_back("tick_lower", Value::make_int64(-887220));
  dd.emplace_back("tick_upper", Value::make_int64(887220));
  dd.emplace_back("nft_index", Value::make_int64(4242));
  a.dex_deposit_liquidity_data = Value::make_dict(std::move(dd));

  // dex_wallet_1/dex_wallet_2 are pool-side addresses that fill_dex_withdraw_liquidity
  // and fill_dedust_v2_claim_reward emit. They are real columns of Python's
  // dex_withdraw_liquidity_details composite, but ton-index-go had no struct
  // field for either, so both were silently dropped on decode. Covered here so
  // the Go round-trip fails if that field pair is ever removed again.
  Value::Fields dw;
  dw.emplace_back("dex", Value::make_str("dedust"));
  dw.emplace_back("amount1", Value::make_int(td::make_refint(900000000)));
  dw.emplace_back("dex_jetton_wallet_1", Value::make_str(kAddrA));
  dw.emplace_back("dex_wallet_1", Value::make_str(kAddrA));
  dw.emplace_back("dex_wallet_2", Value::make_str(kAddrB));
  dw.emplace_back("is_refund", Value::make_bool(false));
  dw.emplace_back("lp_tokens_burnt", Value::make_int(td::make_refint(123456789)));
  a.dex_withdraw_liquidity_data = Value::make_dict(std::move(dw));
  return a;
}

std::string wire_fixture_bytes() {
  return serialize_actions({wire_fixture_action()},
                           make_view({EmuFinality::confirmed, EmuFinality::finalized}));
}

namespace {

// The C++ half of G2: the fixture must at least decode and carry the types the
// Go test then checks against real struct fields.
void test_wire_fixture() {
  Decoded d(wire_fixture_bytes());
  const msgpack::object *row = first_row(d);
  check("wire/decodes", d.ok() && row != nullptr);
  if (row == nullptr) {
    return;
  }
  check("wire/opcode_int", is_uint(at(*row, "opcode"), 0x0f8a7ea5));
  check("wire/amount_256bit_str",
        is_str(at(*row, "amount"), "340282366920938463463374607431768211456"));
  check("wire/finality_min", is_uint(at(*row, "finality"), 1));
  check("wire/parent_action_id",
        is_str(at(*row, "parent_action_id"), "QIoX0flkEEHBMhk2Z6QlnxrXlWXACZBCee6T8YKYSKE="));
  check("wire/ancestor_type", is_str(elem(at(*row, "ancestor_type"), 0), "jvault_stake"));
  check("wire/query_id_str",
        is_str(at(*at(*row, "jetton_transfer_data"), "query_id"), "18446744073709551615"));
  check("wire/lz_nonce_str",
        is_str(at(*at(*row, "layerzero_packet_data"), "nonce"), "1234567890123"));
  const msgpack::object *lzs = at(*row, "layerzero_send_data");
  check("wire/lz_send_request_id_str",
        is_str(at(*lzs, "send_request_id"), "18446744073709551614"));
  check("wire/lz_native_fee_str", is_str(at(*lzs, "native_fee"), "14835102020"));
  check("wire/lz_zro_fee_str", is_str(at(*lzs, "zro_fee"), "0"));
  check("wire/lz_msglib_str", is_str(at(*lzs, "msglib"), "0x63bdfbf347f883dd"));
  const msgpack::object *lzv = at(*row, "layerzero_dvn_verify_data");
  check("wire/lz_dvn_nonce_str", is_str(at(*lzv, "nonce"), "125"));
  check("wire/lz_dvn_status_str", is_str(at(*lzv, "status"), "succeeded"));
  const msgpack::object *tdp = at(*row, "tonco_deploy_pool_data");
  check("wire/tonco_initial_price_x96_str",
        is_str(at(*tdp, "initial_price_x96"), "2159938633973433351177391787024"));
  check("wire/tonco_protocol_fee_int", is_uint(at(*tdp, "protocol_fee"), 33268));
  check("wire/tonco_tick_spacing_int", is_uint(at(*tdp, "tick_spacing"), 60));
  const msgpack::object *vaw = at(*row, "vesting_add_whitelist_data");
  check("wire/whitelist_accounts_added_str",
        is_str(elem(at(*vaw, "accounts_added"), 1), kAddrB));
  const msgpack::object *mco = at(*row, "multisig_create_order_data");
  check("wire/multisig_order_seqno_str", is_str(at(*mco, "order_seqno"),
        "115792089237316195423570985008687907853269984665640564039457584007913129639935"));
  check("wire/multisig_signer_index_int",
        is_int(at(*at(*row, "multisig_approve_data"), "signer_index"), -1));
  check("wire/multisig_approvals_num_int",
        is_uint(at(*at(*row, "multisig_execute_data"), "approvals_num"), 1));
  const msgpack::object *cwp = at(*row, "cocoon_worker_payout_data");
  check("wire/cocoon_new_tokens_str", is_str(at(*cwp, "new_tokens"), "9229614747703451079"));
  check("wire/cocoon_worker_tokens_str", is_str(at(*cwp, "worker_tokens"), "73063940556"));
  check("wire/cocoon_worker_state_int", is_uint(at(*cwp, "worker_state"), 2));
  check("wire/cocoon_payout_type_str", is_str(at(*cwp, "payout_type"), "last"));
  // The u64 nonce past int64, a natural pack would have nil'd it out.
  check("wire/cocoon_nonce_str",
        is_str(at(*at(*row, "cocoon_client_register_data"), "nonce"),
               "11924145372215500834"));
  check("wire/cocoon_via_wallet_bool",
        at(*at(*row, "cocoon_client_request_refund_data"), "via_wallet") != nullptr &&
            at(*at(*row, "cocoon_client_request_refund_data"), "via_wallet")->type ==
                msgpack::type::BOOLEAN);
  check("wire/cocoon_seqno_int",
        is_uint(at(*at(*row, "cocoon_unregister_proxy_data"), "seqno"), 1));
  check("wire/dns_flags_int", is_uint(at(*at(*row, "change_dns_record_data"), "flags"), 7));
  check("wire/claimed_amounts_all_str",
        is_str(elem(at(*at(*row, "jvault_claim_data"), "claimed_amounts"), 0), "1000"));
  check("wire/nft_index_str",
        is_str(at(*at(*row, "nft_transfer_data"), "nft_item_index"), "1037"));
  check("wire/tick_lower_negative_int",
        is_int(at(*at(*row, "dex_deposit_liquidity_data"), "tick_lower"), -887220));
  const msgpack::object *dwd = at(*row, "dex_withdraw_liquidity_data");
  check("wire/dex_wallet_1_str", is_str(at(*dwd, "dex_wallet_1"), kAddrA));
  check("wire/dex_wallet_2_str", is_str(at(*dwd, "dex_wallet_2"), kAddrB));
  check("wire/lp_tokens_burnt_str", is_str(at(*dwd, "lp_tokens_burnt"), "123456789"));
}

// The rest of the VType -> msgpack table, inside a composite.
void test_value_table() {
  Action a = base_action();
  Value::Fields inner;
  inner.emplace_back("depth", Value::make_int64(2));
  Value::Fields f;
  f.emplace_back("encrypted", Value::make_bool(false));
  f.emplace_back("payload", Value::make_bytes(std::string("\x01\x02\xff", 3)));
  f.emplace_back("content", Value::null());
  f.emplace_back("owner", Value::make_account_none());
  f.emplace_back("items", Value::make_list({Value::make_int64(7), Value::null(),
                                            Value::make_str("tail")}));
  f.emplace_back("nested", Value::make_dict(std::move(inner)));
  a.ton_transfer_data = Value::make_dict(std::move(f));

  Decoded d(serialize_actions({a}, make_view({EmuFinality::emulated})));
  const msgpack::object *row = first_row(d);
  check("table/decodes", d.ok() && row != nullptr);
  if (row == nullptr) {
    return;
  }
  const msgpack::object *ttd = at(*row, "ton_transfer_data");
  check("table/bool_stays_bool", is_bool(at(*ttd, "encrypted"), false));
  check("table/bytes_base64",
        is_str(at(*ttd, "payload"), td::base64_encode(td::Slice("\x01\x02\xff", 3))));
  // The two-level presence rule: a composite's key set is fixed by the fill
  // and an unfilled field is present-as-NIL. Contrast test_fixed_keys, where
  // an unfilled TOP-LEVEL field is dropped entirely.
  check("table/null_key_is_nil", is_nil(at(*ttd, "content")));
  check("table/addr_none_key_is_nil", is_nil(at(*ttd, "owner")));
  const msgpack::object *items = at(*ttd, "items");
  // A List keeps its arity: position is significant, so a null element is nil
  // rather than dropped.
  check("table/list_arity", items != nullptr && items->type == msgpack::type::ARRAY &&
                                items->via.array.size == 3);
  // `items` is not a decimal-string key, so its int elements naturalize.
  check("table/list_int_natural", is_uint(elem(items, 0), 7));
  check("table/list_null_is_nil", is_nil(elem(items, 1)));
  check("table/list_str", is_str(elem(items, 2), "tail"));
  check("table/nested_map", is_map(at(*ttd, "nested"), 1) &&
                                is_uint(at(*at(*ttd, "nested"), "depth"), 2));
  // All six keys survive, nils included, a composite's arity is its fill's.
  check("table/composite_key_count", is_map(ttd, 6));
  check("table/finality_min_emulated", is_uint(at(*row, "finality"), 0));
}

// Unexpected cells are serialized and counted so a future fill is visible.
void test_counters() {
  Action a = base_action();
  Value::Fields f;
  f.emplace_back("boc", Value::make_cell(vm::CellBuilder().finalize()));
  a.extra = Value::make_dict(std::move(f));

  ActionSerializeStats st;
  Decoded d(serialize_actions({a}, make_view({EmuFinality::finalized}), &st));
  const msgpack::object *row = first_row(d);
  check("counters/decodes", d.ok() && row != nullptr);
  if (row == nullptr) {
    return;
  }
  check("counters/cells_counted", st.cell_values == 1);
  check("counters/nothing_unrenderable", st.unrenderable == 0);
  const msgpack::object *ex = at(*row, "extra");
  const msgpack::object *boc = at(*ex, "boc");
  // The real BOC bytes, base64'd, NOT the twin-diff root-hash substitution.
  check("counters/cell_is_b64_str", boc != nullptr && boc->type == msgpack::type::STR &&
                                        boc->via.str.size > 0);
}

// Every row of one emission carries the same trace key and finality; the
// payload is a flat array with no trace-level envelope.
void test_multiple_actions() {
  Action a = base_action();
  Action b = base_action();
  b.action_id = "second";
  b.type = "call_contract";
  b.success = false;

  Decoded d(serialize_actions({a, b}, make_view({EmuFinality::confirmed, EmuFinality::emulated,
                                                 EmuFinality::finalized})));
  check("multi/array_of_two",
        d.ok() && d.root().type == msgpack::type::ARRAY && d.root().via.array.size == 2);
  const msgpack::object *r0 = elem(&d.root(), 0);
  const msgpack::object *r1 = elem(&d.root(), 1);
  if (r0 == nullptr || r1 == nullptr) {
    check("multi/rows_present", false);
    return;
  }
  check("multi/row0_type", is_str(at(*r0, "type"), "ton_transfer"));
  check("multi/row1_type", is_str(at(*r1, "type"), "call_contract"));
  check("multi/row1_success_false", is_bool(at(*r1, "success"), false));
  // min over the view's node finalities, matching Python's min(tx.finality).
  check("multi/row0_finality_min", is_uint(at(*r0, "finality"), 0));
  check("multi/row1_finality_min", is_uint(at(*r1, "finality"), 0));
  check("multi/shared_trace_key", is_str(at(*r1, "trace_external_hash_norm"), kTraceKey));
}

// create_unknown_action's row shape, gated hermetically. The corpus twin
// covers it on 50 real fixtures.
void test_unknown_action() {
  Trace trace;
  trace.trace_id = kTraceKey;
  const std::pair<const char *, bool> txs[] = {
      {"hashB", false}, {"hashA", false}, {"hashC", true},  // one aborted
  };
  std::int64_t lt = 400, now = 1753900100, seqno = 47110800;
  for (const auto &[hash, aborted] : txs) {
    auto tx = std::make_unique<Transaction>();
    tx->hash = hash;
    tx->aborted = aborted;
    tx->account = kAddrA;  // deliberately repeated: accounts must dedup
    tx->lt = lt;
    tx->now = now;
    tx->mc_block_seqno = seqno;
    lt -= 100;  // DESCENDING, so min/max cannot come from first/last by luck
    now -= 10;
    seqno -= 5;
    trace.transactions.push_back(std::move(tx));
  }
  trace.transactions[1]->account = kAddrB;
  // The PRODUCTION derivation, SchemaTraceLoader has no trace header and calls
  // exactly this, gated here alongside the row that consumes it.
  fill_trace_aggregates(trace);

  Decoded d(serialize_actions({create_unknown_action(trace)},
                              make_view({EmuFinality::emulated})));
  const msgpack::object *row = first_row(d);
  check("unknown/decodes", d.ok() && row != nullptr);
  if (row == nullptr) {
    return;
  }
  check("unknown/type", is_str(at(*row, "type"), "unknown"));
  // The only action_id in the system not derived from a hash.
  check("unknown/action_id_is_trace_id", is_str(at(*row, "action_id"), kTraceKey));
  // success = not any(tx.aborted), a whole-trace property, not a block's.
  check("unknown/success_false_when_any_aborted", is_bool(at(*row, "success"), false));
  // fill_trace_aggregates' min/max, carried through onto the row.
  check("unknown/start_lt", is_uint(at(*row, "start_lt"), 200));
  check("unknown/end_lt", is_uint(at(*row, "end_lt"), 400));
  check("unknown/start_utime", is_uint(at(*row, "start_utime"), 1753900080));
  check("unknown/end_utime", is_uint(at(*row, "end_utime"), 1753900100));
  const msgpack::object *th = at(*row, "tx_hashes");
  check("unknown/tx_hashes_sorted",
        th != nullptr && th->type == msgpack::type::ARRAY && th->via.array.size == 3 &&
            is_str(elem(th, 0), "hashA") && is_str(elem(th, 2), "hashC"));
  const msgpack::object *acc = at(*row, "accounts");
  check("unknown/accounts_deduped",
        acc != nullptr && acc->type == msgpack::type::ARRAY && acc->via.array.size == 2);
  // It sets no directional or composite field at all, so the row is exactly the
  // fixed key set, the same shape Python's create_unknown_action produces.
  check("unknown/no_optional_keys", row->via.map.size == 12);
}

void test_schema_no_gas_ton_transfer() {
  auto bits = [](unsigned char marker) {
    td::Bits256 value;
    value.set_zero();
    value.data()[31] = marker;
    return value;
  };
  auto ordinary = [](schema::ComputeSkipReason reason, bool aborted) {
    schema::TransactionDescr_ord descr{};
    descr.compute_ph = schema::TrComputePhase_skipped{reason};
    descr.aborted = aborted;
    return schema::TransactionDescr{descr};
  };

  schema::Message transfer{};
  transfer.hash = bits(3);
  transfer.source = kAddrA;
  transfer.destination = kAddrB;
  transfer.created_lt = 150;
  transfer.bounce = false;

  schema::Transaction root{};
  root.hash = bits(1);
  root.account = block::StdAddress(0, bits(4));
  root.lt = 100;
  root.now = 1000;
  root.mc_seqno = 10;
  root.orig_status = schema::AccountStatus::active;
  root.end_status = schema::AccountStatus::active;
  root.description = ordinary(schema::cskip_no_state, false);
  root.out_msgs.push_back(transfer);

  schema::Transaction child{};
  child.hash = bits(2);
  child.account = block::StdAddress(0, bits(5));
  child.lt = 200;
  child.now = 1001;
  child.mc_seqno = 10;
  child.orig_status = schema::AccountStatus::active;
  child.end_status = schema::AccountStatus::active;
  child.description = ordinary(schema::cskip_no_gas, true);
  child.in_msg = transfer;

  auto loaded = schema_to_trace("schema-no-gas", {root, child});
  check("schema_no_gas/loads", loaded.is_ok());
  if (loaded.is_error()) {
    return;
  }

  TraceContext ctx;
  ctx.trace = loaded.move_as_ok();
  check("schema_no_gas/reason_loaded",
        ctx.trace.transactions.size() == 2 &&
            ctx.trace.transactions[1]->skipped_reason == std::optional<std::string>("no_gas"));
  ctx.tree = to_tree(ctx.trace);
  check("schema_no_gas/tree_has_root", ctx.tree.root != nullptr);
  if (ctx.tree.root == nullptr) {
    return;
  }
  ctx.root = init_block(ctx.arena, ctx.tree.root);
  check("schema_no_gas/incoming_ton_transfer_not_failed",
        ctx.root != nullptr && ctx.root->next_blocks.size() == 1 &&
            !ctx.root->next_blocks.front()->failed);
}

// Action fills without fixture-corpus coverage are exercised directly here.
void test_bucket_b_fills() {
  BlockArena arena;
  auto row = [&](const char *btype, Value::Fields data) -> Action {
    Block *b = arena.make(btype);
    b->data = Value::make_dict(std::move(data));
    Action a;
    // Each supported block type must produce an action row.
    check(std::string("bucketB/") + btype + "/builds", build_action(b, a));
    return a;
  };
  auto is_str_v = [](const Value &v, const std::string &want) {
    return v.t == VType::Str && v.str == want;
  };
  auto fld = [](const Value &composite, const char *k) -> Value {
    const Value *v = composite.field(k);
    return v != nullptr ? *v : Value::null();
  };

  {
    Action a = row("renew_dns", {{"source", Value::make_account_raw(kAddrA)},
                                 {"destination", Value::make_account_raw(kAddrB)},
                                 {"collection_address", Value::make_account_raw(kAddrA)}});
    check("bucketB/renew_dns/source", is_str_v(a.source, kAddrA));
    check("bucketB/renew_dns/destination", is_str_v(a.destination, kAddrB));
    check("bucketB/renew_dns/asset", is_str_v(a.asset, kAddrA));
  }
  {
    Action a = row("tick_tock", {{"account", Value::make_account_raw(kAddrA)}});
    check("bucketB/tick_tock/type", a.type == "tick_tock");
    check("bucketB/tick_tock/source", is_str_v(a.source, kAddrA));
    check("bucketB/tick_tock/no_destination", a.destination.is_null());
    check("bucketB/tick_tock/no_amount", a.amount.is_null());
  }
  {
    // No payout yet -> still a REQUEST.
    Action a = row("nominator_pool_withdraw_request",
                   {{"source", Value::make_account_raw(kAddrA)},
                    {"pool", Value::make_account_raw(kAddrB)},
                    {"payout_amount", Value::null()}});
    check("bucketB/nominator/type_request", a.type == "stake_withdrawal_request");
    check("bucketB/nominator/no_amount", a.amount.is_null());
    check("bucketB/nominator/provider", is_str_v(fld(a.staking_data, "provider"), "nominator"));
    // Payout present -> the BTYPE is a request but the ACTION is a withdrawal.
    Action b = row("nominator_pool_withdraw_request",
                   {{"source", Value::make_account_raw(kAddrA)},
                    {"pool", Value::make_account_raw(kAddrB)},
                    {"payout_amount", Value::make_amount(td::make_refint(7000000000LL))}});
    check("bucketB/nominator/type_withdrawal", b.type == "stake_withdrawal");
    check("bucketB/nominator/amount",
          b.amount.t == VType::Int && b.amount.num->to_dec_string() == "7000000000");
  }
  {
    Action a = row("tonstakers_withdraw",
                   {{"stake_holder", Value::make_account_raw(kAddrA)},
                    {"pool", Value::make_account_raw(kAddrB)},
                    {"amount", Value::make_amount(td::make_refint(500))},
                    {"asset", Value::make_account_raw(kAddrB)},
                    {"burnt_nft", Value::make_account_raw(kAddrA)},
                    {"tokens_burnt", Value::make_amount(td::make_refint(499))}});
    check("bucketB/tonstakers_withdraw/type", a.type == "stake_withdrawal");
    check("bucketB/tonstakers_withdraw/source", is_str_v(a.source, kAddrA));
    check("bucketB/tonstakers_withdraw/provider",
          is_str_v(fld(a.staking_data, "provider"), "liquid_staking"));
    check("bucketB/tonstakers_withdraw/ts_nft", is_str_v(fld(a.staking_data, "ts_nft"), kAddrA));
    check("bucketB/tonstakers_withdraw/tokens_burnt",
          fld(a.staking_data, "tokens_burnt").t == VType::Int);
  }
  {
    Action a = row("tonstakers_withdraw_request",
                   {{"source", Value::make_account_raw(kAddrA)},
                    {"tsTON_wallet", Value::make_account_raw(kAddrB)},
                    {"pool", Value::make_account_raw(kAddrB)},
                    {"tokens_burnt", Value::make_amount(td::make_refint(42))},
                    {"asset", Value::make_account_raw(kAddrA)},
                    {"minted_nft", Value::make_account_raw(kAddrB)}});
    check("bucketB/tonstakers_request/type", a.type == "stake_withdrawal_request");
    check("bucketB/tonstakers_request/source_secondary", is_str_v(a.source_secondary, kAddrB));
    check("bucketB/tonstakers_request/ts_nft", is_str_v(fld(a.staking_data, "ts_nft"), kAddrB));
  }
  {
    // Note the field-name shift the bare stonfi-v2 producer uses: amount_1 /
    // sender_wallet_1 going in, amount1 / user_jetton_wallet_1 coming out.
    Action a = row("dex_deposit_liquidity",
                   {{"sender", Value::make_account_raw(kAddrA)},
                    {"pool", Value::make_account_raw(kAddrB)},
                    {"dex", Value::make_str("stonfi_v2")},
                    {"amount_1", Value::make_amount(td::make_refint(10))},
                    {"amount_2", Value::make_amount_none()},
                    {"asset_1", Value::make_asset_jetton(kAddrA)},
                    {"asset_2", Value::make_asset_ton()},
                    {"sender_wallet_1", Value::make_account_raw(kAddrB)},
                    {"sender_wallet_2", Value::make_account_none()},
                    {"lp_tokens_minted", Value::make_amount(td::make_refint(3))}});
    const Value &c = a.dex_deposit_liquidity_data;
    check("bucketB/dex_deposit/dex", is_str_v(fld(c, "dex"), "stonfi_v2"));
    check("bucketB/dex_deposit/amount1", fld(c, "amount1").t == VType::Int);
    check("bucketB/dex_deposit/amount2_null", fld(c, "amount2").is_null());
    check("bucketB/dex_deposit/asset1", is_str_v(fld(c, "asset1"), kAddrA));
    check("bucketB/dex_deposit/asset2_ton_null", fld(c, "asset2").is_null());
    check("bucketB/dex_deposit/wallet1", is_str_v(fld(c, "user_jetton_wallet_1"), kAddrB));
    check("bucketB/dex_deposit/lp_minted", fld(c, "lp_tokens_minted").t == VType::Int);
    check("bucketB/dex_deposit/source", is_str_v(a.source, kAddrA));
  }
  {
    Action a = row("dex_withdraw_liquidity",
                   {{"sender", Value::make_account_raw(kAddrA)},
                    {"sender_wallet", Value::make_account_raw(kAddrB)},
                    {"pool", Value::make_account_raw(kAddrB)},
                    {"asset", Value::make_asset_jetton(kAddrA)},
                    {"dex", Value::make_str("dedust")},
                    {"amount1_out", Value::make_amount(td::make_refint(11))},
                    {"amount2_out", Value::make_amount(td::make_refint(12))},
                    {"asset1_out", Value::make_asset_jetton(kAddrA)},
                    {"asset2_out", Value::make_asset_ton()},
                    {"wallet1", Value::make_account_raw(kAddrA)},
                    {"wallet2", Value::make_account_none()},
                    {"dex_jetton_wallet_1", Value::make_account_raw(kAddrB)},
                    {"dex_wallet_1", Value::make_account_raw(kAddrA)},
                    {"dex_wallet_2", Value::make_account_none()},
                    {"dex_jetton_wallet_2", Value::make_account_none()},
                    {"is_refund", Value::make_bool(true)},
                    {"lp_tokens_burnt", Value::make_amount(td::make_refint(9))}});
    const Value &c = a.dex_withdraw_liquidity_data;
    check("bucketB/dex_withdraw/dex", is_str_v(fld(c, "dex"), "dedust"));
    check("bucketB/dex_withdraw/amount1", fld(c, "amount1").t == VType::Int);
    check("bucketB/dex_withdraw/asset2_out_ton_null", fld(c, "asset2_out").is_null());
    check("bucketB/dex_withdraw/dex_jetton_wallet_1",
          is_str_v(fld(c, "dex_jetton_wallet_1"), kAddrB));
    // is_refund stays a Boolean on the wire, never a stringified integer.
    check("bucketB/dex_withdraw/is_refund_bool",
          fld(c, "is_refund").t == VType::Bool && fld(c, "is_refund").boolean);
    check("bucketB/dex_withdraw/lp_burnt", fld(c, "lp_tokens_burnt").t == VType::Int);
    check("bucketB/dex_withdraw/source_secondary", is_str_v(a.source_secondary, kAddrB));
  }
  {
    // nft_purchase must serialize even though the fixture corpus has no sample.
    Action a = row("nft_purchase",
                   {{"prev_owner", Value::make_account_raw(kAddrA)},
                    {"new_owner", Value::make_account_raw(kAddrB)},
                    {"nft_address", Value::make_account_raw(kAddrB)},
                    {"collection_address", Value::make_account_raw(kAddrA)},
                    {"query_id", Value::make_int64(77)},
                    {"price", Value::make_amount(td::make_refint(1500000000LL))},
                    {"nft_index", Value::make_int64(1037)},
                    {"forward_amount", Value::make_amount_none()},
                    {"custom_payload", Value::null()},
                    {"forward_payload", Value::null()},
                    {"response_destination", Value::make_account_none()},
                    {"marketplace", Value::make_str("getgems")},
                    {"marketplace_address", Value::make_account_raw(kAddrB)},
                    {"real_prev_owner", Value::make_account_raw(kAddrA)},
                    {"payout_amount", Value::make_amount(td::make_refint(1425000000LL))},
                    {"payout_comment_encrypted", Value::make_bool(false)},
                    {"payout_comment_encoded", Value::make_bool(false)},
                    {"payout_comment", Value::null()}});
    check("bucketB/nft_purchase/source", is_str_v(a.source, kAddrA));
    check("bucketB/nft_purchase/destination", is_str_v(a.destination, kAddrB));
    check("bucketB/nft_purchase/asset_is_collection", is_str_v(a.asset, kAddrA));
    check("bucketB/nft_purchase/asset_secondary_is_item", is_str_v(a.asset_secondary, kAddrB));
    const Value &c = a.nft_transfer_data;
    // The constant that separates a purchase from a plain transfer.
    check("bucketB/nft_purchase/is_purchase",
          fld(c, "is_purchase").t == VType::Bool && fld(c, "is_purchase").boolean);
    check("bucketB/nft_purchase/price", fld(c, "price").t == VType::Int);
    check("bucketB/nft_purchase/nft_item_index", fld(c, "nft_item_index").t == VType::Int);
    check("bucketB/nft_purchase/marketplace", is_str_v(fld(c, "marketplace"), "getgems"));
    check("bucketB/nft_purchase/payout_amount", fld(c, "payout_amount").t == VType::Int);
    // Unlike fill_nft_transfer, this fill writes NO listing data.
    check("bucketB/nft_purchase/no_listing_data", a.nft_listing_data.is_null());
  }
  {
    // change_dns, one branch per DNS record schema. The key set is PYTHON's,
    // not the branch's: value_schema/flags/address/key always, `value` only for
    // a recognised schema.
    auto rec = [](std::vector<std::pair<std::string, Value>> fields) {
      Value::Fields f;
      for (auto &[k, v] : fields) f.emplace_back(k, std::move(v));
      return Value::make_dict(std::move(f));
    };
    // A raw 32-byte key proves the hex is lowercase. td::buffer_to_hex would
    // render it uppercase, which the Go decoder would accept silently.
    const std::string key_bytes(4, '\xAB');

    Action a = row("change_dns",
                   {{"source", Value::make_account_raw(kAddrA)},
                    {"destination", Value::make_account_raw(kAddrB)},
                    {"collection_address", Value::make_account_raw(kAddrA)},
                    {"key", Value::make_bytes(key_bytes)},
                    {"value", rec({{"schema", Value::make_str("DNSSmcAddress")},
                                   {"address", Value::make_account_raw(kAddrB)},
                                   {"flags", Value::make_int64(3)}})}});
    check("bucketB/change_dns/source", is_str_v(a.source, kAddrA));
    check("bucketB/change_dns/asset", is_str_v(a.asset, kAddrA));
    const Value &c = a.change_dns_record_data;
    check("bucketB/change_dns/key_hex_lowercase", is_str_v(fld(c, "key"), "abababab"));
    check("bucketB/change_dns/value_schema", is_str_v(fld(c, "value_schema"), "DNSSmcAddress"));
    check("bucketB/change_dns/smc_value_is_address", is_str_v(fld(c, "value"), kAddrB));
    // DNSSmcAddress carries its flags alongside the address value.
    check("bucketB/change_dns/smc_flags", fld(c, "flags").t == VType::Int);
    // The database composite has no address field.
    check("bucketB/change_dns/address_key_absent", c.field("address") == nullptr);

    Action t = row("change_dns",
                   {{"source", Value::make_account_raw(kAddrA)},
                    {"destination", Value::make_account_raw(kAddrB)},
                    {"key", Value::make_bytes(key_bytes)},
                    {"value", rec({{"schema", Value::make_str("DNSText")},
                                   {"dns_text", Value::make_str("hello")}})}});
    check("bucketB/change_dns/text_value", is_str_v(fld(t.change_dns_record_data, "value"),
                                                    "hello"));
    check("bucketB/change_dns/text_no_flags",
          fld(t.change_dns_record_data, "flags").is_null());

    Action adnl = row("change_dns",
                      {{"source", Value::make_account_raw(kAddrA)},
                       {"destination", Value::make_account_raw(kAddrB)},
                       {"key", Value::make_bytes(key_bytes)},
                       {"value", rec({{"schema", Value::make_str("DNSAdnlAddress")},
                                      {"address", Value::make_bytes(key_bytes)},
                                      {"flags", Value::make_int64(7)}})}});
    check("bucketB/change_dns/adnl_value_is_hex",
          is_str_v(fld(adnl.change_dns_record_data, "value"), "abababab"));

    // Unknown schemas carry an explicit null value.
    Action u = row("change_dns",
                   {{"source", Value::make_account_raw(kAddrA)},
                    {"destination", Value::make_account_raw(kAddrB)},
                    {"key", Value::make_bytes(key_bytes)},
                    {"value", rec({{"schema", Value::make_str("Unknown")}})}});
    check("bucketB/change_dns/unknown_value_present_and_null",
          u.change_dns_record_data.field("value") != nullptr &&
              fld(u.change_dns_record_data, "value").is_null());
  }
  {
    // delete_dns: the same column, everything but the key nulled.
    Action a = row("delete_dns", {{"source", Value::make_account_raw(kAddrA)},
                                  {"destination", Value::make_account_raw(kAddrB)},
                                  {"collection_address", Value::make_account_raw(kAddrA)},
                                  {"key", Value::make_bytes(std::string(2, '\x0F'))}});
    const Value &c = a.change_dns_record_data;
    check("bucketB/delete_dns/destination", is_str_v(a.destination, kAddrB));
    check("bucketB/delete_dns/key_hex", is_str_v(fld(c, "key"), "0f0f"));
    check("bucketB/delete_dns/value_schema_null", fld(c, "value_schema").is_null());
    check("bucketB/delete_dns/no_value_key", c.field("value") == nullptr);
  }
}

// EVAA liquidation Stage A: exercise the fill and wire surface directly. No
// matcher or build_action dispatch exists until Stage B.
void test_evaa_liquidate_surface() {
  Action a = base_action();
  Value::Fields data;
  data.emplace_back("liquidator", Value::make_account_raw(kAddrA));
  data.emplace_back("borrower", Value::make_account_raw(kAddrB));
  data.emplace_back("borrower_contract", Value::make_account_raw(kAddrA));
  data.emplace_back("collateral_asset_id", Value::make_int64(0x1234));
  data.emplace_back("collateral_amount", Value::make_int64(125));
  data.emplace_back("debt_amount",
                    Value::make_int(td::string_to_int256(
                        std::string("340282366920938463463374607431768211456"))));
  data.emplace_back("is_success", Value::make_bool(false));
  data.emplace_back("fail_reason", Value::make_str("master_not_enough_liquidity"));
  fill_evaa_liquidate(Value::make_dict(std::move(data)), a);

  check("evaa_liquidate/source", a.source.t == VType::Str && a.source.str == kAddrA);
  check("evaa_liquidate/destination",
        a.destination.t == VType::Str && a.destination.str == kAddrB);
  check("evaa_liquidate/borrower_contract",
        a.destination_secondary.t == VType::Str && a.destination_secondary.str == kAddrA);
  check("evaa_liquidate/collateral_amount",
        a.amount.t == VType::Int && a.amount.num->to_dec_string() == "125");
  check("evaa_liquidate/success", !a.success);
  const Value *asset_id = a.evaa_liquidate_data.field("asset_id");
  check("evaa_liquidate/asset_id",
        asset_id != nullptr && asset_id->t == VType::Str && asset_id->str == "0x1234");
  check("evaa_liquidate/render_column",
        render_action(a).find("evaa_liquidate_data:") != std::string::npos);

  Decoded d(serialize_actions({a}, make_view({EmuFinality::finalized})));
  const msgpack::object *row = first_row(d);
  check("evaa_liquidate/msgpack_row", d.ok() && row != nullptr);
  if (row == nullptr) {
    return;
  }
  const msgpack::object *details = at(*row, "evaa_liquidate_data");
  check("evaa_liquidate/msgpack_details", is_map(details, 3));
  if (details != nullptr) {
    check("evaa_liquidate/debt_amount_string",
          is_str(at(*details, "debt_amount"),
                 "340282366920938463463374607431768211456"));
    check("evaa_liquidate/fail_reason",
          is_str(at(*details, "fail_reason"), "master_not_enough_liquidity"));
  }
}

// A view with no nodes degrades to `emulated`, the conservative end, and what
// ton-index-go reads as FinalityStatePending.
void test_empty_view_finality() {
  Decoded d(serialize_actions({base_action()}, make_view({})));
  const msgpack::object *row = first_row(d);
  check("view/empty_nodes_decodes", d.ok() && row != nullptr);
  if (row != nullptr) {
    check("view/empty_nodes_finality_pending", is_uint(at(*row, "finality"), 0));
  }
}

// view_finality is not reachable from a fixture and is easy to reverse without
// an A/B failure. It feeds the `finality` wire key and is exercised here without
// requiring Redis.

EmuTraceView view_of(std::initializer_list<EmuFinality> finalities) {
  EmuTraceView v;
  for (EmuFinality f : finalities) {
    EmuTxRef node;
    node.finality = f;
    v.nodes.push_back(node);
  }
  return v;
}

void test_view_finality() {
  check("view_finality/empty_is_emulated", view_finality(view_of({})) == EmuFinality::emulated);
  check("view_finality/single_finalized",
        view_finality(view_of({EmuFinality::finalized})) == EmuFinality::finalized);
  // The MIN, not the root's and not the max: one emulated node makes the whole
  // emission emulated, which is what stops a partly-emulated re-emission from
  // outranking the finalized emission it would be superseding.
  check("view_finality/min_over_mixed",
        view_finality(view_of({EmuFinality::finalized, EmuFinality::emulated,
                               EmuFinality::confirmed})) == EmuFinality::emulated);
  check("view_finality/min_confirmed",
        view_finality(view_of({EmuFinality::finalized, EmuFinality::confirmed})) ==
            EmuFinality::confirmed);
}

}  // namespace

int run_action_msgpack_test() {
  g_fail = 0;
  test_empty_actions();
  test_fixed_keys();
  test_child_row_fields();
  test_null_and_stringify();
  test_natural_types();
  test_string_list_fields();
  test_out_of_range_bounded_int();
  test_determinism();
  test_wire_fixture();
  test_value_table();
  test_counters();
  test_multiple_actions();
  test_unknown_action();
  test_schema_no_gas_ton_transfer();
  test_bucket_b_fills();
  test_evaa_liquidate_surface();
  test_empty_view_finality();
  test_view_finality();
  std::printf("ACTIONS-MSGPACK-TEST %s\n", g_fail == 0 ? "ALL PASS" : "FAILURES");
  return g_fail == 0 ? 0 : 1;
}

}  // namespace mch
