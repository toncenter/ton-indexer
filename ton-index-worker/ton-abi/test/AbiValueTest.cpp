// These units build AbiValue trees by hand without involving the kernel or
// generated walker. They assert the exact canonical JSON dump for each kind,
// including lowercase hex, cellOf {"ref":...}, the union hasValueField wrapper,
// and struct "$" first.

#include "AbiTestSupport.h"

namespace {

using namespace ton_abi;

td::RefInt256 Idec(const std::string &s) {
  return td::dec_string_to_int256(s);
}

td::Ref<vm::Cell> cell_u(unsigned long long val, int bits) {
  vm::CellBuilder b;
  REQUIRE(b.store_long_bool(static_cast<long long>(val), bits));
  return b.finalize();
}

}  // namespace

TEST_CASE("AbiValue dump: Int -> quoted decimal string, positive and negative") {
  CHECK(AbiValue::make_int(Idec("0")).to_json() == R"("0")");
  CHECK(AbiValue::make_int(Idec("123456789")).to_json() == R"("123456789")");
  CHECK(AbiValue::make_int(Idec("-38")).to_json() == R"("-38")");
}

TEST_CASE("AbiValue dump: Bool -> JSON bool") {
  CHECK(AbiValue::make_bool(true).to_json() == "true");
  CHECK(AbiValue::make_bool(false).to_json() == "false");
}

TEST_CASE("AbiValue dump: Null -> JSON null") {
  CHECK(AbiValue::make_null().to_json() == "null");
}

TEST_CASE("AbiValue dump: Void -> {\"$\":\"void\"}") {
  CHECK(AbiValue::make_void().to_json() == R"({"$":"void"})");
}

TEST_CASE("AbiValue dump: Address Std -> \"wc:hex\", lowercase hash") {
  AbiAddress a;
  a.kind = AbiAddressKind::Std;
  a.workchain = 0;
  // hash = 0x0123456789ABCDEF... (vm gives uppercase; dump must lowercase it)
  vm::CellBuilder b;
  REQUIRE(b.store_long_bool(0x0123456789ABCDEFLL, 64));
  REQUIRE(b.store_long_bool(0x1122334455667788LL, 64));
  REQUIRE(b.store_long_bool(0x2233445566778899LL, 64));
  REQUIRE(b.store_long_bool(0x3344556677889900LL, 64));
  auto cs = vm::load_cell_slice(b.finalize());
  td::Bits256 hash;
  REQUIRE(cs.prefetch_bits_to(hash));
  a.hash = hash;
  std::string got = AbiValue::make_address(a).to_json();
  CHECK(got == R"("0:0123456789abcdef112233445566778822334455667788993344556677889900")");
  CHECK(got.find_first_of("ABCDEF") == std::string::npos);  // vm gives uppercase; dump must lowercase
}

TEST_CASE("AbiValue dump: Address Extern -> {\"extern\":{\"bits\":n,\"value\":hex}}") {
  AbiAddress a;
  a.kind = AbiAddressKind::Extern;
  a.ext_bits = 12;
  vm::CellBuilder b;
  REQUIRE(b.store_long_bool(0xABC, 12));
  a.ext_value = vm::load_cell_slice_ref(b.finalize());
  CHECK(AbiValue::make_address(a).to_json() == R"({"extern":{"bits":12,"value":"abc"}})");
}

TEST_CASE("AbiValue dump: Address None -> \"none\" (any_address)") {
  AbiAddress a;
  a.kind = AbiAddressKind::None;
  CHECK(AbiValue::make_address(a).to_json() == R"("none")");
}

TEST_CASE("AbiValue dump: Cell -> base64(std_boc_serialize(root,2))") {
  // Same cell as AbiLeavesRefTest's BOC-mode pin: 0x1234 (16 bits) + ref 0xAB (8 bits).
  vm::CellBuilder root;
  REQUIRE(root.store_long_bool(0x1234, 16));
  REQUIRE(root.store_ref_bool(cell_u(0xAB, 8)));
  CHECK(AbiValue::make_cell(root.finalize()).to_json() == R"("te6cckEBAgEACAABBBI0AQACqyFjJBU=")");
}

TEST_CASE("AbiValue dump: CellOf -> {\"ref\": <inner>}") {
  auto v = AbiValue::make_cell_of(AbiValue::make_int(Idec("42")));
  CHECK(v.to_json() == R"({"ref":"42"})");
}

TEST_CASE("AbiValue dump: Bits -> {\"bits\":binary-string,\"refs\":[...]}, no refs") {
  vm::CellBuilder b;
  REQUIRE(b.store_long_bool(0b10110, 5));
  auto v = AbiValue::make_bits(vm::load_cell_slice_ref(b.finalize()));
  CHECK(v.to_json() == R"({"bits":"10110","refs":[]})");
}

TEST_CASE("AbiValue dump: Bits with one ref -> refs carries its BOC b64") {
  vm::CellBuilder b;
  REQUIRE(b.store_long_bool(0b11, 2));
  REQUIRE(b.store_ref_bool(cell_u(0xAB, 8)));
  auto v = AbiValue::make_bits(vm::load_cell_slice_ref(b.finalize()));
  std::string got = v.to_json();
  CHECK(got.rfind(R"({"bits":"11","refs":[")", 0) == 0);
  CHECK(got.find("]}") != std::string::npos);
}

TEST_CASE("AbiValue dump: String -> JSON string, escapes quote/backslash/control chars") {
  CHECK(AbiValue::make_string("hello").to_json() == R"("hello")");
  CHECK(AbiValue::make_string(R"(a"b\c)").to_json() == R"("a\"b\\c")");
  CHECK(AbiValue::make_string("a\nb").to_json() == R"("a\nb")");
}

// String dumping uses @ton/core's lossy-UTF8 decode
// (Node Buffer.toString('utf8')), U+FFFD = the 3-byte sequence EF BF BD.
// string_v itself stays RAW (pack round-trips the original bytes exactly);
// only to_json() applies this. Every case below was cross-checked against
// live `node -e` output (see AbiValue.cpp's decode_utf8_lossy comment).
TEST_CASE("AbiValue dump: String lossy-UTF8 decode matches @ton/core (Node Buffer.toString) exactly") {
  const std::string kRepl = "\xEF\xBF\xBD";  // U+FFFD

  SUBCASE("valid multi-byte passes through: euro sign U+20AC (E2 82 AC)") {
    std::string bytes = "A\xE2\x82\xAC" "B";
    CHECK(AbiValue::make_string(bytes).to_json() == "\"A\xE2\x82\xAC" "B\"");
  }
  SUBCASE("valid 4-byte emoji U+1F600 (F0 9F 98 80) passes through") {
    std::string bytes = "\xF0\x9F\x98\x80";
    CHECK(AbiValue::make_string(bytes).to_json() == "\"\xF0\x9F\x98\x80\"");
  }
  SUBCASE("lone continuation byte -> one replacement") {
    CHECK(AbiValue::make_string(std::string("A\x80" "B")).to_json() == "\"A" + kRepl + "B\"");
  }
  SUBCASE("truncated 2-byte at end of buffer -> one replacement") {
    CHECK(AbiValue::make_string(std::string("A\xC2")).to_json() == "\"A" + kRepl + "\"");
  }
  SUBCASE("3-byte lead+valid-cont1 but bad cont2 -> ONE replacement, bad byte reprocessed (not swallowed)") {
    // E2 82 followed by 'A' (not a continuation byte): node -> "A�A"
    CHECK(AbiValue::make_string(std::string("A\xE2\x82" "A")).to_json() == "\"A" + kRepl + "A\"");
  }
  SUBCASE("overlong 2-byte lead C0/C1 -> always-invalid, one replacement each, NOT swallowing the next byte") {
    // C0 80: node -> "A��B" (TWO replacements: C0 alone, then 80 alone)
    CHECK(AbiValue::make_string(std::string("A\xC0\x80" "B")).to_json() == "\"A" + kRepl + kRepl + "B\"");
    // C1 BF: node -> "��"
    CHECK(AbiValue::make_string(std::string("\xC1\xBF")).to_json() == "\"" + kRepl + kRepl + "\"");
  }
  SUBCASE("overlong 3-byte E0 80 80 -> THREE replacements (bad 2nd byte is reprocessed, not swallowed)") {
    CHECK(AbiValue::make_string(std::string("\xE0\x80\x80")).to_json() == "\"" + kRepl + kRepl + kRepl + "\"");
  }
  SUBCASE("UTF-16 surrogate encoding ED A0 80 -> THREE replacements") {
    CHECK(AbiValue::make_string(std::string("\xED\xA0\x80")).to_json() == "\"" + kRepl + kRepl + kRepl + "\"");
  }
  SUBCASE("valid 3-byte then an invalid trailing byte -> exactly one extra replacement") {
    // E2 82 AC (valid euro) + FF (always-invalid lead): node -> "€�"
    CHECK(AbiValue::make_string(std::string("\xE2\x82\xAC\xFF")).to_json() == "\"\xE2\x82\xAC" + kRepl + "\"");
  }
  SUBCASE("valid 4-byte emoji then truncated 4-byte lead at buffer end -> ONE replacement for the pair") {
    // F0 9F 98 80 (emoji) + F0 9F (truncated): node -> "\U0001F600�" (not two)
    CHECK(AbiValue::make_string(std::string("\xF0\x9F\x98\x80\xF0\x9F")).to_json() ==
          "\"\xF0\x9F\x98\x80" + kRepl + "\"");
  }
  SUBCASE("4-byte lead with out-of-range 2nd byte (overlong, F0 80..) -> FOUR replacements") {
    CHECK(AbiValue::make_string(std::string("\xF0\x80\x80\x80")).to_json() ==
          "\"" + kRepl + kRepl + kRepl + kRepl + "\"");
  }
  SUBCASE("4-byte lead F4 with 2nd byte beyond U+10FFFF cap -> FOUR replacements") {
    CHECK(AbiValue::make_string(std::string("\xF4\x90\x80\x80")).to_json() ==
          "\"" + kRepl + kRepl + kRepl + kRepl + "\"");
  }
  SUBCASE("always-invalid lead 0xF5 followed by continuations -> FOUR replacements (each byte independent)") {
    CHECK(AbiValue::make_string(std::string("\xF5\x80\x80\x80")).to_json() ==
          "\"" + kRepl + kRepl + kRepl + kRepl + "\"");
  }
  SUBCASE("4-byte: valid lead+cont1, bad 3rd byte -> one replacement, bad byte reprocessed as ASCII") {
    CHECK(AbiValue::make_string(std::string("\xF0\x9F" "AB")).to_json() == "\"" + kRepl + "AB\"");
  }
  SUBCASE("4-byte: valid lead+cont1+cont2, bad 4th byte -> one replacement, bad byte reprocessed as ASCII") {
    CHECK(AbiValue::make_string(std::string("\xF0\x9F\x98" "A")).to_json() == "\"" + kRepl + "A\"");
  }
  SUBCASE("embedded NUL byte is valid ASCII, dumped as the \u0000 control-char escape") {
    std::string s = "A";
    s += (char)0;
    s += "B";
    CHECK(AbiValue::make_string(s).to_json() == R"("A\u0000B")");
  }
  SUBCASE("empty string") {
    CHECK(AbiValue::make_string("").to_json() == "\"\"");
  }
}

TEST_CASE("AbiValue dump: List -> JSON array") {
  std::vector<AbiValue> items;
  items.push_back(AbiValue::make_int(Idec("1")));
  items.push_back(AbiValue::make_int(Idec("2")));
  items.push_back(AbiValue::make_int(Idec("3")));
  CHECK(AbiValue::make_list(std::move(items)).to_json() == R"(["1","2","3"])");
}

TEST_CASE("AbiValue dump: List empty -> []") {
  CHECK(AbiValue::make_list({}).to_json() == "[]");
}

TEST_CASE("AbiValue dump: Struct -> {\"$\":name, fields in decl order}") {
  auto v = AbiValue::make_struct("IncreaseCounter");
  v.add_field("queryId", AbiValue::make_int(Idec("0")));
  v.add_field("increaseBy", AbiValue::make_int(Idec("1")));
  CHECK(v.to_json() == R"({"$":"IncreaseCounter","queryId":"0","increaseBy":"1"})");
}

TEST_CASE("AbiValue dump: Struct with no fields -> just {\"$\":name}") {
  auto v = AbiValue::make_struct("EmptyMsg");
  CHECK(v.to_json() == R"({"$":"EmptyMsg"})");
}

TEST_CASE("AbiValue dump: Union (hasValueField) -> {\"$\":label,\"value\":inner}") {
  auto v = AbiValue::make_union("int32", AbiValue::make_int(Idec("7")));
  CHECK(v.to_json() == R"({"$":"int32","value":"7"})");
}

TEST_CASE("AbiValue dump: Union wrapping a struct value (duplicate-label case)") {
  auto inner = AbiValue::make_struct("Foo");
  inner.add_field("x", AbiValue::make_bool(true));
  auto v = AbiValue::make_union("Foo2", std::move(inner));
  CHECK(v.to_json() == R"({"$":"Foo2","value":{"$":"Foo","x":true}})");
}

TEST_CASE("AbiValue dump: struct-labeled union variant is NOT wrapped -- walker uses inner directly") {
  // hasValueField == false case: the walker never calls make_union; it just
  // uses the Struct value as-is, which already carries its own "$".
  auto inner = AbiValue::make_struct("MyVariant");
  inner.add_field("a", AbiValue::make_int(Idec("5")));
  CHECK(inner.to_json() == R"({"$":"MyVariant","a":"5"})");
}

TEST_CASE("AbiValue dump: Map -> array of [key,value] pairs in given (wire) order") {
  std::vector<std::pair<AbiValue, AbiValue>> entries;
  entries.emplace_back(AbiValue::make_int(Idec("1")), AbiValue::make_int(Idec("100")));
  entries.emplace_back(AbiValue::make_int(Idec("5")), AbiValue::make_int(Idec("500")));
  CHECK(AbiValue::make_map(std::move(entries)).to_json() == R"([["1","100"],["5","500"]])");
}

TEST_CASE("AbiValue dump: Map empty -> []") {
  CHECK(AbiValue::make_map({}).to_json() == "[]");
}

TEST_CASE("AbiValue dump: nested struct field (List of Structs)") {
  auto s1 = AbiValue::make_struct("Point");
  s1.add_field("x", AbiValue::make_int(Idec("1")));
  s1.add_field("y", AbiValue::make_int(Idec("2")));
  auto s2 = AbiValue::make_struct("Point");
  s2.add_field("x", AbiValue::make_int(Idec("3")));
  s2.add_field("y", AbiValue::make_int(Idec("4")));
  std::vector<AbiValue> items;
  items.push_back(std::move(s1));
  items.push_back(std::move(s2));
  CHECK(AbiValue::make_list(std::move(items)).to_json() ==
        R"([{"$":"Point","x":"1","y":"2"},{"$":"Point","x":"3","y":"4"}])");
}
