// LayerZero message parsers. Objects preserve source cells, remaining slices,
// and asserted header integers. Identifier fields use minimal lowercase `0x`
// hexadecimal, while message bits retain leading zeros and pad the final byte.
// Receive, send, commit, and DVN verification follow their distinct reference
// chains; verification status codes are mapped to strings in the parser.
#include "parse/Parsers.h"

#include "parse/PSlice.h"

#include "common/refint.h"
#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cellslice.h"
#include "vm/cells/CellBuilder.h"

#include <string>
#include <utility>

namespace mch {

namespace {

td::Result<std::string> lz_hex_min(vm::CellSlice &cs, int bits) {
  auto v = cs.fetch_int256(bits, false);
  if (v.is_null()) {
    return td::Status::Error("lz: int underflow");
  }
  return "0x" + td::hex_string(v);
}

td::Result<Value> lz_parse_path(const td::Ref<vm::Cell> &c) {
  PSlice ps = pslice_from_cell(c);
  auto hi = ps.cs.fetch_int256(152, false);
  if (hi.is_null() || td::dec_string(hi) != "8903714975572488637007080065659") {
    return td::Status::Error("lz path: header info mismatch");
  }
  auto hf = ps.cs.fetch_int256(198, true);
  if (hf.is_null() || td::dec_string(hf) != "-1") {
    return td::Status::Error("lz path: header filler mismatch");
  }
  if (!ps.cs.have(32)) {
    return td::Status::Error("lz path: src_eid underflow");
  }
  auto src_eid = ps.cs.fetch_ulong(32);
  TRY_RESULT(src_oapp, lz_hex_min(ps.cs, 256));
  if (!ps.cs.have(32)) {
    return td::Status::Error("lz path: dst_eid underflow");
  }
  auto dst_eid = ps.cs.fetch_ulong(32);
  TRY_RESULT(dst_oapp, lz_hex_min(ps.cs, 256));
  TRY_RESULT(rem, pslice_to_cell(ps));
  Value::Fields f;
  f.emplace_back("b", Value::make_cell(c));
  f.emplace_back("s", Value::make_cell(std::move(rem)));
  f.emplace_back("header_info", Value::make_int(std::move(hi)));
  f.emplace_back("header_filler", Value::make_int(std::move(hf)));
  f.emplace_back("src_eid", Value::make_int64(static_cast<std::int64_t>(src_eid)));
  f.emplace_back("src_oapp", Value::make_str(std::move(src_oapp)));
  f.emplace_back("dst_eid", Value::make_int64(static_cast<std::int64_t>(dst_eid)));
  f.emplace_back("dst_oapp", Value::make_str(std::move(dst_oapp)));
  return Value::make_obj(std::move(f));
}

td::Result<Value> lz_parse_packet(const td::Ref<vm::Cell> &c) {
  PSlice ps = pslice_from_cell(c);
  auto hi = ps.cs.fetch_int256(152, false);
  if (hi.is_null() || td::dec_string(hi) != "417359019239977417716476838698419835") {
    return td::Status::Error("lz packet: header info mismatch");
  }
  auto hf = ps.cs.fetch_int256(198, true);
  if (hf.is_null() || td::dec_string(hf) != "-1") {
    return td::Status::Error("lz packet: header filler mismatch");
  }
  if (ps.off >= ps.refs.size()) {
    return td::Status::Error("lz packet: path ref missing");
  }
  auto path_cell = ps.refs[ps.off++];
  TRY_RESULT(path, lz_parse_path(path_cell));
  if (ps.off >= ps.refs.size()) {
    return td::Status::Error("lz packet: message ref missing");
  }
  auto msg_cell = ps.refs[ps.off++];
  std::string message;
  {
    bool special = false;
    vm::CellSlice mcs;
    try {
      mcs = vm::load_cell_slice_special(msg_cell, special);
    } catch (...) {
      return td::Status::Error("lz packet: bad message cell");
    }
    unsigned n_bits = mcs.size();
    size_t n_bytes = (n_bits + 7) / 8;
    std::string buf(n_bytes, '\0');
    if (n_bits > 0) {
      td::BitPtr(reinterpret_cast<unsigned char *>(buf.data())).copy_from(mcs.data_bits(), n_bits);
    }
    static const char hexd[] = "0123456789abcdef";
    message = "0x";
    for (char cbyte : buf) {
      unsigned char b = static_cast<unsigned char>(cbyte);
      message += hexd[b >> 4];
      message += hexd[b & 0xF];
    }
  }
  if (!ps.cs.have(64)) {
    return td::Status::Error("lz packet: nonce underflow");
  }
  auto nonce = ps.cs.fetch_ulong(64);
  TRY_RESULT(guid, lz_hex_min(ps.cs, 256));
  TRY_RESULT(rem, pslice_to_cell(ps));
  Value::Fields f;
  f.emplace_back("b", Value::make_cell(c));
  f.emplace_back("s", Value::make_cell(std::move(rem)));
  f.emplace_back("header_info", Value::make_int(std::move(hi)));
  f.emplace_back("header_filler", Value::make_int(std::move(hf)));
  f.emplace_back("path", std::move(path));
  f.emplace_back("message", Value::make_str(std::move(message)));
  f.emplace_back("nonce", Value::make_int(td::make_refint(nonce)));
  f.emplace_back("guid", Value::make_str(std::move(guid)));
  return Value::make_obj(std::move(f));
}

// md::LzSend (py:87 LayerZeroMDLzSend). 80-bit NAME ("lzSend" big-endian,
// zero-padded), 180-bit header info, 90 filler ones, then the scalar prefix and
// three refs; the third ref carries the fee block. Every assert in the Python
// __init__ is a hard check here, a mismatch rejects the whole parse, exactly
// as the raised AssertionError does.
td::Result<Value> lz_parse_md_lz_send(const td::Ref<vm::Cell> &c) {
  PSlice ps = pslice_from_cell(c);
  auto name = ps.cs.fetch_int256(80, false);
  if (name.is_null() || td::dec_string(name) != "119272640966244") {
    return td::Status::Error("lz lzSend: wrong name");
  }
  auto hi = ps.cs.fetch_int256(180, false);
  if (hi.is_null() ||
      td::dec_string(hi) != "582890735024998957421269964955452773563747974476099581") {
    return td::Status::Error("lz lzSend: header info mismatch");
  }
  auto hf = ps.cs.fetch_int256(90, true);
  if (hf.is_null() || td::dec_string(hf) != "-1") {
    return td::Status::Error("lz lzSend: header filler mismatch");
  }
  if (!ps.cs.have(64)) {
    return td::Status::Error("lz lzSend: send_request_id underflow");
  }
  auto send_request_id = ps.cs.fetch_ulong(64);
  TRY_RESULT(send_msglib_manager, lz_hex_min(ps.cs, 256));
  TRY_RESULT(send_msglib, lz_hex_min(ps.cs, 256));
  if (ps.off + 2 >= ps.refs.size()) {
    return td::Status::Error("lz lzSend: packet/options/fee refs missing");
  }
  TRY_RESULT(packet, lz_parse_packet(ps.refs[ps.off++]));
  auto extra_options = ps.refs[ps.off++];
  auto fee_ref = ps.refs[ps.off++];
  PSlice fs = pslice_from_cell(fee_ref);
  auto conn = fs.cs.fetch_int256(256, false);
  auto native_fee = fs.cs.fetch_int256(128, false);
  auto zro_fee = fs.cs.fetch_int256(128, false);
  if (conn.is_null() || native_fee.is_null() || zro_fee.is_null()) {
    return td::Status::Error("lz lzSend: fee block underflow");
  }
  if (fs.off + 1 >= fs.refs.size()) {
    return td::Status::Error("lz lzSend: enforced_options/callback_data refs missing");
  }
  auto enforced_options = fs.refs[fs.off++];
  auto callback_data = fs.refs[fs.off++];
  TRY_RESULT(rem, pslice_to_cell(ps));
  Value::Fields f;
  f.emplace_back("b", Value::make_cell(c));
  f.emplace_back("s", Value::make_cell(std::move(rem)));
  f.emplace_back("name", Value::make_int(std::move(name)));
  f.emplace_back("header_info", Value::make_int(std::move(hi)));
  f.emplace_back("header_filler", Value::make_int(std::move(hf)));
  f.emplace_back("send_request_id", Value::make_int(td::make_refint(send_request_id)));
  f.emplace_back("send_msglib_manager", Value::make_str(std::move(send_msglib_manager)));
  f.emplace_back("send_msglib", Value::make_str(std::move(send_msglib)));
  f.emplace_back("packet", std::move(packet));
  f.emplace_back("extra_options", Value::make_cell(std::move(extra_options)));
  f.emplace_back("ref", Value::make_cell(std::move(fee_ref)));
  f.emplace_back("send_msglib_connection", Value::make_int(std::move(conn)));
  f.emplace_back("native_fee", Value::make_int(std::move(native_fee)));
  f.emplace_back("zro_fee", Value::make_int(std::move(zro_fee)));
  f.emplace_back("enforced_options", Value::make_cell(std::move(enforced_options)));
  f.emplace_back("callback_data", Value::make_cell(std::move(callback_data)));
  return Value::make_obj(std::move(f));
}

}  // namespace

td::Result<Value> parse_layerzero_oapp_execute_callback(const td::Ref<vm::Cell> &body) {
  // self.cell = slice.to_cell() on the FRESH body slice, pytoniq rebuilds the
  // cell; hash-identical to the body root, but rebuilt to mirror the semantics.
  PSlice ps = pslice_from_cell(body);
  TRY_RESULT(cell0, pslice_to_cell(ps));
  if (ps.refs.empty()) {
    return td::Status::Error("lz callback: refs[0] missing");
  }
  PSlice r0 = pslice_from_cell(ps.refs[0]);
  if (r0.refs.empty()) {
    return td::Status::Error("lz callback: refs[0].refs[0] missing");
  }
  TRY_RESULT(packet, lz_parse_packet(r0.refs[0]));
  Value::Fields f;
  f.emplace_back("cell", Value::make_cell(std::move(cell0)));
  f.emplace_back("packet", std::move(packet));
  return Value::make_obj(std::move(f));
}

td::Result<Value> parse_layerzero_channel_send_callback(const td::Ref<vm::Cell> &body) {
  // py:600. `self.cell = s.to_cell()` runs on the FRESH slice (the whole body),
  // then `self.opcode = self.s.load_uint(32)` advances the SAME slice, so `s`
  // is the body minus 32 bits with every ref still on it.
  PSlice ps = pslice_from_cell(body);
  TRY_RESULT(cell0, pslice_to_cell(ps));
  if (!ps.cs.have(32)) {
    return td::Status::Error("lz send callback: opcode underflow");
  }
  auto opcode = ps.cs.fetch_ulong(32);
  TRY_RESULT(rem, pslice_to_cell(ps));
  if (ps.refs.empty()) {
    return td::Status::Error("lz send callback: refs[0] missing");
  }
  PSlice r0 = pslice_from_cell(ps.refs[0]);
  if (r0.refs.empty()) {
    return td::Status::Error("lz send callback: refs[0].refs[0] missing");
  }
  PSlice r1 = pslice_from_cell(r0.refs[0]);
  if (r1.refs.empty()) {
    return td::Status::Error("lz send callback: refs[0].refs[0].refs[0] missing");
  }
  TRY_RESULT(lz_send, lz_parse_md_lz_send(r1.refs[0]));
  Value::Fields f;
  f.emplace_back("cell", Value::make_cell(std::move(cell0)));
  f.emplace_back("s", Value::make_cell(std::move(rem)));
  f.emplace_back("opcode", Value::make_int64(static_cast<std::int64_t>(opcode)));
  f.emplace_back("lz_send", std::move(lz_send));
  return Value::make_obj(std::move(f));
}

td::Result<Value> parse_layerzero_channel_commit_packet(const td::Ref<vm::Cell> &body) {
  // py:384. Pure ref walk, the 32 opcode bits are never read, so nothing is
  // skipped here either.
  PSlice ps = pslice_from_cell(body);
  if (ps.refs.empty()) {
    return td::Status::Error("lz commit packet: extended md ref missing");
  }
  auto extended_md_cell = ps.refs[0];
  PSlice md = pslice_from_cell(extended_md_cell);
  if (md.refs.empty()) {
    return td::Status::Error("lz commit packet: packet ref missing");
  }
  auto packet_cell = md.refs[0];
  TRY_RESULT(packet, lz_parse_packet(packet_cell));
  Value::Fields f;
  f.emplace_back("extended_md_cell", Value::make_cell(std::move(extended_md_cell)));
  f.emplace_back("packet_cell", Value::make_cell(std::move(packet_cell)));
  f.emplace_back("packet", std::move(packet));
  return Value::make_obj(std::move(f));
}

td::Result<Value> parse_uln_connection_verify_callback(const td::Ref<vm::Cell> &body) {
  // py:913. md::MdObj ref[0] = md::VerificationStatus. Python checks the FILLER
  // first and the INFO second (both raise ValueError); order is immaterial to
  // the outcome but kept for readability against the source.
  PSlice ps = pslice_from_cell(body);
  if (ps.refs.empty()) {
    return td::Status::Error("uln verify callback: md obj ref missing");
  }
  auto md_obj_cell = ps.refs[0];
  PSlice md = pslice_from_cell(md_obj_cell);
  if (md.refs.empty()) {
    return td::Status::Error("uln verify callback: verification status ref missing");
  }
  auto status_cell = md.refs[0];
  PSlice vs = pslice_from_cell(status_cell);
  auto hi = vs.cs.fetch_int256(116, false);
  auto hf = vs.cs.fetch_int256(234, true);
  if (hf.is_null() || td::dec_string(hf) != "-1") {
    return td::Status::Error("uln verify callback: header filler is not 0");
  }
  if (hi.is_null() || td::dec_string(hi) != "38421788582694199859296615363593851") {
    return td::Status::Error("uln verify callback: header info mismatch");
  }
  if (!vs.cs.have(96)) {
    return td::Status::Error("uln verify callback: nonce/status underflow");
  }
  auto nonce = vs.cs.fetch_ulong(64);
  auto status_code = vs.cs.fetch_ulong(32);
  // md::VerificationStatus codes (py:936-947). Anything else renders the raw
  // DECIMAL code, matching Python's f"unknown_{self.status_code}".
  std::string status;
  switch (status_code) {
    case 0x3bbc306bu:
      status = "succeeded";
      break;
    case 0x7fcbb4acu:
      status = "nonce_out_of_range";
      break;
    case 0x29c53fabu:
      status = "dvn_not_configured";
      break;
    default:
      status = "unknown_" + std::to_string(status_code);
      break;
  }
  Value::Fields f;
  f.emplace_back("md_obj_cell", Value::make_cell(std::move(md_obj_cell)));
  f.emplace_back("verification_status_cell", Value::make_cell(std::move(status_cell)));
  f.emplace_back("nonce", Value::make_int(td::make_refint(nonce)));
  f.emplace_back("status_code", Value::make_int64(static_cast<std::int64_t>(status_code)));
  f.emplace_back("status", Value::make_str(std::move(status)));
  return Value::make_obj(std::move(f));
}

}  // namespace mch
