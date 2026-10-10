#include "special.h"
#include "schemes.h"
#include "tolk_array.h"

namespace ton_marker {

namespace {

bool try_parse_tg_wallet_bulk(unsigned opcode, vm::CellSlice cs, std::string& output, int print_limit) {
    constexpr unsigned kBulkInternal = 0x73896e74;
    constexpr unsigned kBulkExternal = 0x73896e75;
    if (opcode != kBulkInternal && opcode != kBulkExternal) return false;

    std::string json;
    tlb::JsonPrinter printer(&json);
    printer.set_limit(print_limit);
    if (!printer.open("tg_wallet_signed") || !printer.fetch_bits_field(cs, 512, "signature") ||
        cs.fetch_ulong(32) != opcode || !printer.field("request") ||
        !printer.open(opcode == kBulkInternal ? "tg_wallet_send_bulk_internal" : "tg_wallet_send_bulk_external") ||
        !printer.fetch_uint_field(cs, 32, "subwallet_id") ||
        !printer.fetch_uint_field(cs, 32, "valid_until") ||
        !printer.fetch_uint_field(cs, 32, "seqno")) {
        return false;
    }

    auto array = cs;
    if (!array.have(8) || !printer.fetch_uint_field(cs, 8, "messages_count") ||
        !printer.field("messages") ||
        !TolkArray(schemes::t_TgWalletMessageToSend, 1).print_skip(printer, array) ||
        !array.empty_ext() || !printer.close() || !printer.close()) {
        return false;
    }
    output = std::move(json);
    return true;
}

} // namespace

int count_actions_depth(vm::Ref<vm::Cell> list) {
    if (list.is_null()) {
        return 0;
    }
    int i = -1;
    do {
        ++i;
        bool special = true;
        auto cs = vm::load_cell_slice_special(std::move(list), special);
        if (special) {
            break;
        }
        list = cs.prefetch_ref();
    } while (list.not_null());
    return i;
}

// JsonPrinter should be reset if this function fails, 
// bc it writes failed result to pp
bool try_parse_special(std::string opcode_name, vm::CellSlice& cs, tlb::JsonPrinter& pp, std::string& output_str,
                       int print_limit) {
    // Telegram wallet signs the request with the signature at the beginning,
    // so the first 32 bits of the body are not its opcode.
    if (cs.have(512 + 32)) {
        auto request = cs;
        request.advance(512);
        const auto opcode = request.prefetch_ulong(32);
        const schemes::TgWalletRequest tg_request;
        for (size_t i = 0; i < sizeof(tg_request.cons_tag) / sizeof(tg_request.cons_tag[0]); ++i) {
            if (tg_request.cons_tag[i] == opcode) {
                if (try_parse_tg_wallet_bulk(opcode, cs, output_str, print_limit)) return true;
                auto copy = cs;
                const schemes::TgWalletMsgBody parser;
                if (parser.print_skip(pp, copy) && copy.empty_ext()) {
                    return true;
                }
                output_str = "";
                pp = tlb::JsonPrinter(&output_str);
                pp.set_limit(print_limit);
                break;
            }
        }
    }
    if (opcode_name == "w5_external_signed_request" || opcode_name == "w5_internal_signed_request" || opcode_name == "w5_extension_action_request") {        
        // we can't use W5MsgBody just as is. it has snake cells for actions,
        // and tlb-generated code is not capable of detecting how many cells are in the snake.
        // so we first just count actions depth on refs,
        // and then parsing with W5MsgBody using the counts.
        auto cs_copy = cs;
        int out_actions_count = 0;
        int extended_actions_count = 0;

        // skip opcode
        cs_copy.advance(32);

        // skip fields before actions
        if (opcode_name == "w5_extension_action_request") {
            cs_copy.advance(64); // skip query_id
        } else {
        cs_copy.advance(32); // skip wallet_id
        cs_copy.advance(32); // skip valid_until
        cs_copy.advance(32); // skip msg_seqno
        }

        // check flags for actions
        bool has_out_actions = cs_copy.fetch_ulong(1);
        bool has_extended_actions = cs_copy.fetch_ulong(1);

        // get out_actions depth from first ref if present
        if (has_out_actions && cs_copy.size_refs() > 0) {
            auto first_ref = cs_copy.fetch_ref();
            if (first_ref.not_null()) {
                out_actions_count = count_actions_depth(first_ref);
            }
        }

        if (has_extended_actions) {
            extended_actions_count = 1; // extended_actions are starting in the cell itself
            while (cs_copy.size_refs() > 0) {
                extended_actions_count++;
                cs_copy = vm::load_cell_slice(cs_copy.fetch_ref());
            }
        }
        schemes::W5MsgBody w5_parser(out_actions_count, extended_actions_count-1); // last ref is not empty cell, that's why -1
        auto cs_copy2 = cs;
        return w5_parser.print_skip(pp, cs_copy2);
    }
    if (opcode_name == "highload_v3_internal_request") {
        // same as W5MsgBody
        // actions_count = n
        auto cs_copy = cs;
        int out_actions_count = 0;
        if (cs_copy.size_refs() > 0) {
            auto first_ref = cs_copy.fetch_ref();
            if (first_ref.not_null()) {
                out_actions_count = count_actions_depth(first_ref);
            }
        }
        schemes::HighloadV3InternalRequest highload_v3_parser(out_actions_count);
        auto cs_copy2 = cs;
        return highload_v3_parser.print_skip(pp, cs_copy2);
    }
    if (opcode_name.find("unknown") != std::string::npos) {
        // try parse externals that start with signatures
        
        // helper to try wallet parser with output restoration on failure
        auto try_wallet_parser = [&cs, &pp, &output_str, print_limit](const auto& parser) -> bool {
            auto cs_copy = cs;
            if (parser.print_skip(pp, cs_copy) && cs_copy.empty_ext()) { // empty_ext() checks that all refs are empty
                return true;
            }
            // restore output on failure
            output_str = "";
            pp = tlb::JsonPrinter(&output_str);
            pp.set_limit(print_limit);
            return false;
        };

        // order should not really matter, since we're checking remaining data,
        // it may give false positives only when extra bits remaining in refs (that are loaded deep in print_skips)
        
        // preprocessed wallet v2 - 
        if (cs.size_refs() >= 1) {
            auto msg_ref = cs.prefetch_ref();
            if (msg_ref.not_null()) {
                auto inner_cs = vm::load_cell_slice(msg_ref);
                if (inner_cs.size_refs() >= 1) {
                    auto actions_ref = inner_cs.prefetch_ref();
                    int actions_count = count_actions_depth(actions_ref);
                    if (actions_count > 0) {
                        const schemes::PreprocessedWalletV2MsgBody parser_pw2(actions_count);
                        if (try_wallet_parser(parser_pw2)) {
                            return true;
                        }
                    }
                }
            }
        }

        // highload v3 - signature + ref to msg_inner
        const schemes::HighloadV3MsgBody parser_hl3;
        if (try_wallet_parser(parser_hl3)) {
            return true;
        }

        // highload v2 - signature + subwallet_id + query_id + inline hashmap
        const schemes::HighloadV2MsgBody parser_hl2;
        if (try_wallet_parser(parser_hl2)) {
            return true;
        }

        // highload v1 - signature + subwallet_id + valid_until + msg_seqno + ref to hashmap
        const schemes::HighloadV1MsgBody parser_hl1;
        if (try_wallet_parser(parser_hl1)) {
            return true;
        }

        // wallet v4 - signature + subwallet_id + valid_until + msg_seqno + op
        // n = msgs count, calculated as refs count
        int msgs_count = cs.size_refs();
        const schemes::W4MsgBody parser_w4(msgs_count);
        if (try_wallet_parser(parser_w4)) {
            return true;
        }

        // wallet v3 - signature + subwallet_id + valid_until + msg_seqno + msgs
        // n = msgs count, calculated as refs count
        const schemes::W3MsgBody parser_w3(msgs_count);
        if (try_wallet_parser(parser_w3)) {
            return true;
        }

        // wallet v1/v2 - signature + msg_seqno + valid_until + msgs
        // n = msgs count, calculated as refs count
        const schemes::W1W2MsgBody parser_w12(msgs_count);
        if (try_wallet_parser(parser_w12)) {
            return true;
        }
    }
    
    return false;
}

} // namespace ton_marker
