#include "InsertManager.h"
#include "tokens-tlb.h"
#include "common/checksum.h"


td::Result<std::string> parse_snake_data(td::Ref<vm::CellSlice> data) {
  size_t bsize = 1024 * 8;
  unsigned char buffer[bsize];
  td::BitPtr bw{buffer};

  while (true) {
    if (bw.offs + data->size() > bsize * 8) {
      break; // buffer overflow
    }
    bw.concat(data->prefetch_bits(data->size()));

    auto cell = data->prefetch_ref();
    if (cell.is_null()) {
      break;
    }
    data = vm::load_cell_slice_ref(cell);
  }
  
  if (!bw.byte_aligned()) {
    return td::Status::Error("Not byte aligned");
  }

  return std::string(buffer, bw.get_byte_ptr());
}

td::Result<std::string> parse_chunks_data(td::Ref<vm::CellSlice> data) {
  try {
    vm::Dictionary dict(data, 32);
    
    size_t bsize = 1024 * 8;
    unsigned char buffer[bsize];
    td::BitPtr bw{buffer};

    uint c = 0;
    while (dict.uint_key_exists(c)) {
      auto value = dict.lookup_ref(td::BitArray<32>(c));
      if (value.not_null()) {
        auto data = vm::load_cell_slice_ref(value);
        if (bw.offs + data->size() > bsize * 8) {
          break; // buffer overflow
        }
        
        bw.concat(data->prefetch_bits(data->size()));
      }
      c++;
    }

    if (!bw.byte_aligned()) {
      return td::Status::Error("Not byte aligned");
    }

    return std::string(buffer, bw.get_byte_ptr());
  } catch (vm::VmError& err) {
    return td::Status::Error(PSLICE() << "Exception while parsing chunks data: " << err.get_msg());
  }
}

td::Result<std::string> parse_content_data(td::Ref<vm::CellSlice> cs) {
  switch (tokens::gen::t_ContentData.check_tag(*cs)) {
    case tokens::gen::ContentData::snake: {
      tokens::gen::ContentData::Record_snake snake_record;
      if (!tlb::csr_unpack(cs, snake_record)) {
        return td::Status::Error("Failed to unpack snake token data");
      }
      return parse_snake_data(snake_record.data);
    }
    case tokens::gen::ContentData::chunks: {
      tokens::gen::ContentData::Record_chunks chunks_record;
      if (!tlb::csr_unpack(cs, chunks_record)) {
        return td::Status::Error("Failed to unpack chunks token data");
      }
      return parse_chunks_data(chunks_record.data);
    }
    default:
      return td::Status::Error("Unknown content data");
  }
}

td::Result<std::map<std::string, std::string>> parse_token_data(td::Ref<vm::Cell> cell) {
  static std::string attributes[] = {"uri", "name", "description", "image", "image_data", "symbol", "decimals", "amount_style", "render_type"};
  
  try {
    auto cs = vm::load_cell_slice_ref(cell);
    switch (tokens::gen::t_FullContent.check_tag(*cs)) {
      case tokens::gen::FullContent::offchain: {
        tokens::gen::FullContent::Record_offchain offchain_record;
        if (!tlb::csr_unpack(cs, offchain_record)) {
          return td::Status::Error("Failed to unpack offchain token data");
        }
        auto uri_r = parse_snake_data(offchain_record.uri);
        if (uri_r.is_error()) {
          return uri_r.move_as_error();
        }
        auto uri = uri_r.move_as_ok();
        if (!td::check_utf8(uri)) {
          return td::Status::Error("Invalid uri");
        }
        return std::map<std::string, std::string>{{"uri", std::move(uri)}};
      }
      case tokens::gen::FullContent::onchain: {
        tokens::gen::FullContent::Record_onchain onchain_record;
        if (!tlb::csr_unpack(cs, onchain_record)) {
          return td::Status::Error("Failed to unpack onchain token data");
        }

        vm::Dictionary dict(onchain_record.data, 256);
        std::map<std::string, std::string> res;
        
        for (auto attr : attributes) {
          auto value_cs = dict.lookup(td::sha256_bits256(attr));
          if (value_cs.not_null()) {
            // TEP-64 standard requires that all attributes are stored in a dictionary with a single reference as a value:
            //    onchain#00 data:(HashmapE 256 ^ContentData) = FullContent;
            // however, some contracts store attributes as a direct value:
            //    onchain#00 data:(HashmapE 256 ContentData) = FullContent;
            // so we need to handle both cases.
            if (value_cs->size() == 0 && value_cs->size_refs() == 1) {
              value_cs = vm::load_cell_slice_ref(value_cs->prefetch_ref());
            }
          
            auto attr_data_r = parse_content_data(value_cs);
            if (attr_data_r.is_error()) {
              LOG(ERROR) << "Failed to parse attribute " << attr << ": " << attr_data_r.move_as_error().message();
              continue;
            }
            auto attr_data = attr_data_r.move_as_ok();
            if (attr == "image_data") {
              res[attr] = td::base64_encode(attr_data);
            } else {
              if (!td::check_utf8(attr_data)) {
                LOG(ERROR) << "Invalid data (not utf8) in attribute " << attr;
                continue;
              }
              res[attr] = std::move(attr_data);
            }
          }
        }
        return res;
      }
      default:
        return td::Status::Error("Unknown token data type");
    }
  } catch (vm::VmError& err) {
    return td::Status::Error(PSLICE() << "Failed to parse token data: " << err.get_msg());
  }
}
