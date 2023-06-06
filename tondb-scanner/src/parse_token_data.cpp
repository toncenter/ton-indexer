#include "InsertManager.h"
#include "tokens.h"
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
    return td::Status::Error(ErrorCode::SMC_INTERFACE_PARSE_ERROR, "Not byte aligned");
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
      return td::Status::Error(ErrorCode::SMC_INTERFACE_PARSE_ERROR, "Not byte aligned");
    }

    return std::string(buffer, bw.get_byte_ptr());
  } catch (vm::VmError& err) {
    return td::Status::Error(ErrorCode::SMC_INTERFACE_PARSE_ERROR, PSLICE() << "Exception while parsing chunks data: " << err.get_msg());
  }
}

td::Result<std::string> parse_content_data(td::Ref<vm::Cell> data) {
  auto cs = vm::load_cell_slice_ref(data);
  switch (tokens::gen::t_ContentData.check_tag(*cs)) {
    case tokens::gen::ContentData::snake: {
      tokens::gen::ContentData::Record_snake snake_record;
      if (!tlb::csr_unpack(cs, snake_record)) {
        return td::Status::Error(ErrorCode::SMC_INTERFACE_PARSE_ERROR, "Failed to unpack snake token data");
      }
      return parse_snake_data(snake_record.data);
    }
    case tokens::gen::ContentData::chunks: {
      tokens::gen::ContentData::Record_chunks chunks_record;
      if (!tlb::csr_unpack(cs, chunks_record)) {
        return td::Status::Error(ErrorCode::SMC_INTERFACE_PARSE_ERROR, "Failed to unpack chunks token data");
      }
      return parse_chunks_data(chunks_record.data);
    }
    default:
      return td::Status::Error(ErrorCode::SMC_INTERFACE_PARSE_ERROR, "Unknown content data");
  }
}

td::Result<std::map<std::string, std::string>> parse_token_data(td::Ref<vm::Cell> cell) {
  static std::string attributes[] = {"uri", "name", "description", "image", "image_data", "symbol", "decimals", "amount_style", "render_type"};
  
  auto cs = vm::load_cell_slice_ref(cell);
  switch (tokens::gen::t_FullContent.check_tag(*cs)) {
    case tokens::gen::FullContent::offchain: {
      tokens::gen::FullContent::Record_offchain offchain_record;
      if (!tlb::csr_unpack(cs, offchain_record)) {
        return td::Status::Error(ErrorCode::SMC_INTERFACE_PARSE_ERROR, "Failed to unpack offchain token data");
      }
      auto uri = parse_snake_data(offchain_record.uri);
      if (uri.is_error()) {
        return uri.move_as_error();
      }
      return std::map<std::string, std::string>{{"uri", uri.move_as_ok()}};
    }
    case tokens::gen::FullContent::onchain: {
      tokens::gen::FullContent::Record_onchain onchain_record;
      if (!tlb::csr_unpack(cs, onchain_record)) {
        return td::Status::Error(ErrorCode::SMC_INTERFACE_PARSE_ERROR, "Failed to unpack onchain token data");
      }

      try {
        vm::Dictionary dict(onchain_record.data, 256);
        std::map<std::string, std::string> res;
        
        for (auto attr : attributes) {
          auto value = dict.lookup_ref(td::sha256_bits256(attr));
          if (value.not_null()) {
            auto attr_data = parse_content_data(value);
            if (attr_data.is_error()) {
              LOG(ERROR) << "Failed to parse attribute " << attr << ": " << attr_data.move_as_error().to_string();
              continue;
            }
            res[attr] = attr_data.move_as_ok();
          }
        }
        return res;
      } catch (vm::VmError& err) {
        return td::Status::Error(ErrorCode::SMC_INTERFACE_PARSE_ERROR, PSLICE() << "Failed to parse onchain dictionary: " << err.get_msg());
      }
    }
    default:
      return td::Status::Error(ErrorCode::SMC_INTERFACE_PARSE_ERROR, "Unknown token data type");
  }
}