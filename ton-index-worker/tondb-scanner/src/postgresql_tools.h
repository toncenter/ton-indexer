#pragma once
#include <pqxx/pqxx>
#include "IndexData.h"

namespace pqxx
{

template<> struct nullness<schema::BlockReference> : pqxx::no_null<schema::BlockReference> {};

template<> struct string_traits<schema::BlockReference>
{
  static constexpr bool converts_to_string{true};
  static constexpr bool converts_from_string{false};

  static zview to_buf(char *begin, char *end, schema::BlockReference const &value) {
    return zview{
      begin,
      static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
  }

  static char *into_buf(char *begin, char *end, schema::BlockReference const &value) {
    std::ostringstream stream;
    stream << "(" << value.workchain << ", " << value.shard << ", " << value.seqno << ")";
    auto text = stream.str();
    if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
      throw conversion_overrun{"Not enough buffer for schema::BlockReference."};
    std::memcpy(begin, text.c_str(), std::size(text) + 1);
    return begin + std::size(text) + 1;
  }
  static std::size_t size_buffer(schema::BlockReference const &value) noexcept {
    return 64;
  }
};

template<> struct nullness<td::RefInt256>
{
  static constexpr bool has_null{true};

  static constexpr bool always_null{false};

  static bool is_null(td::RefInt256 const &value) {
    return value.is_null();
  }
};

template<> struct string_traits<td::RefInt256>
{
  static constexpr bool converts_to_string{true};
  static constexpr bool converts_from_string{false};

  static zview to_buf(char *begin, char *end, td::RefInt256 const &value) {
    return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
  }

  static char *into_buf(char *begin, char *end, td::RefInt256 const &value) {
    auto text = value->to_dec_string();
    if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
      throw conversion_overrun{"Not enough buffer for td::RefInt256."};
    std::memcpy(begin, text.c_str(), std::size(text) + 1);
    return begin + std::size(text) + 1;
  }
  static std::size_t size_buffer(td::RefInt256 const &value) noexcept {
    return 128;
  }
};

template<> struct nullness<block::StdAddress> : pqxx::no_null<block::StdAddress> {};

template<> struct string_traits<block::StdAddress>
{
  static constexpr bool converts_to_string{true};
  static constexpr bool converts_from_string{false};

  static zview to_buf(char *begin, char *end, block::StdAddress const &value) {
    return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
  }

  static char *into_buf(char *begin, char *end, block::StdAddress const &value) {
    std::ostringstream stream;
    stream << value.workchain << ":" << value.addr;
    auto text = stream.str();
    if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
      throw conversion_overrun{"Not enough buffer for block::StdAddress."};
    std::memcpy(begin, text.c_str(), std::size(text) + 1);
    return begin + std::size(text) + 1;
  }
  static std::size_t size_buffer(block::StdAddress const &value) noexcept {
    return 80;
  }
};

template<> struct nullness<td::Bits256> : pqxx::no_null<td::Bits256> {};

template<> struct string_traits<td::Bits256>
{
  static constexpr bool converts_to_string{true};
  static constexpr bool converts_from_string{false};

  static zview to_buf(char *begin, char *end, td::Bits256 const &value) {
    return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
  }

  static char *into_buf(char *begin, char *end, td::Bits256 const &value) {
    auto text = td::base64_encode(value.as_slice());
    if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
      throw conversion_overrun{"Not enough buffer for td::Bits256."};
    std::memcpy(begin, text.c_str(), std::size(text) + 1);
    return begin + std::size(text) + 1;
  }
  static std::size_t size_buffer(td::Bits256 const &value) noexcept {
    return 64;
  }
};

template<> struct nullness<vm::CellHash> : pqxx::no_null<vm::CellHash> {};

template<> struct string_traits<vm::CellHash>
{
  static constexpr bool converts_to_string{true};
  static constexpr bool converts_from_string{false};

  static zview to_buf(char *begin, char *end, vm::CellHash const &value) {
    return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
  }

  static char *into_buf(char *begin, char *end, vm::CellHash const &value) {
    auto text = td::base64_encode(value.as_slice());
    if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
      throw conversion_overrun{"Not enough buffer for vm::CellHash."};
    std::memcpy(begin, text.c_str(), std::size(text) + 1);
    return begin + std::size(text) + 1;
  }
  static std::size_t size_buffer(vm::CellHash const &value) noexcept {
    return 64;
  }
};
}

class PopulateTableStream {
private:
    pqxx::work& txn_;
    std::string table_name_;
    std::initializer_list<std::string_view> column_names_;
    int batch_size_;
    bool with_copy_;
    bool is_first_row_{true};
    std::optional<pqxx::stream_to> copy_stream_;
    std::ostringstream insert_stream_;
    std::string conflict_clause_;

    bool conflict_clause_added_{false};

    enum class ConflictAction { None, DoNothing, DoUpdate };
    ConflictAction conflict_action_{ConflictAction::None};
    std::initializer_list<std::string_view> conflict_columns_;
    std::string update_condition_;

public:
    struct UpsertConfig {
        std::initializer_list<std::string_view> conflict_columns;
        std::string_view update_condition;
    };

    PopulateTableStream(
        pqxx::work& txn,
        std::string_view table_name,
        std::initializer_list<std::string_view> column_names,
        int batch_size,
        bool with_copy = false)
        : txn_(txn)
        , table_name_(table_name)
        , column_names_(column_names)
        , batch_size_(batch_size)
        , with_copy_(with_copy)
    {
        initializeStream();
    }

    void setConflictDoNothing() {
        if (with_copy_) {
            throw std::runtime_error("ON CONFLICT not supported with COPY mode");
        }
        conflict_action_ = ConflictAction::DoNothing;
        buildConflictClause();
    }

    void setConflictDoUpdate(std::initializer_list<std::string_view> conflict_columns, std::string_view update_condition) {
        if (with_copy_) {
            throw std::runtime_error("ON CONFLICT not supported with COPY mode");
        }
        conflict_action_ = ConflictAction::DoUpdate;
        conflict_columns_ = std::move(conflict_columns);
        update_condition_ = std::move(update_condition);
        buildConflictClause();
    }

private:
    void buildConflictClause() {
      std::ostringstream conflict_stream;

      if (conflict_action_ != ConflictAction::None) {
          conflict_stream << " ON CONFLICT ";

          if (conflict_columns_.size()) {
              conflict_stream << "(";
              auto it = conflict_columns_.begin();
              conflict_stream << *it++;
              for(; it != conflict_columns_.end(); ++it) {
                  conflict_stream << ", " << *it;
              }
              conflict_stream << ") ";
          }

          if (conflict_action_ == ConflictAction::DoNothing) {
              conflict_stream << "DO NOTHING";
          } else if (conflict_action_ == ConflictAction::DoUpdate) {
              conflict_stream << "DO UPDATE SET ";
              bool first = true;
              for (const auto& col : column_names_) {
                  if (!first) conflict_stream << ", ";
                  conflict_stream << col << " = EXCLUDED." << col;
                  first = false;
              }
              if (!update_condition_.empty()) {
                  conflict_stream << " WHERE " << update_condition_;
              }
          }
      }

      conflict_clause_ = conflict_stream.str();
    }

    void initializeStream() {
        if (with_copy_) {
            copy_stream_.emplace(pqxx::stream_to::table(txn_, {table_name_}, column_names_));
            return;
        }

        insert_stream_.str("");
        insert_stream_.clear();
        is_first_row_ = true;

        // Build INSERT part
        insert_stream_ << "INSERT INTO " << table_name_ << " (";
        bool first = true;
        for (const auto& col : column_names_) {
          if (!first) insert_stream_ << ", ";
          insert_stream_ << col;
          first = false;
        }
        insert_stream_ << ") VALUES ";
    }

public:
    template <typename ...T>
    void insert_row(std::tuple<T...> row) {
      if (std::tuple_size<decltype(row)>::value != column_names_.size()) {
        throw std::runtime_error("row size doesn't match column names size");
      }
      if (with_copy_) {
        copy_stream_->write_row(row);
        return;
      }

      if (conflict_clause_added_) {
        throw std::runtime_error("can't insert row after conflict clause");
      }

      if (!is_first_row_) {
        insert_stream_ << ",";
      }
      is_first_row_ = false;

      insert_stream_ << "(";
      bool first = true;
      std::apply([&](const auto&... args) {
        ((insert_stream_ << (first ? "" : ",") << txn_.quote(args), first = false), ...);
      }, row);
      insert_stream_ << ")";
    }

    std::string get_str() {
      if (with_copy_) {
        throw std::runtime_error("get_str not supported with COPY mode");
      }
      if (is_first_row_) {
        return "";
      }
      if (!conflict_clause_added_) {
        insert_stream_ << conflict_clause_ << ";";
        conflict_clause_added_ = true;
      }
      return insert_stream_.str();
    }

    void finish() {
      if (with_copy_) {
        copy_stream_->complete();
        return;
      }
      if (is_first_row_) {
        return;
      }

      txn_.exec0(get_str());
    }
};

