// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2023 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#pragma once

#include <type_traits>

#include "rgw/driver/sfs/sqlite/sqlite_orm.h"
#include "rgw_common.h"
// we need to include dbapi_type_wrapper.h only because including dbapi.h
// creates circular dependencies
#include "rgw/driver/sfs/sqlite/dbapi_type_wrapper.h"

namespace sqlite_orm {
template <>
struct type_printer<uuid_d> : public text_printer {};

template <>
struct statement_binder<uuid_d> {
  int bind(sqlite3_stmt* stmt, int index, const uuid_d& value) const {
    return statement_binder<std::string>().bind(stmt, index, value.to_string());
  }
};

template <>
struct field_printer<uuid_d> {
  std::string operator()(const uuid_d& value) const {
    return value.to_string();
  }
};

template <>
struct row_extractor<uuid_d> {
  uuid_d extract(const char* row_value) const {
    if (row_value) {
      uuid_d ret_value;
      if (!ret_value.parse(row_value)) {
        throw std::system_error(
            ERANGE, std::system_category(),
            "incorrect uuid string (" + std::string(row_value) + ")"
        );
      }
      return ret_value;
    } else {
      // ! row_value
      throw std::system_error(
          ERANGE, std::system_category(), "incorrect uuid string (nullptr)"
      );
    }
  }

  uuid_d extract(sqlite3_stmt* stmt, int columnIndex) const {
    auto str = sqlite3_column_text(stmt, columnIndex);
    // sqlite3_colume_text returns const unsigned char*
    return this->extract(reinterpret_cast<const char*>(str));
  }
  uuid_d extract(sqlite3_value* row_value) const {
    // sqlite3_colume_text returns const unsigned char*
    auto characters =
        reinterpret_cast<const char*>(sqlite3_value_text(row_value));
    return extract(characters);
  }
};
}  // namespace sqlite_orm

namespace rgw::sal::sfs::dbapi::sqlite {

template <>
struct has_sqlite_type<uuid_d, SQLITE_TEXT, void> : ::std::true_type {};

inline int bind_col_in_db(sqlite3_stmt* stmt, int inx, const uuid_d& val) {
  return bind_col_in_db(stmt, inx, val.to_string());
}
inline void store_result_in_db(sqlite3_context* db, const uuid_d& val) {
  store_result_in_db(db, val.to_string());
}
inline uuid_d
get_col_from_db(sqlite3_stmt* stmt, int inx, result_type<uuid_d>) {
  std::string db_value = get_col_from_db(stmt, inx, result_type<std::string>());
  uuid_d ret_value;
  if (!ret_value.parse(db_value.c_str())) {
    throw std::system_error(
        ERANGE, std::system_category(),
        "incorrect uuid string (" + db_value + ")"
    );
  }
  return ret_value;
}

inline uuid_d get_val_from_db(sqlite3_value* value, result_type<uuid_d>) {
  std::string db_value = get_val_from_db(value, result_type<std::string>());
  uuid_d ret_value;
  if (!ret_value.parse(db_value.c_str())) {
    throw std::system_error(
        ERANGE, std::system_category(),
        "incorrect uuid string (" + db_value + ")"
    );
  }
  return ret_value;
}
}  // namespace rgw::sal::sfs::dbapi::sqlite
