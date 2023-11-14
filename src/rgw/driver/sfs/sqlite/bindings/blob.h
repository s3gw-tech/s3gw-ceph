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

#include <tuple>
#include <type_traits>

#include "rgw/driver/sfs/sqlite/conversion_utils.h"
// we need to include dbapi_type_wrapper.h only because including dbapi.h
// creates circular dependencies
#include "rgw/driver/sfs/sqlite/dbapi_type_wrapper.h"
#include "rgw/driver/sfs/sqlite/sqlite_orm.h"
#include "rgw_common.h"

namespace blob_utils {

template <typename T, typename Tuple>
struct has_type;

template <typename T>
struct has_type<T, std::tuple<>> : std::false_type {};

template <typename T, typename U, typename... Ts>
struct has_type<T, std::tuple<U, Ts...>> : has_type<T, std::tuple<Ts...>> {};

template <typename T, typename... Ts>
struct has_type<T, std::tuple<T, Ts...>> : std::true_type {};

// list of types that are stored as blobs and have the encode/decode functions
using BlobTypes = std::tuple<
    rgw::sal::Attrs, ACLOwner, rgw_placement_rule,
    std::map<std::string, RGWAccessKey>, std::map<std::string, RGWSubUser>,
    RGWUserCaps, std::list<std::string>, std::map<int, std::string>,
    RGWQuotaInfo, std::set<std::string>, RGWBucketWebsiteConf,
    std::map<std::string, uint32_t>, RGWObjectLock, rgw_sync_policy_info>;
}  // namespace blob_utils

namespace sqlite_orm {

template <typename T>
inline constexpr bool is_sqlite_blob =
    blob_utils::has_type<T, blob_utils::BlobTypes>::value;

template <class T>
struct type_printer<T, typename std::enable_if<is_sqlite_blob<T>, void>::type>
    : public blob_printer {};

template <class T>
struct statement_binder<
    T, typename std::enable_if<is_sqlite_blob<T>, void>::type> {
  int bind(sqlite3_stmt* stmt, int index, const T& value) {
    std::vector<char> blobValue;
    rgw::sal::sfs::sqlite::encode_blob(value, blobValue);
    return statement_binder<std::vector<char>>().bind(stmt, index, blobValue);
  }
};

template <class T>
struct field_printer<
    T, typename std::enable_if<is_sqlite_blob<T>, void>::type> {
  std::string operator()(const T& value) const { return "ENCODED BLOB"; }
};

template <class T>
struct row_extractor<
    T, typename std::enable_if<is_sqlite_blob<T>, void>::type> {
  T extract(sqlite3_stmt* stmt, int columnIndex) {
    auto blob_data = sqlite3_column_blob(stmt, columnIndex);
    auto blob_size = sqlite3_column_bytes(stmt, columnIndex);
    if (blob_data == nullptr || blob_size < 0) {
      throw(std::system_error(
          ERANGE, std::system_category(),
          "Invalid blob at column : (" + std::to_string(columnIndex) + ")"
      ));
    }
    T ret;
    rgw::sal::sfs::sqlite::decode_blob(
        reinterpret_cast<const char*>(blob_data),
        static_cast<size_t>(blob_size), ret
    );
    return ret;
  }
};
}  // namespace sqlite_orm

namespace rgw::sal::sfs::dbapi::sqlite {
template <typename T>
struct has_sqlite_type<T, SQLITE_BLOB, void>
    : blob_utils::has_type<T, blob_utils::BlobTypes> {};

template <class T>
inline std::enable_if<sqlite_orm::is_sqlite_blob<T>, int>::type bind_col_in_db(
    sqlite3_stmt* stmt, int inx, const T& val
) {
  std::vector<char> blobValue;
  rgw::sal::sfs::sqlite::encode_blob(val, blobValue);
  return dbapi::sqlite::bind_col_in_db(stmt, inx, blobValue);
}
template <class T>
inline std::enable_if<sqlite_orm::is_sqlite_blob<T>, void>::type
store_result_in_db(sqlite3_context* db, const T& val) {
  std::vector<char> blobValue;
  rgw::sal::sfs::sqlite::encode_blob(val, blobValue);
  dbapi::sqlite::store_result_in_db(db, blobValue);
}
template <class T>
inline std::enable_if<sqlite_orm::is_sqlite_blob<T>, T>::type
get_col_from_db(sqlite3_stmt* stmt, int inx, result_type<T>) {
  if (sqlite3_column_type(stmt, inx) == SQLITE_NULL) {
    ceph_abort_msg("cannot make blob value from NULL");
  }
  auto blob_data = sqlite3_column_blob(stmt, inx);
  auto blob_size = sqlite3_column_bytes(stmt, inx);
  if (blob_data == nullptr || blob_size < 0) {
    ceph_abort_msg("Invalid blob at column : (" + std::to_string(inx) + ")");
  }
  T ret;
  rgw::sal::sfs::sqlite::decode_blob(
      reinterpret_cast<const char*>(blob_data), static_cast<size_t>(blob_size),
      ret
  );
  return ret;
}

template <class T>
inline std::enable_if<sqlite_orm::is_sqlite_blob<T>, T>::type
get_val_from_db(sqlite3_value* value, result_type<T>) {
  if (sqlite3_value_type(value) == SQLITE_NULL) {
    ceph_abort_msg("cannot make blob value from NULL");
  }
  std::vector<char> vector_value;
  vector_value = get_val_from_db(value, result_type<std::vector<char>>());
  T ret;
  rgw::sal::sfs::sqlite::decode_blob(
      reinterpret_cast<const char*>(vector_value),
      static_cast<size_t>(vector_value.size()), ret
  );
  return ret;
}

}  // namespace rgw::sal::sfs::dbapi::sqlite
