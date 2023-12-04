// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#pragma once

#include "rgw/driver/sfs/sqlite/sqlite_orm.h"
#include "rgw_acl.h"
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
}  // namespace blob_utils

namespace rgw::sal::sfs::sqlite {

// Normally the encode/decode methods for rgw types are found in the ceph
// namespace. But there are a few types where that's not true.
// This tuple lists all the types where the encode/decode methods are NOT in the
// ceph namespace.
// This is required to specify which call will need your type when encoding or
// decoding it from/to a bufferlist
using TypesDecodeIsNOTInCephNamespace = std::tuple<
    RGWAccessControlPolicy, RGWQuotaInfo, RGWObjectLock, RGWUserCaps, ACLOwner,
    rgw_placement_rule>;

/// Returns if a type has its encode/decode methods in the ceph namespace.
template <typename T>
inline constexpr bool ceph_ns_decode =
    !blob_utils::has_type<T, TypesDecodeIsNOTInCephNamespace>::value;

template <typename BLOB_HOLDER, typename DEST>
void decode_blob(const BLOB_HOLDER& blob_holder, DEST& dest) {
  bufferlist buffer;
  buffer.append(
      reinterpret_cast<const char*>(blob_holder.data()), blob_holder.size()
  );
  ceph::decode(dest, buffer);
}

template <typename DEST>
void decode_blob(const char* data, size_t data_size, DEST& dest) {
  bufferlist buffer;
  buffer.append(data, data_size);
  ceph::decode(dest, buffer);
}

template <typename ORIGIN, typename BLOB_HOLDER>
typename std::enable_if<ceph_ns_decode<ORIGIN>, void>::type encode_blob(
    const ORIGIN& origin, BLOB_HOLDER& dest
) {
  bufferlist buffer;
  ceph::encode(origin, buffer);
  dest.reserve(buffer.length());
  std::copy(
      buffer.c_str(), buffer.c_str() + buffer.length(), std::back_inserter(dest)
  );
}

template <typename ORIGIN, typename BLOB_HOLDER>
typename std::enable_if<!ceph_ns_decode<ORIGIN>, void>::type encode_blob(
    const ORIGIN& origin, BLOB_HOLDER& dest
) {
  bufferlist buffer;
  encode(origin, buffer);
  dest.reserve(buffer.length());
  std::copy(
      buffer.c_str(), buffer.c_str() + buffer.length(), std::back_inserter(dest)
  );
}

template <typename SOURCE, typename DEST>
typename std::enable_if<
    !std::is_same<SOURCE, std::vector<char>>::value, void>::type
assign_value(const SOURCE& source, DEST& dest) {
  dest = source;
}

template <typename SOURCE, typename DEST>
typename std::enable_if<
    std::is_same<SOURCE, std::vector<char>>::value, void>::type
assign_value(const SOURCE& source, DEST& dest) {
  decode_blob(source, dest);
}

template <typename OPTIONAL, typename DEST>
void assign_optional_value(const OPTIONAL& optional_value, DEST& dest) {
  // if value is not set, do nothing
  if (!optional_value) return;
  assign_value(*optional_value, dest);
}

template <typename SOURCE, typename DEST>
void assign_db_value(const SOURCE& source, DEST& dest) {
  dest = source;
}

template <typename DEST>
void assign_db_value(const std::string& source, DEST& dest) {
  if (source.empty()) return;
  dest = source;
}

template <typename SOURCE>
void assign_db_value(
    const SOURCE& source, std::optional<std::vector<char>>& dest
) {
  std::vector<char> blob_vector;
  encode_blob(source, blob_vector);
  dest = blob_vector;
}

template <typename SOURCE>
void assign_db_value(const SOURCE& source, std::vector<char>& dest) {
  std::vector<char> blob_vector;
  encode_blob(source, blob_vector);
  dest = blob_vector;
}

std::string prefix_to_escaped_like(const std::string& prefix, char escape);

template <typename COL>
sqlite_orm::internal::like_t<COL, std::basic_string<char>, const char*>
prefix_to_like(COL col, const std::string& prefix) {
  return sqlite_orm::like(col, prefix_to_escaped_like(prefix, '\a'), "\a");
}

}  // namespace rgw::sal::sfs::sqlite
