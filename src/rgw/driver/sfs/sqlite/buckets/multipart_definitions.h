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

#include <string>

#include "rgw/driver/sfs/multipart_types.h"
#include "rgw/rgw_common.h"

namespace rgw::sal::sfs::sqlite {

using BLOB = std::vector<char>;

struct DBMultipartPart {
  int id;
  std::string upload_id;
  uint32_t part_num;
  uint64_t size;
  std::optional<std::string> etag;
  std::optional<ceph::real_time> mtime;

  inline bool is_finished() const { return etag.has_value(); }
};

struct DBMultipart {
  int id;
  std::string bucket_id;
  std::string upload_id;
  MultipartState state;
  ceph::real_time state_change_time;
  std::string object_name;
  uuid_d path_uuid;
  std::string meta_str;

  ACLOwner owner_id;
  ceph::real_time mtime;
  rgw::sal::Attrs attrs;
  rgw_placement_rule placement;
};

using DBDeletedMultipartItem = std::tuple<
    decltype(DBMultipart::upload_id), decltype(DBMultipart::path_uuid),
    decltype(DBMultipartPart::id)>;

using DBDeletedMultipartItems = std::vector<DBDeletedMultipartItem>;

/// DBDeletedMultipartItem helpers
inline decltype(DBMultipart::upload_id) get_upload_id(
    const DBDeletedMultipartItem& item
) {
  return std::get<0>(item);
}

inline decltype(DBMultipart::path_uuid) get_path_uuid(
    const DBDeletedMultipartItem& item
) {
  return std::get<1>(item);
}

inline decltype(DBMultipartPart::part_num) get_part_id(
    const DBDeletedMultipartItem& item
) {
  return std::get<2>(item);
}

}  // namespace rgw::sal::sfs::sqlite
