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

#include <optional>
#include <string>
#include <vector>

// #include "../conversion_utils.h"
#include "rgw/driver/sfs/sqlite/bindings/blob.h"
#include "rgw/driver/sfs/sqlite/dbapi.h"
#include "rgw_common.h"

namespace rgw::sal::sfs::sqlite {

// user to be mapped in DB
// Optional values mean they might have (or not) a value defined.
// Blobs are stored as their type and converted using encode/decode functions
struct DBUser {
  std::string user_id;
  std::optional<std::string> tenant;
  std::optional<std::string> ns;
  std::optional<std::string> display_name;
  std::optional<std::string> user_email;
  std::optional<std::map<std::string, RGWAccessKey>> access_keys;
  std::optional<std::map<std::string, RGWAccessKey>> swift_keys;
  std::optional<std::map<std::string, RGWSubUser>> sub_users;
  std::optional<unsigned char> suspended;
  std::optional<int> max_buckets;
  std::optional<int> op_mask;
  std::optional<RGWUserCaps> user_caps;
  std::optional<int> admin;
  std::optional<int> system;
  std::optional<std::string> placement_name;
  std::optional<std::string> placement_storage_class;
  std::optional<std::list<std::string>> placement_tags;
  std::optional<RGWQuotaInfo> bucket_quota;
  std::optional<std::map<int, std::string>> temp_url_keys;
  std::optional<RGWQuotaInfo> user_quota;
  std::optional<int> type;
  std::optional<std::set<std::string>> mfa_ids;
  std::optional<std::string> assumed_role_arn;
  std::optional<rgw::sal::Attrs> user_attrs;
  std::optional<int> user_version;
  std::optional<std::string> user_version_tag;
};

// This is a helper for queries in which we want to retrieve all the columns in
// the table.
// Could be used with the GetSQLiteSingleObject or GetSQLiteObjects helpers
using DBUserQueryResult = std::tuple<
    decltype(DBUser::user_id), decltype(DBUser::tenant), decltype(DBUser::ns),
    decltype(DBUser::display_name), decltype(DBUser::user_email),
    decltype(DBUser::access_keys), decltype(DBUser::swift_keys),
    decltype(DBUser::sub_users), decltype(DBUser::suspended),
    decltype(DBUser::max_buckets), decltype(DBUser::op_mask),
    decltype(DBUser::user_caps), decltype(DBUser::admin),
    decltype(DBUser::system), decltype(DBUser::placement_name),
    decltype(DBUser::placement_storage_class), decltype(DBUser::placement_tags),
    decltype(DBUser::bucket_quota), decltype(DBUser::temp_url_keys),
    decltype(DBUser::user_quota), decltype(DBUser::type),
    decltype(DBUser::mfa_ids), decltype(DBUser::assumed_role_arn),
    decltype(DBUser::user_attrs), decltype(DBUser::user_version),
    decltype(DBUser::user_version_tag)>;

// access keys are stored in a different table because a user could have more
// than one key and we need to be able to query by all of them.
// Keys are stored as a blob in the user, so this table is only used for the
// purpose of getting the user id based on the access key.
struct DBAccessKey {
  int id;
  std::string access_key;
  std::string user_id;
};

// Struct with information needed by SAL layer
// Because sqlite doesn't like nested members like, for instance, uinfo.user_id.id
// we need to create this structure that will be returned to the user using the SQLiteUsers API.
// The structure stored and retrieved from the database will be DBUser and the one
// received and returned by the SQLiteUsers API will be DBOPUserInfo.
// SQLiteUsers does the needed conversions.
struct DBOPUserInfo {
  DBOPUserInfo() = default;

  // sqlite_modern_cpp returns rows as a tuple.
  // This is a helper constructor for the case in which we want to get all the
  // columns and return a full object.
  explicit DBOPUserInfo(DBUserQueryResult&& values) {
    uinfo.user_id.id = std::move(std::get<0>(values));
    assign_optional_value(std::get<1>(values), uinfo.user_id.tenant);
    assign_optional_value(std::get<2>(values), uinfo.user_id.ns);
    assign_optional_value(std::get<3>(values), uinfo.display_name);
    assign_optional_value(std::get<4>(values), uinfo.user_email);
    assign_optional_value(std::get<5>(values), uinfo.access_keys);
    assign_optional_value(std::get<6>(values), uinfo.swift_keys);
    assign_optional_value(std::get<7>(values), uinfo.subusers);
    assign_optional_value(std::get<8>(values), uinfo.suspended);
    assign_optional_value(std::get<9>(values), uinfo.max_buckets);
    assign_optional_value(std::get<10>(values), uinfo.op_mask);
    assign_optional_value(std::get<11>(values), uinfo.caps);
    assign_optional_value(std::get<12>(values), uinfo.admin);
    assign_optional_value(std::get<13>(values), uinfo.system);
    assign_optional_value(std::get<14>(values), uinfo.default_placement.name);
    assign_optional_value(
        std::get<15>(values), uinfo.default_placement.storage_class
    );
    assign_optional_value(std::get<16>(values), uinfo.placement_tags);
    assign_optional_value(std::get<17>(values), uinfo.quota.bucket_quota);
    assign_optional_value(std::get<18>(values), uinfo.temp_url_keys);
    assign_optional_value(std::get<19>(values), uinfo.quota.user_quota);
    assign_optional_value(std::get<20>(values), uinfo.type);
    assign_optional_value(std::get<21>(values), uinfo.mfa_ids);
    // assumed_role_arn no longer exists
    assign_optional_value(std::get<23>(values), user_attrs);
    assign_optional_value(std::get<24>(values), user_version.ver);
    assign_optional_value(std::get<25>(values), user_version.tag);
  }

  DBOPUserInfo(
      const RGWUserInfo& _uinfo, const obj_version& _user_version,
      const rgw::sal::Attrs& _user_attrs
  )
      : uinfo(_uinfo), user_version(_user_version), user_attrs(_user_attrs) {}

  RGWUserInfo uinfo;
  obj_version user_version;
  rgw::sal::Attrs user_attrs;
};

}  // namespace rgw::sal::sfs::sqlite
