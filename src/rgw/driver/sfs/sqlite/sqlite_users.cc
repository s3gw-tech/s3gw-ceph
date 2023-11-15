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
#include "sqlite_users.h"

#include <filesystem>
#include <iostream>

#include "dbapi.h"
#include "sqlite_query_utils.h"

namespace rgw::sal::sfs::sqlite {

SQLiteUsers::SQLiteUsers(DBConnRef _conn) : conn(_conn) {}

std::optional<DBOPUserInfo> SQLiteUsers::get_user(const std::string& userid
) const {
  return GetSQLiteSingleObject<DBOPUserInfo>(
      conn->get(), "users", "user_id", userid
  );
}

std::optional<DBOPUserInfo> SQLiteUsers::get_user_by_email(
    const std::string& email
) const {
  return GetSQLiteSingleObject<DBOPUserInfo>(
      conn->get(), "users", "user_email", email
  );
}

std::optional<DBOPUserInfo> SQLiteUsers::get_user_by_access_key(
    const std::string& key
) const {
  auto user_id = _get_user_id_by_access_key(key);
  return GetSQLiteSingleObject<DBOPUserInfo>(
      conn->get(), "users", "user_id", user_id
  );
}

std::vector<std::string> SQLiteUsers::get_user_ids() const {
  dbapi::sqlite::database db = conn->get();
  auto rows = db << R"sql(SELECT user_id FROM users;)sql";
  std::vector<std::string> ret;
  for (std::tuple<std::string> row : rows) {
    ret.emplace_back(std::get<0>(row));
  }
  return ret;
}

void SQLiteUsers::store_user(const DBOPUserInfo& user) const {
  dbapi::sqlite::database db = conn->get();
  db << R"sql(
    REPLACE INTO users ( user_id, tenant, ns, display_name, user_email,
                         access_keys, swift_keys, sub_users, suspended,
                         max_buckets, op_mask, user_caps, admin, system,
                         placement_name, placement_storage_class,
                         placement_tags, bucket_quota, temp_url_keys,
                         user_quota, type, mfa_ids, assumed_role_arn,
                         user_attrs, user_version, user_version_tag )
    VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
               ?, ?, ?, ?, ?, ?, ?, ? );)sql"
     << user.uinfo.user_id.id << user.uinfo.user_id.tenant
     << user.uinfo.user_id.ns << user.uinfo.display_name
     << user.uinfo.user_email << user.uinfo.access_keys << user.uinfo.swift_keys
     << user.uinfo.subusers << user.uinfo.suspended << user.uinfo.max_buckets
     << user.uinfo.op_mask << user.uinfo.caps << user.uinfo.admin
     << user.uinfo.system << user.uinfo.default_placement.name
     << user.uinfo.default_placement.storage_class << user.uinfo.placement_tags
     << user.uinfo.quota.bucket_quota << user.uinfo.temp_url_keys
     << user.uinfo.quota.user_quota << user.uinfo.type << user.uinfo.mfa_ids
     << nullptr << user.user_attrs << user.user_version.ver
     << user.user_version.tag;
  _store_access_keys(user);
}

void SQLiteUsers::remove_user(const std::string& userid) const {
  dbapi::sqlite::database db = conn->get();
  _remove_access_keys(userid);
  db << R"sql(
    DELETE FROM users
    WHERE user_id = ?;)sql"
     << userid;
}

void SQLiteUsers::_store_access_keys(const DBOPUserInfo& user) const {
  // remove existing keys for the user (in case any of them had changed)
  _remove_access_keys(user.uinfo.user_id.id);
  dbapi::sqlite::database db = conn->get();
  for (auto const& key : user.uinfo.access_keys) {
    db << R"sql(INSERT INTO access_keys (access_key, user_id) VALUES (?,?);)sql"
       << key.first << user.uinfo.user_id.id;
  }
}

void SQLiteUsers::_remove_access_keys(const std::string& userid) const {
  dbapi::sqlite::database db = conn->get();
  db << R"sql(
    DELETE FROM access_keys
    WHERE user_id = ?;)sql"
     << userid;
}

std::optional<std::string> SQLiteUsers::_get_user_id_by_access_key(
    const std::string& key
) const {
  std::optional<std::string> ret;
  dbapi::sqlite::database db = conn->get();
  auto rows = db << R"sql(
    SELECT user_id FROM access_keys
    WHERE access_key = ?;)sql"
                 << key;
  for (std::tuple<std::string> row : rows) {
    ret = std::get<0>(row);
    // in case we have 2 keys that are equal in different users we return
    // the first one.
    // TODO Consider this an error?
    break;
  }
  return ret;
}

}  // namespace rgw::sal::sfs::sqlite
