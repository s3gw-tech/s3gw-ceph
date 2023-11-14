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

#include <string>
#include <vector>

#include "common/Formatter.h"
#include "rgw/driver/sfs/sqlite/dbapi.h"

namespace rgw::sal::sfs::sqlite {

// Helper to return a vector with all the objects in a given table
// This needs that Target has a constructor with a tuple listing all the
// columns.
// TODO See if we can do this in a more elegant way without investing too much
// time.
template <typename Target>
inline std::vector<Target> GetSQLiteObjects(
    dbapi::sqlite::database db, const std::string& table_name
) {
  auto rows = db << fmt::format("SELECT * FROM {};", table_name);
  std::vector<Target> ret;
  for (auto&& row : rows) {
    ret.emplace_back(Target(row));
  }
  return ret;
}

// Helper to get a vector with all the objects in a given table with a single
// condition.
template <typename Target, typename ColumWhereType>
inline std::vector<Target> GetSQLiteObjectsWhere(
    dbapi::sqlite::database db, const std::string& table_name,
    const std::string& column_name, const ColumWhereType& column_value
) {
  auto rows =
      db << fmt::format(
                "SELECT * FROM {} WHERE {} = ?;", table_name, column_name
            )
         << column_value;
  std::vector<Target> ret;
  for (auto&& row : rows) {
    ret.emplace_back(Target(row));
  }
  return ret;
}

// Helper to get a single object from a table
template <typename Target, typename KeyType>
inline std::optional<Target> GetSQLiteSingleObject(
    dbapi::sqlite::database db, const std::string& table_name,
    const std::string& key_name, const KeyType& key_value
) {
  auto rows =
      db << fmt::format("SELECT * FROM {} WHERE {} = ?;", table_name, key_name)
         << key_value;
  std::optional<Target> ret;
  for (auto&& row : rows) {
    ret = Target(row);
    break;  // looking for a single object, it should return 0 or 1 entries.
            // TODO Return an error in there are more than 1 entry?
  }
  return ret;
}
}  // namespace rgw::sal::sfs::sqlite
