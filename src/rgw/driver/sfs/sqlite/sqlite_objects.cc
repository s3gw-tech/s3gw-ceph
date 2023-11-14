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
#include "sqlite_objects.h"

#include "dbapi.h"
#include "sqlite_query_utils.h"
#include "sqlite_versioned_objects.h"

namespace rgw::sal::sfs::sqlite {

SQLiteObjects::SQLiteObjects(DBConnRef _conn) : conn(_conn) {}

std::vector<DBObject> SQLiteObjects::get_objects(const std::string& bucket_id
) const {
  return GetSQLiteObjectsWhere<DBObject>(
      conn->get(), "objects", "bucket_id", bucket_id
  );
}

std::optional<DBObject> SQLiteObjects::get_object(const uuid_d& uuid) const {
  return GetSQLiteSingleObject<DBObject>(
      conn->get(), "objects", "uuid", uuid.to_string()
  );
}

std::optional<DBObject> SQLiteObjects::get_object(
    const std::string& bucket_id, const std::string& object_name
) const {
  auto rows =
      conn->get()
      << R"sql(SELECT * FROM objects WHERE bucket_id = ? AND name = ?;)sql"
      << bucket_id << object_name;
  std::optional<DBObject> ret_object;
  for (auto&& row : rows) {
    ret_object = DBObject(row);
    break;  // looking for a single object, it should return 0 or 1 entries.
            // TODO Return an error in there are more than 1 entry?
  }
  return ret_object;
}

void SQLiteObjects::store_object(const DBObject& object) const {
  dbapi::sqlite::database db = conn->get();
  db << R"sql(
    REPLACE INTO objects ( uuid, bucket_id, name )
    VALUES (?, ?, ?);)sql"
     << object.uuid << object.bucket_id << object.name;
}

void SQLiteObjects::remove_object(const uuid_d& uuid) const {
  dbapi::sqlite::database db = conn->get();
  db << R"sql(
    DELETE FROM objects
    WHERE uuid = ?;)sql"
     << uuid;
}

}  // namespace rgw::sal::sfs::sqlite
