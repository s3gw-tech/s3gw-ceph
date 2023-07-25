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
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "common/ceph_context.h"
#include "common/random_string.h"
#include "driver/sfs/sqlite/objects/object_definitions.h"
#include "driver/sfs/sqlite/versioned_object/versioned_object_definitions.h"
#include "rgw/driver/sfs/sqlite/sqlite_list.h"
#include "rgw/rgw_perf_counters.h"
#include "test/rgw/sfs/rgw_sfs_utils.h"

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;

class TestSFSList : public ::testing::Test {
 protected:
  const std::unique_ptr<CephContext> cct;
  const fs::path database_directory;

  std::unique_ptr<rgw::sal::SFStore> store;
  DBConnRef dbconn;

  TestSFSList()
      : cct(new CephContext(CEPH_ENTITY_TYPE_ANY)),
        database_directory(create_database_directory()) {
    cct->_conf.set_val("rgw_sfs_data_path", database_directory);
    cct->_conf.set_val("rgw_sfs_sqlite_profile", "1");
    cct->_log->start();
    rgw_perf_start(cct.get());
  }
  void SetUp() override {
    ASSERT_TRUE(fs::exists(database_directory)) << database_directory;
    dbconn = std::make_shared<DBConn>(cct.get());
    store.reset(new rgw::sal::SFStore(cct.get(), database_directory));

    SQLiteUsers users(dbconn);
    DBOPUserInfo user;
    user.uinfo.user_id.id = "testuser";
    user.uinfo.display_name = "display_name";
    users.store_user(user);

    SQLiteBuckets db_buckets(dbconn);
    DBOPBucketInfo db_binfo;
    db_binfo.binfo.bucket = rgw_bucket("", "testbucket", "testbucket");
    db_binfo.binfo.owner = rgw_user("testuser");
    db_binfo.binfo.creation_time = ceph::real_clock::now();
    db_binfo.binfo.placement_rule = rgw_placement_rule();
    db_binfo.binfo.zonegroup = "zone";
    db_binfo.deleted = false;
    db_buckets.store_bucket(db_binfo);
    RGWUserInfo bucket_owner;
  }
  void TearDown() override {
    if (::testing::Test::HasFailure()) {
      dump_db();
    }
    fs::remove_all(database_directory);
  }

  fs::path create_database_directory() const {
    const std::string rand = gen_rand_alphanumeric(cct.get(), 23);
    const auto result{fs::temp_directory_path() / rand};
    fs::create_directory(result);
    return result;
  }

  std::pair<DBObject, DBVersionedObject> add_obj_single_ver(
      const std::string& prefix = "", rgw::sal::sfs::ObjectState version_state =
                                          rgw::sal::sfs::ObjectState::COMMITTED
  ) const {
    std::string name(prefix);
    name.append(gen_rand_alphanumeric(cct.get(), 23));
    const auto obj = create_test_object("testbucket", name);
    SQLiteObjects os(dbconn);
    os.store_object(obj);
    auto ver = create_test_versionedobject(obj.uuid, "testversion");
    ver.object_state = version_state;
    SQLiteVersionedObjects vos(dbconn);
    vos.insert_versioned_object(ver);
    return std::make_pair(obj, ver);
  }

  void dump_db() {
    auto storage = dbconn->get_storage();
    lderr(cct.get()) << "Dumping objects:" << dendl;
    for (const auto& row : storage.get_all<DBObject>()) {
      lderr(cct.get()) << row << dendl;
    }
    lderr(cct.get()) << "Dumping versioned objects:" << dendl;
    for (const auto& row : storage.get_all<DBVersionedObject>()) {
      lderr(cct.get()) << row << dendl;
    }
  }

  SQLiteList make_uut() { return SQLiteList(dbconn); }
};

TEST_F(TestSFSList, empty__lists_nothing) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  ASSERT_TRUE(uut.objects("testbucket", "", "", 10, results));
  ASSERT_EQ(results.size(), 0);
}

TEST_F(TestSFSList, single_object__plain_list_returns_it) {
  // See also s3-test bucket_list_return_data
  const auto obj = add_obj_single_ver();
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  ASSERT_TRUE(uut.objects("testbucket", "", "", 100, results));
  EXPECT_EQ(results.size(), 1);
  const auto e = results[0];
  EXPECT_EQ(e.key.name, obj.first.name);
  EXPECT_EQ(e.meta.mtime, obj.second.mtime);
  EXPECT_EQ(e.meta.etag, obj.second.etag);
  EXPECT_EQ(e.meta.size, obj.second.size);
  EXPECT_EQ(e.meta.accounted_size, obj.second.size);
}

TEST_F(TestSFSList, never_returns_more_than_max) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();

  ASSERT_TRUE(uut.objects("testbucket", "", "", 2, results));
  ASSERT_EQ(results.size(), 2);
}

TEST_F(TestSFSList, result_is_sorted) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();

  ASSERT_TRUE(uut.objects("testbucket", "", "", 1000, results));

  std::vector<rgw_bucket_dir_entry> expected(results);
  std::sort(expected.begin(), expected.end(), [](const auto& a, const auto& b) {
    return a.key.name < b.key.name;
  });
  for (size_t i = 0; i < results.size(); i++) {
    EXPECT_EQ(results[i].key.name, expected[i].key.name);
  }
}

TEST_F(TestSFSList, prefix_search_returns_only_prefixed) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  add_obj_single_ver("aaa/");
  add_obj_single_ver("aaa/");
  add_obj_single_ver("aaa/");
  add_obj_single_ver("XXX/");
  add_obj_single_ver("XXX/");

  ASSERT_TRUE(uut.objects("testbucket", "aaa/", "", 1000, results));
  for (size_t i = 0; i < results.size(); i++) {
    EXPECT_TRUE(results[i].key.name.starts_with("aaa/"));
  }
}

TEST_F(TestSFSList, start_after_object_name) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  add_obj_single_ver("aaa");
  add_obj_single_ver("bbb");
  add_obj_single_ver("ccc");
  const auto after_this = add_obj_single_ver("ddd");
  add_obj_single_ver("eee");

  ASSERT_TRUE(
      uut.objects("testbucket", "", after_this.first.name, 1000, results)
  );
  ASSERT_EQ(results.size(), 1);
  EXPECT_TRUE(results[0].key.name.starts_with("eee"));
}

TEST_F(TestSFSList, more_avail__false_if_all) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  bool more_avail{true};
  add_obj_single_ver();
  add_obj_single_ver();

  ASSERT_TRUE(uut.objects("testbucket", "", "", 2, results, &more_avail));
  EXPECT_EQ(results.size(), 2);
  EXPECT_FALSE(more_avail);
}

TEST_F(TestSFSList, more_avail__true_if_more) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  bool more_avail{false};
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();

  ASSERT_TRUE(uut.objects("testbucket", "", "", 2, results, &more_avail));
  EXPECT_EQ(results.size(), 2);
  EXPECT_TRUE(more_avail);
}

TEST_F(TestSFSList, more_avail__max_zero_bucket_empty) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  bool more_avail{false};
  ASSERT_TRUE(uut.objects("testbucket", "", "", 0, results, &more_avail));
  EXPECT_EQ(results.size(), 0);
  EXPECT_FALSE(more_avail);
}

TEST_F(TestSFSList, more_avail__max_zero_bucket_not_empty) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  bool more_avail{true};
  add_obj_single_ver();
  ASSERT_TRUE(uut.objects("testbucket", "", "", 0, results, &more_avail));
  EXPECT_EQ(results.size(), 0);
  EXPECT_TRUE(more_avail);
}


