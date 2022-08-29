// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <errno.h>
#include <stdlib.h>
#include <system_error>
#include <unistd.h>
#include <sstream>

#include "common/errno.h"
#include "common/environment.h"

#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "driver/rados/config/store.h"
#include "driver/json_config/store.h"
#include "rgw_d3n_datacache.h"

#ifdef WITH_RADOSGW_SFS
#include "rgw_sal_sfs.h"
#endif // WITH_RADOSGW_SFS

#ifdef WITH_RADOSGW_DBSTORE
#include "rgw_sal_dbstore.h"
#include "driver/dbstore/config/store.h"
#endif

#ifdef WITH_RADOSGW_MOTR
#include "rgw_sal_motr.h"
#endif

#ifdef WITH_RADOSGW_DAOS
#include "rgw_sal_daos.h"
#endif

#define dout_subsys ceph_subsys_rgw

extern "C" {
extern rgw::sal::Driver* newRadosStore(void);
#ifdef WITH_RADOSGW_SFS
extern rgw::sal::Driver* newSFStore(CephContext *cct);
#endif // WITH_RADOS_SFS
#ifdef WITH_RADOSGW_DBSTORE
extern rgw::sal::Driver* newDBStore(CephContext *cct);
#endif
#ifdef WITH_RADOSGW_MOTR
extern rgw::sal::Driver* newMotrStore(CephContext *cct);
#endif
#ifdef WITH_RADOSGW_DAOS
extern rgw::sal::Driver* newDaosStore(CephContext *cct);
#endif
extern rgw::sal::Driver* newBaseFilter(rgw::sal::Driver* next);

}

RGWObjState::RGWObjState() {
}

RGWObjState::~RGWObjState() {
}

RGWObjState::RGWObjState(const RGWObjState& rhs) : obj (rhs.obj) {
  is_atomic = rhs.is_atomic;
  has_attrs = rhs.has_attrs;
  exists = rhs.exists;
  size = rhs.size;
  accounted_size = rhs.accounted_size;
  mtime = rhs.mtime;
  epoch = rhs.epoch;
  if (rhs.obj_tag.length()) {
    obj_tag = rhs.obj_tag;
  }
  if (rhs.tail_tag.length()) {
    tail_tag = rhs.tail_tag;
  }
  write_tag = rhs.write_tag;
  fake_tag = rhs.fake_tag;
  shadow_obj = rhs.shadow_obj;
  has_data = rhs.has_data;
  if (rhs.data.length()) {
    data = rhs.data;
  }
  prefetch_data = rhs.prefetch_data;
  keep_tail = rhs.keep_tail;
  is_olh = rhs.is_olh;
  objv_tracker = rhs.objv_tracker;
  pg_ver = rhs.pg_ver;
  compressed = rhs.compressed;
}

rgw::sal::Driver* DriverManager::init_storage_provider(const DoutPrefixProvider* dpp,
						     CephContext* cct,
						     const Config& cfg,
						     bool use_gc_thread,
						     bool use_lc_thread,
						     bool quota_threads,
						     bool run_sync_thread,
						     bool run_reshard_thread,
						     bool use_cache,
						     bool use_gc)
{
  rgw::sal::Driver* driver{nullptr};

  if (cfg.store_name.compare("rados") == 0) {
    driver = newRadosStore();
    RGWRados* rados = static_cast<rgw::sal::RadosStore* >(driver)->getRados();

    if ((*rados).set_use_cache(use_cache)
                .set_use_datacache(false)
                .set_use_gc(use_gc)
                .set_run_gc_thread(use_gc_thread)
                .set_run_lc_thread(use_lc_thread)
                .set_run_quota_threads(quota_threads)
                .set_run_sync_thread(run_sync_thread)
                .set_run_reshard_thread(run_reshard_thread)
                .init_begin(cct, dpp) < 0) {
      delete driver;
      return nullptr;
    }
    if (driver->initialize(cct, dpp) < 0) {
      delete driver;
      return nullptr;
    }
    if (rados->init_complete(dpp) < 0) {
      delete driver;
      return nullptr;
    }
  }
  else if (cfg.store_name.compare("d3n") == 0) {
    driver = new rgw::sal::RadosStore();
    RGWRados* rados = new D3nRGWDataCache<RGWRados>;
    dynamic_cast<rgw::sal::RadosStore*>(driver)->setRados(rados);
    rados->set_store(static_cast<rgw::sal::RadosStore* >(driver));

    if ((*rados).set_use_cache(use_cache)
                .set_use_datacache(true)
                .set_run_gc_thread(use_gc_thread)
                .set_run_lc_thread(use_lc_thread)
                .set_run_quota_threads(quota_threads)
                .set_run_sync_thread(run_sync_thread)
                .set_run_reshard_thread(run_reshard_thread)
                .init_begin(cct, dpp) < 0) {
      delete driver;
      return nullptr;
    }
    if (driver->initialize(cct, dpp) < 0) {
      delete driver;
      return nullptr;
    }
    if (rados->init_complete(dpp) < 0) {
      delete driver;
      return nullptr;
    }

    lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_local_datacache_enabled=" <<
      cct->_conf->rgw_d3n_l1_local_datacache_enabled << dendl;
    lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_datacache_persistent_path='" <<
      cct->_conf->rgw_d3n_l1_datacache_persistent_path << "'" << dendl;
    lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_datacache_size=" <<
      cct->_conf->rgw_d3n_l1_datacache_size << dendl;
    lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_evict_cache_on_start=" <<
      cct->_conf->rgw_d3n_l1_evict_cache_on_start << dendl;
    lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_fadvise=" <<
      cct->_conf->rgw_d3n_l1_fadvise << dendl;
    lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_eviction_policy=" <<
      cct->_conf->rgw_d3n_l1_eviction_policy << dendl;
  }

#ifdef WITH_RADOSGW_SFS
  if (cfg.store_name.compare("sfs") == 0) {
    const auto& data_path =
      g_conf().get_val<std::string>("rgw_sfs_data_path");
    ldpp_dout(dpp, 0) << "sfs init!" << dendl;
    rgw::sal::SFStore *store =
      new rgw::sal::SFStore(cct, data_path);
    const char *id = get_env_char(
      "RGW_DEFAULT_USER_ID",
      "testid");
    const char *display_name = get_env_char(
      "RGW_DEFAULT_USER_DISPLAY_NAME",
      "M. Tester");
    const char *email = get_env_char(
      "RGW_DEFAULT_USER_EMAIL",
      "tester@ceph.com");
    const char *access_key = get_env_char(
      "RGW_DEFAULT_USER_ACCESS_KEY",
      "test");
    const char *secret_key = get_env_char(
      "RGW_DEFAULT_USER_SECRET_KEY",
      "test");
    const char *caps = get_env_char(
      "RGW_DEFAULT_USER_CAPS");
    const int system = get_env_int(
      "RGW_DEFAULT_USER_SYSTEM"); // Defaults to 0.
    const char *assumed_role_arn = get_env_char(
      "RGW_DEFAULT_USER_ASSUMED_ROLE_ARN");

    /* XXX: temporary - create testid user */
    rgw_user testid_user("", id, "");
    std::unique_ptr<rgw::sal::User> user = store->get_user(testid_user);

    if ( user->load_user(dpp, null_yield) == 0 ) {
      /* if we're able to load a user with the defailt user id from the backing
       * store, the store is already initialized and we don't need to create
       * this user.
       */
      ldpp_dout(dpp, 0) << "Found default user \"" << id
                        << "\" in database." << dendl;
    } else {
      user->get_info().display_name = display_name;
      user->get_info().user_email = email;
      RGWAccessKey k1(access_key, secret_key);
      user->get_info().access_keys[access_key] = k1;
      user->get_info().max_buckets = RGW_DEFAULT_MAX_BUCKETS;
      user->get_info().system = system;
      user->get_info().admin = 1;   // TODO remove when ACL is implemented
      user->get_info().type = TYPE_RGW;
      if (assumed_role_arn != nullptr) {
          user->get_info().assumed_role_arn = assumed_role_arn;
      }
      if (caps != nullptr) {
          RGWUserCaps rgw_caps;
          rgw_caps.add_from_string(caps);
          user->get_info().caps = rgw_caps;
      }

      int r = user->store_user(dpp, null_yield, true);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed inserting " << id
                          << " user in sfs error r=" << r << dendl;
      }
    }
    return store;
  }
#endif // WITH_RADOSGW_SFS

#ifdef WITH_RADOSGW_DBSTORE
  else if (cfg.store_name.compare("dbstore") == 0) {
    driver = newDBStore(cct);

    if ((*(rgw::sal::DBStore*)driver).set_run_lc_thread(use_lc_thread)
                                    .initialize(cct, dpp) < 0) {
      delete driver;
      return nullptr;
    }
  }
#endif

#ifdef WITH_RADOSGW_MOTR
  else if (cfg.store_name.compare("motr") == 0) {
    driver = newMotrStore(cct);
    if (driver == nullptr) {
      ldpp_dout(dpp, 0) << "newMotrStore() failed!" << dendl;
      return driver;
    }
    ((rgw::sal::MotrStore *)driver)->init_metadata_cache(dpp, cct);

    return store;
  }
#endif

#ifdef WITH_RADOSGW_DAOS
  else if (cfg.store_name.compare("daos") == 0) {
    driver = newDaosStore(cct);
    if (driver == nullptr) {
      ldpp_dout(dpp, 0) << "newDaosStore() failed!" << dendl;
      return driver;
    }
    int ret = driver->initialize(cct, dpp);
    if (ret != 0) {
      ldpp_dout(dpp, 20) << "ERROR: store->initialize() failed: " << ret << dendl;
      delete driver;
      return nullptr;
    }
  }
#endif

  if (cfg.filter_name.compare("base") == 0) {
    rgw::sal::Driver* next = driver;
    driver = newBaseFilter(next);

    if (driver->initialize(cct, dpp) < 0) {
      delete driver;
      delete next;
      return nullptr;
    }
  }

  return driver;
}

rgw::sal::Driver* DriverManager::init_raw_storage_provider(const DoutPrefixProvider* dpp, CephContext* cct, const Config& cfg)
{
  rgw::sal::Driver* driver = nullptr;
  if (cfg.store_name.compare("rados") == 0) {
    driver = newRadosStore();
    RGWRados* rados = static_cast<rgw::sal::RadosStore* >(driver)->getRados();

    rados->set_context(cct);

    int ret = rados->init_svc(true, dpp);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
      delete driver;
      return nullptr;
    }

    if (rados->init_rados() < 0) {
      delete driver;
      return nullptr;
    }
    if (driver->initialize(cct, dpp) < 0) {
      delete driver;
      return nullptr;
    }
  } else if (cfg.store_name.compare("sfs") == 0) {
    driver = newSFStore(cct);
  } else if (cfg.store_name.compare("dbstore") == 0) {
#ifdef WITH_RADOSGW_DBSTORE
    driver = newDBStore(cct);

    if ((*(rgw::sal::DBStore*)driver).initialize(cct, dpp) < 0) {
      delete driver;
      return nullptr;
    }
#else
    driver = nullptr;
#endif
  } else if (cfg.store_name.compare("motr") == 0) {
#ifdef WITH_RADOSGW_MOTR
    driver = newMotrStore(cct);
#else
    driver = nullptr;
#endif
  } else if (cfg.store_name.compare("daos") == 0) {
#ifdef WITH_RADOSGW_DAOS
    driver = newDaosStore(cct);

    if (driver->initialize(cct, dpp) < 0) {
      delete driver;
      return nullptr;
    }
#else
    driver = nullptr;
#endif
  }

  if (cfg.filter_name.compare("base") == 0) {
    rgw::sal::Driver* next = driver;
    driver = newBaseFilter(next);

    if (driver->initialize(cct, dpp) < 0) {
      delete driver;
      delete next;
      return nullptr;
    }
  }

  return driver;
}

void DriverManager::close_storage(rgw::sal::Driver* driver)
{
  if (!driver)
    return;

  driver->finalize();

  delete driver;
}

DriverManager::Config DriverManager::get_config(bool admin, CephContext* cct)
{
  DriverManager::Config cfg;

  // Get the store backend
  const auto& config_store = g_conf().get_val<std::string>("rgw_backend_store");
  if (config_store == "rados") {
    cfg.store_name = "rados";

    /* Check to see if d3n is configured, but only for non-admin */
    const auto& d3n = g_conf().get_val<bool>("rgw_d3n_l1_local_datacache_enabled");
    if (!admin && d3n) {
      if (g_conf().get_val<Option::size_t>("rgw_max_chunk_size") !=
	  g_conf().get_val<Option::size_t>("rgw_obj_stripe_size")) {
	lsubdout(cct, rgw_datacache, 0) << "rgw_d3n:  WARNING: D3N DataCache disabling (D3N requires that the chunk_size equals stripe_size)" << dendl;
      } else if (!g_conf().get_val<bool>("rgw_beast_enable_async")) {
	lsubdout(cct, rgw_datacache, 0) << "rgw_d3n:  WARNING: D3N DataCache disabling (D3N requires yield context - rgw_beast_enable_async=true)" << dendl;
      } else {
	cfg.store_name = "d3n";
      }
    }
  }
#ifdef WITH_RADOSGW_DBSTORE
  else if (config_store == "dbstore") {
    cfg.store_name = "dbstore";
  }
#endif
#ifdef WITH_RADOSGW_SFS
  else if (config_store == "sfs") {
    cfg.store_name = "sfs";
  }
#endif
#ifdef WITH_RADOSGW_MOTR
  else if (config_store == "motr") {
    cfg.store_name = "motr";
  }
#endif
#ifdef WITH_RADOSGW_DAOS
  else if (config_store == "daos") {
    cfg.store_name = "daos";
  }
#endif

  // Get the filter
  cfg.filter_name = "none";
  const auto& config_filter = g_conf().get_val<std::string>("rgw_filter");
  if (config_filter == "base") {
    cfg.filter_name = "base";
  }

  return cfg;
}

auto DriverManager::create_config_store(const DoutPrefixProvider* dpp,
                                       std::string_view type)
  -> std::unique_ptr<rgw::sal::ConfigStore>
{
  try {
    if (type == "rados") {
      return rgw::rados::create_config_store(dpp);
#ifdef WITH_RADOSGW_DBSTORE
    } else if (type == "dbstore") {
      const auto uri = g_conf().get_val<std::string>("dbstore_config_uri");
      return rgw::dbstore::create_config_store(dpp, uri);
#endif
    } else if (type == "json") {
      auto filename = g_conf().get_val<std::string>("rgw_json_config");
      return rgw::sal::create_json_config_store(dpp, filename);
    } else {
      ldpp_dout(dpp, -1) << "ERROR: unrecognized config store type '"
          << type << "'" << dendl;
      return nullptr;
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << "ERROR: failed to initialize config store '"
        << type << "': " << e.what() << dendl;
  }
  return nullptr;
}

namespace rgw::sal {
int Object::range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end)
{
  if (ofs < 0) {
    ofs += obj_size;
    if (ofs < 0)
      ofs = 0;
    end = obj_size - 1;
  } else if (end < 0) {
    end = obj_size - 1;
  }

  if (obj_size > 0) {
    if (ofs >= (off_t)obj_size) {
      return -ERANGE;
    }
    if (end >= (off_t)obj_size) {
      end = obj_size - 1;
    }
  }
  return 0;
}
} // namespace rgw::sal
