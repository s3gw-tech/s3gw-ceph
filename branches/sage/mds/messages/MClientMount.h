// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __MCLIENTMOUNT_H
#define __MCLIENTMOUNT_H

#include "msg/Message.h"

class MClientMount : public Message {
public:
  entity_addr_t addr;

  MClientMount() : Message(MSG_CLIENT_MOUNT) { }
  MClientMount(entity_addr_t a) : 
    Message(MSG_CLIENT_MOUNT),
    addr(a) { }

  char *get_type_name() { return "client_mount"; }

  void decode_payload() { 
    int off = 0;
    ::_decode(addr, payload, off);
  }
  void encode_payload() { 
    ::_encode(addr, payload);
  }
};

#endif
