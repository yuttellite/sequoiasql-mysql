/* Copyright (c) 2018, SequoiaDB and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <string.h>
#include "my_global.h"
#include "sdb_conf.h"

PSI_memory_key sdb_key_memory_conf_coord_addrs;

sdb_conf::sdb_conf() {
  use_partition = TRUE;
}

sdb_conf::~sdb_conf() {
  clear_coord_addrs(coord_num);
  coord_num = 0;
}

sdb_conf *sdb_conf::get_instance() {
  static sdb_conf sdb_conf_inst;
  return &sdb_conf_inst;
}

void sdb_conf::clear_coord_addrs(int num) {
  for (int i = 0; i < num; i++) {
    if (pAddrs[i]) {
      free(pAddrs[i]);
    }
  }
}

int sdb_conf::parse_conn_addrs(const char *conn_addr) {
  int rc = 0;
  const char *p = conn_addr;

  if (NULL == conn_addr || 0 == strlen(conn_addr)) {
    rc = -1;
    goto error;
  }

  // note: no lock here, the coord-addrs must set before used!
  // sdb_rw_lock_w lock( &addrs_mutex ) ;
  clear_coord_addrs(coord_num);
  coord_num = 0;

  while (*p != 0) {
    const char *p_tmp = NULL;
    size_t len = 0;
    if (coord_num >= SDB_COORD_NUM_MAX) {
      goto done;
    }

    p_tmp = strchr(p, ',');
    if (NULL == p_tmp) {
      len = strlen(p);
    } else {
      len = p_tmp - p;
    }
    if (len > 0) {
      char *p_addr = NULL;
      const char *comma_pos = strchr(p, ',');
      const char *colon_pos = strchr(p, ':');
      if (!colon_pos || (comma_pos && comma_pos < colon_pos)) {
        rc = -1;
        goto error;
      }
      p_addr = (char *)malloc(len + 1);
      if (NULL == p_addr) {
        rc = -1;
        goto error;
      }
      memcpy(p_addr, p, len);
      p_addr[len] = 0;
      pAddrs[coord_num] = p_addr;
      ++coord_num;
    }
    p += len;
    if (*p == ',') {
      p++;
    }
  }

done:
  return rc;
error:
  goto done;
}

char **sdb_conf::get_coord_addrs() {
  return pAddrs;
}

int sdb_conf::get_coord_num() {
  return coord_num;
}

void sdb_conf::set_use_partition(my_bool val) {
  use_partition = val;
}

my_bool sdb_conf::get_use_partition() {
  return use_partition;
}

void sdb_conf::set_debug_log(my_bool val) {
  debug_log = val;
}

my_bool sdb_conf::get_debug_log() {
  return debug_log;
}
