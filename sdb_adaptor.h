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

#ifndef SDB_ADAPTOR__H
#define SDB_ADAPTOR__H

#include <map>
#include <mysql/psi/mysql_thread.h>
#include <my_global.h>
#include <atomic_class.h>
#include "client.hpp"
#include "sdb_conn_ptr.h"
#include "sdb_cl_ptr.h"
#include "sdb_util.h"

class Sdb_adaptor {
 public:
  ~Sdb_adaptor();

  static Sdb_adaptor *get_instance();

  int get_sdb_conn(my_thread_id tid, Sdb_conn_auto_ptr &sdb_ptr);

  void del_sdb_conn(my_thread_id tid);

 private:
  Sdb_adaptor();

  Sdb_adaptor(const Sdb_adaptor &rh) {}

  Sdb_adaptor &operator=(const Sdb_adaptor &rh) { return *this; }

 private:
  int conn_max;
  int clear_num;
  Atomic_int32 conn_num;
  std::map<my_thread_id, Sdb_conn_auto_ptr> conn_list;
  pthread_rwlock_t rw_mutex;
};

#define SDB_CONN_MGR_INST Sdb_adaptor::get_instance()

#endif
