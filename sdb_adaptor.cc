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

#include "sdb_adaptor.h"
#include "sdb_err_code.h"
#include "sdb_util.h"
#include "sdb_conn.h"
#include "sdb_conn_ptr.h"

Sdb_adaptor::Sdb_adaptor() : conn_max(500), clear_num(10) {
  pthread_rwlock_init(&rw_mutex, NULL);
  conn_num = 0;
}

Sdb_adaptor::~Sdb_adaptor() {
  pthread_rwlock_destroy(&rw_mutex);
}

Sdb_adaptor *Sdb_adaptor::get_instance() {
  static Sdb_adaptor _sdb_adaptor;
  return &_sdb_adaptor;
}

int Sdb_adaptor::get_sdb_conn(my_thread_id tid, Sdb_conn_auto_ptr &sdb_ptr) {
  int rc = 0;
  std::map<my_thread_id, Sdb_conn_auto_ptr>::iterator iter;

  sdb_ptr.clear();
  {
    Sdb_rw_lock_r r_lock(&rw_mutex);
    iter = conn_list.find(tid);
    if (conn_list.end() != iter) {
      sdb_ptr = iter->second;
      goto done;
    }
  }

  {
    Sdb_conn_auto_ptr tmp_conn(new Sdb_conn(tid));
    sdb_ptr = tmp_conn;
    rc = sdb_ptr->connect();
    Sdb_rw_lock_w w_lock(&rw_mutex);
    conn_list.insert(std::pair<my_thread_id, Sdb_conn_auto_ptr>(tid, sdb_ptr));
    conn_num.atomic_add(1);
  }
done:
  return rc;
}

void Sdb_adaptor::del_sdb_conn(my_thread_id tid) {
  if (conn_num.atomic_get() <= conn_max) {
    goto done;
  }
  {
    /*sdb_rw_lock_w w_lock( &rw_mutex ) ;
    std::map<my_thread_id, Sdb_conn_auto_ptr>::iterator iter ;
    iter = conn_list.find( tid ) ;
    if ( conn_list.end() == iter
         || iter->second.ref() > 1
         || !iter->second->is_idle() )
    {
       goto done ;
    }
    conn_list.erase( iter );*/
    Sdb_rw_lock_w w_lock(&rw_mutex);
    int del_num = 0;
    std::map<my_thread_id, Sdb_conn_auto_ptr>::iterator iter;
    iter = conn_list.begin();
    while (iter != conn_list.end() && del_num <= clear_num) {
      if (iter->second.ref() > 1 || !iter->second->is_idle()) {
        ++iter;
        continue;
      }
      conn_list.erase(iter++);
    }
    conn_num = conn_list.size();
  }
done:
  return;
}
