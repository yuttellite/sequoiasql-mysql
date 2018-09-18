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

#ifndef SDB_CONN_PTR__H
#define SDB_CONN_PTR__H

#include <atomic_class.h>

class Sdb_conn;
class Sdb_conn_auto_ptr;

class Sdb_conn_ref_ptr {
 public:
  friend class Sdb_conn_auto_ptr;

 protected:
  Sdb_conn_ref_ptr(Sdb_conn *connection);

  virtual ~Sdb_conn_ref_ptr();

 protected:
  Sdb_conn *sdb_connection;
  Atomic_int32 ref;
};

class Sdb_conn_auto_ptr {
 public:
  Sdb_conn_auto_ptr();

  virtual ~Sdb_conn_auto_ptr();

  Sdb_conn_auto_ptr(Sdb_conn *connection);

  Sdb_conn_auto_ptr(const Sdb_conn_auto_ptr &other);

  Sdb_conn_auto_ptr &operator=(Sdb_conn_auto_ptr &other);

  Sdb_conn &operator*();

  Sdb_conn *operator->();

  int ref();

  void clear();

 private:
  Sdb_conn_ref_ptr *ref_ptr;
};

#endif
