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

#ifndef SDB_CONN__H
#define SDB_CONN__H

#include <mysql/psi/mysql_thread.h>
#include <my_global.h>
#include "client.hpp"

class Sdb_cl;

class Sdb_conn {
 public:
  Sdb_conn(my_thread_id _tid);

  ~Sdb_conn();

  int connect();

  sdbclient::sdb &get_sdb();

  my_thread_id get_tid();

  int begin_transaction();

  int commit_transaction();

  int rollback_transaction();

  bool is_transaction();

  int get_cl(char *cs_name, char *cl_name, Sdb_cl &cl);

  int create_cl(char *cs_name, char *cl_name,
                const bson::BSONObj &options = sdbclient::_sdbStaticObject);

  int drop_cs(char *cs_name);

  int drop_cl(char *cs_name, char *cl_name);

  int create_global_domain(const char *domain_name);

  int create_global_domain_cs(const char *domain_name, char *cs_name);

  inline bool is_valid() { return connection.isValid(); }

 private:
  sdbclient::sdb connection;
  bool transactionon;
  my_thread_id tid;
};

#endif
