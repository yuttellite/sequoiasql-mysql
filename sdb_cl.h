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

#ifndef SDB_CL__H
#define SDB_CL__H

#include <mysql/psi/mysql_thread.h>
#include "client.hpp"
#include "sdb_def.h"
#include "sdb_conn.h"

class Sdb_cl {
 public:
  Sdb_cl();

  ~Sdb_cl();

  int init(Sdb_conn *connection, char *cs, char *cl);

  int re_init();

  int begin_transaction();

  int commit_transaction();

  int rollback_transaction();

  bool is_transaction();

  char *get_cs_name();

  char *get_cl_name();

  int query(const bson::BSONObj &condition = sdbclient::_sdbStaticObject,
            const bson::BSONObj &selected = sdbclient::_sdbStaticObject,
            const bson::BSONObj &orderBy = sdbclient::_sdbStaticObject,
            const bson::BSONObj &hint = sdbclient::_sdbStaticObject,
            INT64 numToSkip = 0, INT64 numToReturn = -1,
            INT32 flags = QUERY_WITH_RETURNDATA);

  int query_one(bson::BSONObj &obj,
                const bson::BSONObj &condition = sdbclient::_sdbStaticObject,
                const bson::BSONObj &selected = sdbclient::_sdbStaticObject,
                const bson::BSONObj &orderBy = sdbclient::_sdbStaticObject,
                const bson::BSONObj &hint = sdbclient::_sdbStaticObject,
                INT64 numToSkip = 0, INT32 flags = QUERY_WITH_RETURNDATA);

  int current(bson::BSONObj &obj);

  int next(bson::BSONObj &obj);

  int insert(bson::BSONObj &obj);

  int update(const bson::BSONObj &rule,
             const bson::BSONObj &condition = sdbclient::_sdbStaticObject,
             const bson::BSONObj &hint = sdbclient::_sdbStaticObject,
             INT32 flag = 0);

  int upsert(const bson::BSONObj &rule,
             const bson::BSONObj &condition = sdbclient::_sdbStaticObject,
             const bson::BSONObj &hint = sdbclient::_sdbStaticObject,
             const bson::BSONObj &setOnInsert = sdbclient::_sdbStaticObject,
             INT32 flag = 0);

  int del(const bson::BSONObj &condition = sdbclient::_sdbStaticObject,
          const bson::BSONObj &hint = sdbclient::_sdbStaticObject);

  int create_index(const bson::BSONObj &indexDef, const CHAR *pName,
                   BOOLEAN isUnique, BOOLEAN isEnforced);

  int drop_index(const char *pName);

  int truncate();

  void close();  // close cursor

  my_thread_id get_tid();

  int drop();

  int get_count(long long &count);

 private:
  int check_connect(int rc);

 private:
  Sdb_conn *p_conn;
  sdbclient::sdbCollection cl;
  sdbclient::sdbCursor cursor;
  char cs_name[SDB_CS_NAME_MAX_SIZE + 1];
  char cl_name[SDB_CL_NAME_MAX_SIZE + 1];
};
#endif
