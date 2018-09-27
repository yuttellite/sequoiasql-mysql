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

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include "sdb_conn.h"
#include <sql_class.h>
#include <client.hpp>
#include "sdb_cl.h"
#include "sdb_conf.h"
#include "sdb_util.h"
#include "sdb_err_code.h"

Sdb_conn::Sdb_conn(my_thread_id _tid) : transactionon(false), tid(_tid) {}

Sdb_conn::~Sdb_conn() {}

sdbclient::sdb &Sdb_conn::get_sdb() {
  return connection;
}

my_thread_id Sdb_conn::get_tid() {
  return tid;
}

int Sdb_conn::connect() {
  int rc = SDB_ERR_OK;
  if (!connection.isValid()) {
    transactionon = false;
    Sdb_conn_addrs conn_addrs;
    int tmp_rc = conn_addrs.parse_conn_addrs(sdb_conn_str);
    DBUG_ASSERT(tmp_rc == 0);
    rc = connection.connect(conn_addrs.get_conn_addrs(),
                            conn_addrs.get_conn_num(), "", "");
    if (rc != SDB_ERR_OK) {
      goto error;
    }
  }

done:
  return rc;
error:
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::begin_transaction() {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
  while (!transactionon) {
    rc = connection.transactionBegin();
    if (SDB_ERR_OK == rc) {
      transactionon = true;
      break;
    } else if (IS_SDB_NET_ERR(rc) && --retry_times > 0) {
      connect();
    } else {
      goto error;
    }
  }

done:
  return rc;
error:
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::commit_transaction() {
  int rc = SDB_ERR_OK;
  if (transactionon) {
    transactionon = false;
    rc = connection.transactionCommit();
    if (rc != SDB_ERR_OK) {
      goto error;
    }
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    connect();
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::rollback_transaction() {
  if (transactionon) {
    int rc = SDB_ERR_OK;
    transactionon = false;
    rc = connection.transactionRollback();
    if (IS_SDB_NET_ERR(rc)) {
      connect();
    }
  }
  return 0;
}

bool Sdb_conn::is_transaction() {
  return transactionon;
}

int Sdb_conn::get_cl(char *cs_name, char *cl_name, Sdb_cl &cl) {
  int rc = SDB_ERR_OK;
  cl.close();

  rc = cl.init(this, cs_name, cl_name);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    connect();
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::create_cl(char *cs_name, char *cl_name,
                        const bson::BSONObj &options) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
  sdbclient::sdbCollectionSpace cs;
  sdbclient::sdbCollection cl;

retry:
  rc = connection.createCollectionSpace(cs_name, 4096, cs);
  if (SDB_DMS_CS_EXIST == rc) {
    rc = connection.getCollectionSpace(cs_name, cs);
  }
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  rc = cs.createCollection(cl_name, options, cl);
  if (SDB_DMS_EXIST == rc) {
    rc = cs.getCollection(cl_name, cl);
  }
  if (rc != SDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    if (!transactionon && retry_times-- > 0 && 0 == connect()) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::drop_cs(char *cs_name) {
  int rc = SDB_ERR_OK;
  rc = connection.dropCollectionSpace(cs_name);
  if (rc != SDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    connect();
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::drop_cl(char *cs_name, char *cl_name) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
  sdbclient::sdbCollectionSpace cs;

retry:
  rc = connection.getCollectionSpace(cs_name, cs);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  rc = cs.dropCollection(cl_name);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    connect();
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::create_global_domain(const char *domain_name) {
  int rc = SDB_ERR_OK;
  sdbclient::sdbDomain domain;
  sdbclient::sdbCursor cursor;
  bson::BSONObj obj_tmp;
  bson::BSONArrayBuilder rg_list;
  bson::BSONObjBuilder obj_builder;
  BOOLEAN has_rg = FALSE;

  rc = connection.getDomain(domain_name, domain);
  if (SDB_ERR_OK == rc) {
    goto done;
  }
  if (SDB_CAT_DOMAIN_NOT_EXIST != rc) {
    goto error;
  }

  rc = connection.listReplicaGroups(cursor);
  if (rc != SDB_ERR_OK) {
    goto error;
  }
  while (SDB_ERR_OK == (rc = cursor.next(obj_tmp))) {
    bson::BSONElement rg_name;
    bson::BSONElement rg_id;
    rg_id = obj_tmp.getField("GroupID");
    rg_name = obj_tmp.getField("GroupName");
    if (rg_id.isNumber() && rg_name.type() == bson::String &&
        rg_id.numberInt() >= 1000) {
      has_rg = TRUE;
      rg_list.append(rg_name.valuestr());
    }
  }

  if (!has_rg) {
    rc = SDB_SYS;
    goto error;
  }

  obj_builder.appendArray("Groups", rg_list.arr());
  obj_builder.append("AutoSplit", true);

  rc = connection.createDomain(domain_name, obj_builder.obj(), domain);
  if (SDB_CAT_DOMAIN_EXIST == rc) {
    rc = SDB_ERR_OK;
  }
  if (rc != SDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    connect();
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::create_global_domain_cs(const char *domain_name, char *cs_name) {
  int rc = SDB_ERR_OK;
  sdbclient::sdbCollectionSpace cs;
  bson::BSONObj options;

  rc = create_global_domain(domain_name);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  options = BSON("Domain" << domain_name);
  rc = connection.createCollectionSpace(cs_name, options, cs);
  if (SDB_DMS_CS_EXIST == rc) {
    rc = SDB_ERR_OK;
  }
  if (rc != SDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    connect();
  }
  convert_sdb_code(rc);
  goto done;
}
