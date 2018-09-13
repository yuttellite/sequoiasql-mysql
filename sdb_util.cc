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

#include "sql_class.h"
#include "sql_table.h"
#include "sdb_util.h"
#include "sdb_log.h"
#include "sdb_err_code.h"
#include "mysqld.h"
#include <string.h>

int sdb_parse_table_name(const char *from, char *db_name, int db_name_size,
                         char *table_name, int table_name_size) {
  int rc = 0, len = 0;
  const char *pBegin, *pEnd;
  pBegin = from + 2;  // skip "./"
  pEnd = strchr(pBegin, '/');
  if (NULL == pEnd) {
    rc = -1;
    goto error;
  }
  len = pEnd - pBegin;
  if (len >= db_name_size) {
    rc = -1;
    goto error;
  }
  memcpy(db_name, pBegin, len);
  db_name[len] = 0;
  my_casedn_str(system_charset_info, db_name);

  pBegin = pEnd + 1;
  pEnd = strchrnul(pBegin, '/');
  len = pEnd - pBegin;
  if (*pEnd != 0 || len >= table_name_size) {
    rc = -1;
    goto error;
  }
  memcpy(table_name, pBegin, len);
  table_name[len] = 0;
  my_casedn_str(system_charset_info, table_name);

done:
  return rc;
error:
  goto done;
}

int sdb_get_db_name_from_path(const char *path, char *db_name,
                              int db_name_size) {
  int rc = SDB_ERR_OK, len = 0;
  const char *pBegin = NULL, *pEnd = NULL;
  if (NULL == path) {
    rc = SDB_ERR_INVALID_ARG;
    goto error;
  }

  pBegin = path + 2;  // skip "./"
  pEnd = strrchr(pBegin, '/');
  if (pEnd <= pBegin) {
    rc = SDB_ERR_INVALID_ARG;
    goto error;
  }

  len = pEnd - pBegin;
  if (len >= db_name_size) {
    rc = SDB_ERR_INVALID_ARG;
    goto error;
  }
  memcpy(db_name, pBegin, len);
  db_name[len] = 0;
  my_casedn_str(system_charset_info, db_name);

done:
  return rc;
error:
  goto done;
}

int sdb_convert_charset(const String &src_str, String &dst_str,
                        const CHARSET_INFO *dst_charset) {
  int rc = SDB_ERR_OK;
  uint conv_errors = 0;
  if (dst_str.copy(src_str.ptr(), src_str.length(), src_str.charset(),
                   dst_charset, &conv_errors)) {
    rc = SDB_ERR_OOM;
    goto error;
  }
  if (conv_errors) {
    SDB_LOG_DEBUG("String[%s] cannot be converted from %s to %s.",
                  src_str.ptr(), src_str.charset()->csname,
                  dst_charset->csname);
    rc = HA_ERR_UNKNOWN_CHARSET;
    goto error;
  }
done:
  return rc;
error:
  goto done;
}
