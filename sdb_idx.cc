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

#include "sdb_idx.h"
#include <myisampack.h>
#include <bson/bson.hpp>
#include "sdb_cl.h"
#include "sdb_errcode.h"
#include "sdb_def.h"
#include "sdb_log.h"
#include "sdb_util.h"
#include "sql_table.h"

static inline int get_variable_key_length(const uchar *A) {
  return (int)(((uint16)(A[0])) + ((uint16)(A[1]) << 8));
}

int sdb_get_key_direction(ha_rkey_function find_flag) {
  switch (find_flag) {
    case HA_READ_KEY_EXACT:
    case HA_READ_KEY_OR_NEXT:
    case HA_READ_AFTER_KEY:
      return 1;
    case HA_READ_BEFORE_KEY:
    case HA_READ_KEY_OR_PREV:
    case HA_READ_PREFIX_LAST:
    case HA_READ_PREFIX_LAST_OR_PREV:
      return -1;
    case HA_READ_PREFIX:
    default:
      return 1;
  }
}

static BOOLEAN is_field_indexable(const Field *field) {
  switch (field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_DATETIME:
      return TRUE;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB: {
      if (!field->binary())
        return TRUE;
      else
        return FALSE;
    }
    default:
      return FALSE;
  }
}

int sdb_create_index(const KEY *keyInfo, Sdb_cl &cl) {
  const KEY_PART_INFO *keyPart;
  const KEY_PART_INFO *keyEnd;
  int rc = 0;
  bson::BSONObj keyObj;
  BOOLEAN isUnique = FALSE, isEnforced = FALSE;

  bson::BSONObjBuilder keyObjBuilder;
  keyPart = keyInfo->key_part;
  keyEnd = keyPart + keyInfo->user_defined_key_parts;
  for (; keyPart != keyEnd; ++keyPart) {
    if (!is_field_indexable(keyPart->field)) {
      rc = HA_ERR_UNSUPPORTED;
      SDB_PRINT_ERROR(rc,
                      "column '%-.192s' cannot be used in key specification.",
                      keyPart->field->field_name);
      goto error;
    }
    // TODO: ASC or DESC
    keyObjBuilder.append(keyPart->field->field_name, 1);
  }
  keyObj = keyObjBuilder.obj();

  if (!strcmp(keyInfo->name, primary_key_name)) {
    isUnique = TRUE;
    isEnforced = TRUE;
  }

  if (keyInfo->flags & HA_NOSAME) {
    isUnique = TRUE;
  }

  rc = cl.create_index(keyObj, keyInfo->name, isUnique, isEnforced);
  if (rc) {
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int sdb_get_idx_order(KEY *key_info, bson::BSONObj &order,
                      int order_direction) {
  int rc = SDB_ERR_OK;
  const KEY_PART_INFO *keyPart;
  const KEY_PART_INFO *keyEnd;
  bson::BSONObjBuilder obj_builder;
  if (!key_info) {
    rc = SDB_ERR_INVALID_ARG;
    goto error;
  }
  keyPart = key_info->key_part;
  keyEnd = keyPart + key_info->user_defined_key_parts;
  for (; keyPart != keyEnd; ++keyPart) {
    obj_builder.append(keyPart->field->field_name, order_direction);
  }
  order = obj_builder.obj();

done:
  return rc;
error:
  goto done;
}

static void get_int_key_obj(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                            bson::BSONObj &obj, const char *op_str) {
  bson::BSONObjBuilder obj_builder;
  Field *field = key_part->field;
  const uchar *new_ptr = key_ptr + key_part->store_length - key_part->length;
  longlong value = field->val_int(new_ptr);
  if (value < 0 && ((Field_num *)field)->unsigned_flag) {
    // overflow UINT64, so store as DECIMAL
    bson::bsonDecimal decimal_val;
    char buf[24] = {0};
    sprintf(buf, "%llu", (uint64)value);
    decimal_val.fromString(buf);
    obj_builder.append(op_str, decimal_val);
  } else if (value > INT_MAX32 || value < INT_MIN32) {
    // overflow INT32, so store as INT64
    obj_builder.append(op_str, (long long)value);
  } else {
    obj_builder.append(op_str, (int)value);
  }
  obj = obj_builder.obj();
}

static void get_float_key_obj(const uchar *key_ptr,
                              const KEY_PART_INFO *key_part, bson::BSONObj &obj,
                              const char *op_str) {
  bson::BSONObjBuilder obj_builder;
  Field *field = key_part->field;
  const uchar *new_ptr = key_ptr + key_part->store_length - key_part->length;
  const uchar *old_ptr = field->ptr;
  field->ptr = (uchar *)new_ptr;
  double value = field->val_real();
  field->ptr = (uchar *)old_ptr;
  obj_builder.append(op_str, value);
  obj = obj_builder.obj();
}

static void get_text_key_val(const uchar *key_ptr,
                             bson::BSONObjBuilder &obj_builder,
                             const char *op_str, int length) {
  obj_builder.appendStrWithNoTerminating(op_str, (const char *)key_ptr, length);
}

static void get_enum_key_val(const uchar *key_ptr,
                             const KEY_PART_INFO *key_part, String &str_val) {
  const uchar *new_ptr = key_ptr + key_part->store_length - key_part->length;

  // enum charset must be field_charset(latin1), so this convertion is
  // necessary. we assert content is convertable, because error will be return
  // when insert.
  String org_str;
  key_part->field->val_str(&org_str, new_ptr);
  sdb_convert_charset(org_str, str_val, &SDB_CHARSET);
}

static void get_enum_key_val(const uchar *key_ptr,
                             const KEY_PART_INFO *key_part,
                             bson::BSONObjBuilder &obj_builder,
                             const char *op_str) {
  char buf[SDB_IDX_FIELD_SIZE_MAX + 1] = {0};
  String str_val(buf, SDB_IDX_FIELD_SIZE_MAX, key_part->field->charset());
  get_enum_key_val(key_ptr, key_part, str_val);
  obj_builder.appendStrWithNoTerminating(op_str, (const char *)(str_val.ptr()),
                                         str_val.length());
}

static int get_text_key_obj(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                            bson::BSONObj &obj, const char *op_str) {
  int rc = SDB_ERR_OK;
  bson::BSONObjBuilder obj_builder;
  const int suffix_len = 9;  // 9 == strlen( "(%c){0,}$" )
  uchar key_field_str_buf[SDB_IDX_FIELD_SIZE_MAX + 32] = {
      0};  // reserve 32bytes for operators and '\0'

  String *str = NULL;
  String org_str;
  String conv_str;
  const char *char_ptr;

  uchar pad_char = ' ';
  int key_start_pos = key_part->store_length - key_part->length;
  int pos;
  int new_length;
  int key_length = 0;

#define NULL_BITS 1

  if (NULL == key_ptr) {
    goto done;
  }

  /*if key's length is variable, remove spaces filled by mysql from the end of
    varibale key string. otherwise remove from the end of store_length.*/
  if (key_part->store_length - key_part->length > NULL_BITS) {
    key_length = key_part->null_bit ? get_variable_key_length(&key_ptr[1])
                                    : get_variable_key_length(&key_ptr[0]);
  } else {
    key_length = key_part->length;
  }

  org_str.set((const char *)(key_ptr + key_start_pos), key_length,
              key_part->field->charset());
  str = &org_str;
  if (!my_charset_same(org_str.charset(), &SDB_CHARSET)) {
    rc = sdb_convert_charset(org_str, conv_str, &SDB_CHARSET);
    if (rc) {
      goto error;
    }
    str = &conv_str;
  }

  /*we ignore the spaces end of key string which was filled by mysql.*/
  pos = str->length() - 1;
  char_ptr = str->ptr();
  if (' ' == char_ptr[pos] || '\t' == char_ptr[pos] || '\0' == char_ptr[pos]) {
    pad_char = char_ptr[pos];
    while (pos >= 0 && pad_char == char_ptr[pos]) {
      --pos;
    }
    new_length = pos + 1;
    str->set(str->ptr(), new_length, str->charset());
  }

  if (str->length() >= SDB_IDX_FIELD_SIZE_MAX) {
    rc = SDB_ERR_SIZE_OVF;
    goto error;
  }

  if (0 == strcmp("$et", op_str)) {
    if (!(key_part->key_part_flag & HA_PART_KEY_SEG && str->length())) {
      // TODO: it is exact match if start_key_ptr is same as end_key_ptr.
      /*sdb is sensitive to spaces belong to end string, while mysql is not
      sensitive so we return more results to the HA_READ_KEY_EXACT search.
      'where a = "hello"'
      euqal search in sdb with
      '({a:{$regex:"^hello( ){0,}$"})'
      */
      key_field_str_buf[0] = '^';
      int cur_pos = 1;
      if (key_part->field->real_type() == MYSQL_TYPE_ENUM ||
          key_part->field->real_type() == MYSQL_TYPE_SET) {
        char enum_val_buf[SDB_IDX_FIELD_SIZE_MAX] = {0};
        String str_val((char *)enum_val_buf + cur_pos,
                       SDB_IDX_FIELD_SIZE_MAX - cur_pos,
                       key_part->field->charset());
        get_enum_key_val(key_ptr, key_part, str_val);
        if (str_val.length() > 0) {
          memcpy(key_field_str_buf + cur_pos, str_val.ptr(), str_val.length());
          cur_pos += str_val.length();
        }
      } else {
        memcpy(key_field_str_buf + cur_pos, str->ptr(), str->length());
        cur_pos += str->length();
      }

      /*replace {a:{$et:"hello"}} with {a:{$regex:"^hello( ){0,}$"}}*/
      if ('\0' == pad_char)
        pad_char = ' ';
      snprintf((char *)key_field_str_buf + cur_pos, suffix_len, "(%c){0,}$",
               pad_char);
      cur_pos += suffix_len;
      obj_builder.appendStrWithNoTerminating(
          "$regex", (const char *)key_field_str_buf, cur_pos);
    }
    /* Find next rec. after key-record, or part key where a="abcdefg" (a(10),
       key(a(5)->"abcde")) */
    else {
      if (key_part->field->real_type() == MYSQL_TYPE_ENUM ||
          key_part->field->real_type() == MYSQL_TYPE_SET) {
        get_enum_key_val(key_ptr + key_start_pos, key_part, obj_builder,
                         "$gte");
      } else {
        get_text_key_val((const uchar *)str->ptr(), obj_builder, "$gte",
                         str->length());
      }
    }
  } else {
    if (key_part->field->real_type() == MYSQL_TYPE_ENUM ||
        key_part->field->real_type() == MYSQL_TYPE_SET) {
      get_enum_key_val(key_ptr + key_start_pos, key_part, obj_builder, op_str);
    } else {
      get_text_key_val((const uchar *)str->ptr(), obj_builder, op_str,
                       str->length());
    }
  }
  obj = obj_builder.obj();

done:
  return rc;
error:
  goto done;
}

static void get_datetime_key_obj(const uchar *key_ptr,
                                 const KEY_PART_INFO *key_part,
                                 bson::BSONObj &obj, const char *op_str) {
  bson::BSONObjBuilder obj_builder;
  const uchar *new_ptr = key_ptr + key_part->store_length - key_part->length;
  String org_str, str_val;
  key_part->field->val_str(&org_str, new_ptr);
  sdb_convert_charset(org_str, str_val, &SDB_CHARSET);
  obj_builder.appendStrWithNoTerminating(op_str, str_val.ptr(),
                                         str_val.length());
  obj = obj_builder.obj();
}

static int get_key_part_value(const KEY_PART_INFO *key_part,
                              const uchar *key_ptr, const char *op_str,
                              bool ignore_text_key, bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;

  switch (key_part->field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONGLONG: {
      get_int_key_obj(key_ptr, key_part, obj, op_str);
      break;
    }
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE: {
      get_float_key_obj(key_ptr, key_part, obj, op_str);
      break;
    }
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB: {
      if (!key_part->field->binary()) {
        if (!ignore_text_key) {
          rc = get_text_key_obj(key_ptr, key_part, obj, op_str);
          if (rc) {
            goto error;
          }
        }
      } else {
        // TODO: process the binary
        rc = HA_ERR_UNSUPPORTED;
        goto error;
      }
      break;
    }
    case MYSQL_TYPE_DATETIME: {
      get_datetime_key_obj(key_ptr, key_part, obj, op_str);
      break;
    }

    default:
      rc = HA_ERR_UNSUPPORTED;
      goto error;
  }

done:
  return rc;
error:
  goto done;
}

static inline int create_condition(Field *field, const KEY_PART_INFO *key_part,
                                   const uchar *key_ptr, const char *op_str,
                                   bool ignore_text_key,
                                   bson::BSONArrayBuilder &builder) {
  int rc = SDB_ERR_OK;
  bson::BSONObj op_obj;

  rc = get_key_part_value(key_part, key_ptr, op_str, ignore_text_key, op_obj);
  if (SDB_ERR_OK == rc) {
    if (!op_obj.isEmpty()) {
      bson::BSONObj cond = BSON(field->field_name << op_obj);
      builder.append(cond);
    }
  }

  return rc;
}

// This function is modified from ha_federated::create_where_from_key.
int sdb_create_condition_from_key(TABLE *table, KEY *key_info,
                                  const key_range *start_key,
                                  const key_range *end_key,
                                  bool from_records_in_range, bool eq_range_arg,
                                  bson::BSONObj &condition) {
  int rc = SDB_ERR_OK;
  const uchar *key_ptr;
  uint remainder, length;
  const key_range *ranges[2] = {start_key, end_key};
  my_bitmap_map *old_map;
  bson::BSONArrayBuilder builder;
  bson::BSONArray array;

  if (start_key == NULL && end_key == NULL) {
    return rc;
  }

  old_map = dbug_tmp_use_all_columns(table, table->read_set);
  for (uint i = 0; i <= 1; i++) {
    const KEY_PART_INFO *key_part;
    bool ignore_text_key = false;

    if (ranges[i] == NULL) {
      continue;
    }

    // ignore end key of prefix index and like
    if (i > 0 && HA_READ_BEFORE_KEY != ranges[i]->flag) {
      ignore_text_key = true;
    }

    for (key_part = key_info->key_part,
        remainder = key_info->user_defined_key_parts,
        length = ranges[i]->length, key_ptr = ranges[i]->key;
         ; remainder--, key_part++) {
      Field *field = key_part->field;
      uint store_length = key_part->store_length;

      if (key_part->null_bit) {
        if (*key_ptr) {
          /*
            We got "IS [NOT] NULL" condition against nullable column. We
            distinguish between "IS NOT NULL" and "IS NULL" by flag. For
            "IS NULL", flag is set to HA_READ_KEY_EXACT.
          */
          int is_null;
          switch (ranges[i]->flag) {
            case HA_READ_KEY_EXACT:
            case HA_READ_BEFORE_KEY:
            case HA_READ_KEY_OR_PREV:
            case HA_READ_PREFIX_LAST:
            case HA_READ_PREFIX_LAST_OR_PREV:
              is_null = 1;
              break;
            case HA_READ_AFTER_KEY:
              is_null = i > 0 ? 1 : 0;
              break;
            case HA_READ_KEY_OR_NEXT:
              // >= null means read all records
            default:
              goto prepare_for_next_key_part;
          }
          bson::BSONObj is_null_obj = BSON("$isnull" << is_null);
          bson::BSONObj is_null_cond = BSON(field->field_name << is_null_obj);
          builder.append(is_null_cond);

          /*
            We need to adjust pointer and length to be prepared for next
            key part. As well as check if this was last key part.
          */
          goto prepare_for_next_key_part;
        }
      }

      switch (ranges[i]->flag) {
        case HA_READ_KEY_EXACT: {
          DBUG_PRINT("info", ("sequoiadb HA_READ_KEY_EXACT %d", i));
          const char *op_str = from_records_in_range ? "$gte" : "$et";
          rc = create_condition(field, key_part, key_ptr, op_str,
                                ignore_text_key, builder);
          if (0 != rc) {
            goto error;
          }
          break;
        }
        case HA_READ_AFTER_KEY: {
          if (eq_range_arg) {
            break;
          }
          DBUG_PRINT("info", ("sequoiadb HA_READ_AFTER_KEY %d", i));
          if ((store_length >= length) || (i > 0)) /* for all parts of end key*/
          {
            // end_key : start_key
            const char *op_str = i > 0 ? "$lte" : "$gt";
            rc = create_condition(field, key_part, key_ptr, op_str,
                                  ignore_text_key, builder);
            if (0 != rc) {
              goto error;
            }
            break;
          }
        }
        case HA_READ_KEY_OR_NEXT: {
          DBUG_PRINT("info", ("sequoiadb HA_READ_KEY_OR_NEXT %d", i));
          const char *op_str = "$gte";
          rc = create_condition(field, key_part, key_ptr, op_str,
                                ignore_text_key, builder);
          if (0 != rc) {
            goto error;
          }
          break;
        }
        case HA_READ_BEFORE_KEY: {
          DBUG_PRINT("info", ("sequoiadb HA_READ_BEFORE_KEY %d", i));
          if (store_length >= length) {
            const char *op_str = "$lt";
            rc = create_condition(field, key_part, key_ptr, op_str,
                                  ignore_text_key, builder);
            if (0 != rc) {
              goto error;
            }
            break;
          }
        }
        case HA_READ_KEY_OR_PREV:
        case HA_READ_PREFIX_LAST:
        case HA_READ_PREFIX_LAST_OR_PREV: {
          DBUG_PRINT("info", ("sequoiadb HA_READ_KEY_OR_PREV %d", i));
          const char *op_str = "$lte";
          rc = create_condition(field, key_part, key_ptr, op_str,
                                ignore_text_key, builder);
          if (0 != rc) {
            goto error;
          }
          break;
        }
        default:
          DBUG_PRINT("info", ("cannot handle flag %d", ranges[i]->flag));
          rc = HA_ERR_UNSUPPORTED;
          goto error;
      }

    prepare_for_next_key_part:
      if (store_length >= length) {
        break;
      }
      DBUG_PRINT("info", ("remainder %d", remainder));
      DBUG_ASSERT(remainder > 1);
      length -= store_length;
      key_ptr += store_length;
    }
  }
  dbug_tmp_restore_column_map(table->read_set, old_map);

  array = builder.arr();
  if (array.nFields() > 1) {
    condition = BSON("$and" << array);
  } else if (!array.isEmpty()) {
    condition = array.firstElement().embeddedObject().getOwned();
  }

  return rc;

error:
  dbug_tmp_restore_column_map(table->read_set, old_map);
  return rc;
}
