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

#include <my_dbug.h>
#include "sdb_item.h"
#include "sdb_err_code.h"


int sdb_logic_item::push( sdb_item *cond_item )
{
   int rc = 0 ;
   bson::BSONObj obj_tmp ;

   if ( is_finished )
   {
      // there must be something wrong,
      // skip all condition
      rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
      is_ok = FALSE ;
      goto error ;
   }

   rc = cond_item->to_bson( obj_tmp ) ;
   if ( rc != 0 )
   {
      // skip the error and go on to parse the condition-item
      // the error will return in to_bson() ;
      rc = SDB_ERR_COND_PART_UNSUPPORTED ;
      is_ok = FALSE ;
      goto error ;
   }
   delete cond_item ;
   children.append( obj_tmp ) ;

done:
   return rc ;
error:
   goto done ;
}

int sdb_logic_item::push( Item *cond_item )
{
   if ( NULL != cond_item )
   {
      return SDB_ERR_COND_UNEXPECTED_ITEM ;
   }
   is_finished = TRUE ;
   return SDB_ERR_OK ;
}

int sdb_logic_item::to_bson( bson::BSONObj &obj )
{
   int rc = SDB_ERR_OK ;
   if ( is_ok )
   {
      obj = BSON( this->name() << children.arr() ) ;
   }
   else
   {
      rc = SDB_ERR_COND_INCOMPLETED ;
   }
   return rc ;
}

int sdb_and_item::to_bson( bson::BSONObj &obj )
{
   obj = BSON( this->name() << children.arr() ) ;
   return SDB_ERR_OK ;
}

sdb_func_item::sdb_func_item()
   :para_num_cur(0),
   para_num_max(1)
{
   l_child = NULL ;
   r_child = NULL ;
}

sdb_func_item::~sdb_func_item()
{
   para_list.pop() ;
   if ( l_child != NULL )
   {
      delete l_child ;
      l_child = NULL ;
   }
   if ( r_child != NULL )
   {
      delete r_child ;
      r_child = NULL ;
   }
}

void sdb_func_item::update_stat()
{
   if ( ++para_num_cur >= para_num_max )
   {
      is_finished = TRUE ;
   }
}

int sdb_func_item::push( sdb_item *cond_item )
{
   int rc = SDB_ERR_OK ;
   if ( cond_item->type() != Item_func::UNKNOWN_FUNC )
   {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
      goto error ;
   }

   if ( ((sdb_func_unkown *)cond_item)->get_func_item()->const_item() )
   {
      rc = push(((sdb_func_unkown *)cond_item)->get_func_item()) ;
      if ( rc != SDB_ERR_OK )
      {
         goto error ;
      }
      delete cond_item ;
   }
   else
   {
      if ( l_child != NULL || r_child != NULL )
      {
         rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
         goto error ;
      }
      if ( para_num_cur != 0 )
      {
         r_child = cond_item ;
      }
      else
      {
         l_child = cond_item ;
      }
      update_stat() ;
   }
done:
   return rc ;
error:
   goto done ;
}

int sdb_func_item::push( Item *cond_item )
{
   int rc = SDB_ERR_OK ;

   if ( is_finished )
   {
      goto error ;
   }
   para_list.push_back( cond_item ) ;
   update_stat() ;
done:
   return rc ;
error:
   rc = SDB_ERR_COND_UNSUPPORTED ;
   goto done ;
}

int sdb_func_item::pop( Item *&para_item )
{
   if ( para_list.is_empty() )
   {
      return SDB_ERR_EOF ;
   }
   para_item = para_list.pop() ;
   return SDB_ERR_OK ;
}

/*int sdb_func_item::get_item_val( const char *field_name,
                                Item *item_val,
                                Field *field,
                                bson::BSONObj & obj,
                                bson::BSONArrayBuilder *arr_builder )
{
   int rc = SDB_ERR_OK ;
   int type_tmp ;
   enum_field_types type = field->type() ;
   if ( ( MYSQL_TYPE_DOUBLE < type && type < MYSQL_TYPE_TINY_BLOB
       && MYSQL_TYPE_INT24 != type && MYSQL_TYPE_YEAR != type
       && MYSQL_TYPE_VARCHAR != type && MYSQL_TYPE_ENUM != type
       && MYSQL_TYPE_LONGLONG != type ) || type >= MYSQL_TYPE_GEOMETRY )
   {
      rc = SDB_ERR_OVF ;
      goto error ;
   }

   if ( NULL == item_val )
   {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
      goto error ;
   }

   if ( item_val->const_item() )
   {
      switch( item_val->result_type() )
      {
         case INT_RESULT:
            {
               type_tmp = Item::INT_ITEM ;
               break ;
            }
         case STRING_RESULT:
            {
               type_tmp = Item::STRING_ITEM ;
               break ;
            }
         case REAL_RESULT:
            {
               type_tmp = Item::REAL_ITEM ;
               break ;
            }
         case DECIMAL_RESULT:
            {
               type_tmp = Item::DECIMAL_ITEM ;
               break ;
            }
         case ROW_RESULT:
         default:
            {
               rc = SDB_ERR_OVF ;
               goto error ;
            }
      }
   }
   else
   {
      type_tmp = item_val->type() ;
   }
   //TODO:support all types
   //TODO:support all types
   //TODO:support all types
   //TODO:support all types
   switch( type_tmp )
   {
      case Item::STRING_ITEM:
         {
            const char *str = NULL ;
            size_t len = 0 ;
            if ( *(item_val->item_name.ptr()) == '?' )
            {
               str = item_val->str_value.ptr() ;
               len = item_val->str_value.length() ;
            }
            else
            {
               str = item_val->item_name.ptr() ;
               len = item_val->item_name.length() ;
            }
            if ( MYSQL_TYPE_TINY_BLOB == type || MYSQL_TYPE_MEDIUM_BLOB == type
                 || MYSQL_TYPE_LONG_BLOB == type || MYSQL_TYPE_BLOB == type )
            {
               Field_str *f = (Field_str *)field ;
               if ( f->binary() )
               {
                  if ( NULL == arr_builder )
                  {
                     bson::BSONObjBuilder obj_builder ;
                     obj_builder.appendBinData(field_name,
                                               len,
                                               bson::BinDataGeneral,
                                               str ) ;
                     obj = obj_builder.obj() ;
                  }
                  else
                  {
                     //bson::BSONObj obj_tmp
                     //   = BSON( "$binary" << item_val->item_name.ptr()
                     //           << "$type" << bson::BinDataGeneral ) ;
                     //arr_builder->append( obj_tmp ) ;
                     rc = SDB_ERR_TYPE_UNSUPPORTED ;
                  }
                  break ;
               }
            }
            if ( NULL == arr_builder )
            {
               obj = BSON( field_name
                           << item_val->item_name.ptr() ) ;
            }
            else
            {
               arr_builder->append( item_val->item_name.ptr() ) ;
            }
            break ;
         }
      case Item::INT_ITEM:
         {
            if ( NULL == arr_builder )
            {
               obj = BSON( field_name
                           << item_val->val_int() ) ;
            }
            else
            {
               arr_builder->append( item_val->val_int() ) ;
            }
            break ;
         }
      case Item::DECIMAL_ITEM:
         {
            if ( NULL == arr_builder )
            {
               bson::BSONObjBuilder obj_builder ;
               obj_builder.appendDecimal( field_name,
                                          item_val->item_name.ptr() ) ;
               obj = obj_builder.obj() ;
            }
            else
            {
               bson::bsonDecimal decimal ;
               rc = decimal.init() ;
               if ( 0 != rc )
               {
                   rc =  SDB_ERR_OVF ;
                   goto error ;
               }
 
               rc = decimal.fromString( item_val->item_name.ptr() ) ;
               if ( 0 != rc )
               {
                   rc =  SDB_ERR_OVF ;
                   goto error ;
               }
                arr_builder->append( decimal ) ;
            }
            break ;
         }
      case Item::REAL_ITEM:
         {
            if ( NULL == arr_builder )
            {
               obj = BSON( field_name
                           << item_val->val_real() ) ;
            }
            else
            {
               arr_builder->append( item_val->val_real() ) ;
            }
            break ;
         }
      default:
         {
            rc = SDB_ERR_COND_UNKOWN_ITEM ;
            goto error ;
         }
   }
done:
   return rc ;
error:
   goto done ;
}*/

int sdb_func_item::get_item_val( const char *field_name,
                                Item *item_val,
                                Field *field,
                                bson::BSONObj & obj,
                                bson::BSONArrayBuilder *arr_builder )
{
   int rc = SDB_ERR_OK ;
   char buff[MAX_FIELD_WIDTH] = {0};
   
   if ( NULL == item_val )
   {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
      goto error ;
   }

   if ( !item_val->const_item() )
   {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
      goto error ;
   }

   if ( Item::NULL_ITEM == item_val->type() )
   {
      // "$exists" appear in array is not support now
      if ( NULL == arr_builder )
      {
         if ( type() == Item_func::EQ_FUNC )
         {
            obj = BSON( "$exists" << 0 ) ;
            goto done ;
         }
         else if ( type() == Item_func::NE_FUNC )
         {
            obj = BSON( "$exists" << 1 ) ;
            goto done ;
         }
      }
      rc = SDB_ERR_OVF ;
      goto error ;
   }

   switch( field->type() )
   {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE:
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
         {
            if ( item_val->result_type() == INT_RESULT )
            {
               longlong val_tmp = 0 ;
               if ( item_val->unsigned_flag )
               {
                  val_tmp = item_val->val_uint() ;
                  if ( val_tmp < 0 )
                  {
                     if ( NULL == arr_builder )
                     {
                        bson::BSONObjBuilder obj_builder ;
                        my_decimal dec_tmp ;
                        String str( buff, sizeof(buff),
                                    item_val->charset_for_protocol() ) ;
                        item_val->val_decimal( &dec_tmp ) ;
                        my_decimal2string( E_DEC_FATAL_ERROR, &dec_tmp,
                                           0, 0, 0, &str ) ;
                        obj_builder.appendDecimal( field_name, str.c_ptr() ) ;
                        obj = obj_builder.obj() ;
                        break ;
                     }
                     rc = SDB_ERR_OVF ;
                     goto error ;
                  }
               }
               else
               {
                  val_tmp = item_val->val_int() ;
               }
               if ( NULL == arr_builder )
               {
                  obj = BSON( field_name << val_tmp ) ;
               }
               else
               {
                  arr_builder->append( val_tmp ) ;
               }
            }
            else if( item_val->result_type() == REAL_RESULT )
            {
               if ( NULL == arr_builder )
               {
                  obj = BSON( field_name
                              << item_val->val_real() ) ;
               }
               else
               {
                  arr_builder->append( item_val->val_real() ) ;
               }
            }
            else if( item_val->result_type() == DECIMAL_RESULT
                     || item_val->result_type() == STRING_RESULT )
            {
               String str( buff, sizeof(buff),
                           item_val->charset_for_protocol() ) ;
               String *pStr ;
               pStr = item_val->val_str( &str ) ;
               if ( NULL == pStr )
               {
                  rc =  SDB_ERR_INVALID_ARG ;
                  goto error ;
               }
               if ( NULL == arr_builder )
               {
                  bson::BSONObjBuilder obj_builder ;
                  if( !obj_builder.appendDecimal( field_name,
                                                  pStr->c_ptr() ))
                  {
                     rc =  SDB_ERR_INVALID_ARG ;
                     goto error ;
                  }
                  obj = obj_builder.obj() ;
               }
               else
               {
                  bson::bsonDecimal decimal ;
                  if ( item_val->result_type() == STRING_RESULT )
                  {
                     //TODO:SEQUOIADBMAINSTREAM-3365
                     //     the string value is not support for "in"
                     rc = SDB_ERR_TYPE_UNSUPPORTED ;
                     goto error ;
                  }
                  rc = decimal.init() ;
                  if ( 0 != rc )
                  {
                     rc =  SDB_ERR_OOM ;
                     goto error ;
                  }
    
                  rc = decimal.fromString( pStr->c_ptr() ) ;
                  if ( 0 != rc )
                  {
                     rc =  SDB_ERR_INVALID_ARG ;
                     goto error ;
                  }
                   arr_builder->append( decimal ) ;
               }
            }
            else
            {
               rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
               goto error ;
            }
            break ;
         }

      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_BLOB:
         {
            if ( item_val->result_type() == STRING_RESULT )
            {
               String str( buff, sizeof(buff),
                           item_val->charset_for_protocol() ) ;
               String *pStr = NULL ;
               pStr = item_val->val_str( &str ) ;
               Field_str *f = (Field_str *)field ;
               if ( NULL == pStr )
               {
                  rc = SDB_ERR_INVALID_ARG ;
                  break ;
               }
               if ( f->binary() )
               {
                  rc = SDB_ERR_TYPE_UNSUPPORTED ;
                  break ;
                  /*if ( NULL == arr_builder )
                  {
                     bson::BSONObjBuilder obj_builder ;
                     obj_builder.appendBinData(field_name,
                                               pStr->length(),
                                               bson::BinDataGeneral,
                                               pStr->c_ptr() ) ;
                     obj = obj_builder.obj() ;
                  }
                  else
                  {*/
                     // binary is not supported for array in sequoiadb.
                     /*
                     bson::BSONObj obj_tmp
                        = BSON( "$binary" << item_val->item_name.ptr()
                                << "$type" << bson::BinDataGeneral ) ;
                     arr_builder->append( obj_tmp ) ;*/
                     /*rc = SDB_ERR_TYPE_UNSUPPORTED ;
                  }
                  break ;*/
               }
               if ( NULL == arr_builder )
               {
                  obj = BSON( field_name
                              << pStr->c_ptr() ) ;
               }
               else
               {
                  arr_builder->append( pStr->c_ptr() ) ;
               }
            }
            else
            {
               rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
               goto error ;
            }
            break ;
         }

      case MYSQL_TYPE_DATE:
         {
            MYSQL_TIME ltime ;
            if ( STRING_RESULT == item_val->result_type()
               && !item_val->get_date( &ltime, TIME_FUZZY_DATE ) )
            {
               struct tm tm_val ;
               tm_val.tm_sec = ltime.second ;
               tm_val.tm_min = ltime.minute ;
               tm_val.tm_hour = ltime.hour ;
               tm_val.tm_mday = ltime.day ;
               tm_val.tm_mon = ltime.month - 1 ;
               tm_val.tm_year = ltime.year - 1900 ;
               tm_val.tm_wday = 0 ;
               tm_val.tm_yday = 0 ;
               tm_val.tm_isdst = 0 ;
               time_t time_tmp = mktime( &tm_val ) ;
               bson::Date_t dt( (longlong)(time_tmp * 1000) ) ;
               if ( NULL == arr_builder )
               {
                  bson::BSONObjBuilder obj_builder ;
                  obj_builder.appendDate( field_name, dt ) ;
                  obj = obj_builder.obj() ;
               }
               else
               {
                  rc = SDB_ERR_TYPE_UNSUPPORTED ;
               }
               break ;
            }
            rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
            goto error ;
         }

      case MYSQL_TYPE_TIMESTAMP:
      case MYSQL_TYPE_TIMESTAMP2:
      case MYSQL_TYPE_DATETIME:
         {
            MYSQL_TIME ltime ;
            if ( item_val->result_type() != STRING_RESULT
                 || item_val->get_time( &ltime )
                 || ltime.year > 2037 || ltime.year < 1902 )
            {
               rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
               goto error ;
            }
            else
            {
               struct tm tm_val ;
               tm_val.tm_sec = ltime.second ;
               tm_val.tm_min = ltime.minute ;
               tm_val.tm_hour = ltime.hour ;
               tm_val.tm_mday = ltime.day ;
               tm_val.tm_mon = ltime.month - 1 ;
               tm_val.tm_year = ltime.year - 1900 ;
               tm_val.tm_wday = 0 ;
               tm_val.tm_yday = 0 ;
               tm_val.tm_isdst = 0 ;
               time_t time_tmp = mktime( &tm_val ) ;
               unsigned long long time_val_tmp ;
               memcpy( (char *)&time_val_tmp, &(ltime.second_part), 4 ) ;
               memcpy( (char *)&time_val_tmp+4, &time_tmp, 4 ) ;
               if ( NULL == arr_builder )
               {
                  bson::BSONObjBuilder obj_builder ;
                  obj_builder.appendTimestamp( field_name,
                                               time_val_tmp ) ;
                  obj = obj_builder.obj() ;
               }
               else
               {
                  arr_builder->appendTimestamp( time_val_tmp ) ;
               }
               break ;
            }
         }

      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_YEAR:
      case MYSQL_TYPE_NEWDATE:
      case MYSQL_TYPE_DATETIME2:
      case MYSQL_TYPE_TIME2:

      case MYSQL_TYPE_NULL:
      case MYSQL_TYPE_BIT:
      case MYSQL_TYPE_JSON:
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_GEOMETRY:
      default:
         {
            rc = SDB_ERR_TYPE_UNSUPPORTED ;
            goto error ;
         }
   }

done:
   return rc ;
error:
   goto done ;
}

sdb_func_unkown::sdb_func_unkown( Item_func *item )
{
   func_item = item ;
   para_num_max = item->argument_count() ;
   if ( 0 == para_num_max )
   {
      is_finished = TRUE ;
   }
}

sdb_func_unkown::~sdb_func_unkown()
{
}

sdb_func_unary_op::sdb_func_unary_op()
{
   para_num_max = 1 ;
}

sdb_func_unary_op::~sdb_func_unary_op()
{
}

sdb_func_isnull::sdb_func_isnull()
{
}

sdb_func_isnull::~sdb_func_isnull()
{
}

int sdb_func_isnull::to_bson( bson::BSONObj &obj )
{
   int rc = SDB_ERR_OK ;
   Item *item_tmp = NULL ;

   if ( !is_finished || para_list.elements != para_num_max )
   {
      rc = SDB_ERR_COND_INCOMPLETED ;
      goto error ;
   }

   if ( l_child != NULL || r_child != NULL )
   {
      rc = SDB_ERR_COND_UNKOWN_ITEM ;
      goto error ;
   }

   item_tmp = para_list.pop() ;
   if ( Item::FIELD_ITEM != item_tmp->type() )
   {
      rc = SDB_ERR_COND_UNKOWN_ITEM ;
      goto error ;
   }
   obj = BSON( ((Item_field *)item_tmp)->field_name
                << BSON( this->name() << 0 ) ) ;

done:
   return rc ;
error:
   goto done ;
}

sdb_func_isnotnull::sdb_func_isnotnull()
{
}

sdb_func_isnotnull::~sdb_func_isnotnull()
{
}

int sdb_func_isnotnull::to_bson( bson::BSONObj &obj )
{
   int rc = SDB_ERR_OK ;
   Item *item_tmp = NULL ;

   if ( !is_finished || para_list.elements != para_num_max )
   {
      rc = SDB_ERR_COND_INCOMPLETED ;
      goto error ;
   }

   item_tmp = para_list.pop() ;
   if ( Item::FIELD_ITEM != item_tmp->type() )
   {
      rc = SDB_ERR_COND_UNKOWN_ITEM ;
      goto error ;
   }
   obj = BSON( ((Item_field *)item_tmp)->field_name
               << BSON( this->name() << 1 ) ) ;

done:
   return rc ;
error:
   goto done ;
}

sdb_func_bin_op::sdb_func_bin_op()
{
   para_num_max = 2 ;
}

sdb_func_bin_op::~sdb_func_bin_op()
{
}

sdb_func_cmp::sdb_func_cmp()
{
}

sdb_func_cmp::~sdb_func_cmp()
{
}

int sdb_func_cmp::to_bson_with_child( bson::BSONObj &obj )
{
   int rc = SDB_ERR_OK ;
   sdb_item *child = NULL ;
   Item *field1 = NULL, *field2 = NULL, *field3 = NULL, *item_tmp ;
   Item_func *func = NULL ;
   sdb_func_unkown *sdb_func = NULL ;
   bool cmp_inverse = FALSE ;
   bson::BSONObj obj_tmp ;
   bson::BSONObjBuilder builder_tmp ;

   if ( r_child != NULL )
   {
      child = r_child ;
      cmp_inverse = TRUE ;
   }
   else
   {
      child = l_child ;
   }

   if ( child->type() != Item_func::UNKNOWN_FUNC
        || !child->finished()
        || ((sdb_func_item*)child)->get_para_num() != 2
        || this->get_para_num() != 2 )
   {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
      goto error ;
   }
   sdb_func = (sdb_func_unkown *)child ;
   item_tmp = sdb_func->get_func_item() ;
   if ( item_tmp->type() != Item::FUNC_ITEM )
   {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
      goto error ;
   }
   func = (Item_func *)item_tmp ;
   if ( func->functype() != Item_func::UNKNOWN_FUNC )
   {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
      goto error ;
   }

   if( sdb_func->pop( field1 )
       || sdb_func->pop( field2 ) )
   {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
      goto error ;
   }
   field3 = para_list.pop() ;

   if ( Item::FIELD_ITEM == field1->type() )
   {
      if ( Item::FIELD_ITEM == field2->type() )
      {
         if ( !(field3->const_item())
              || ( 0 != strcmp(func->func_name(), "-")
              && 0 != strcmp(func->func_name(), "/") ) )
         {
            rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
            goto error ;
         }

         if ( 0 == strcmp(func->func_name(), "-") )
         {
            // field1 - field2 < num
            rc = get_item_val( "$add", field3,
                               ((Item_field *)field2)->field,
                               obj_tmp ) ;
         }
         else
         {
            // field1 / field2 < num
            rc = get_item_val( "$multiply", field3,
                               ((Item_field *)field2)->field,
                               obj_tmp ) ;
         }
         if ( rc != SDB_ERR_OK )
         {
            goto error ;
         }
         builder_tmp.appendElements( obj_tmp ) ;
         obj_tmp = BSON( (cmp_inverse ? this->name() : this->inverse_name())
                        << BSON( "$field" << ((Item_field *)field1)->field->field_name ) ) ;
         builder_tmp.appendElements( obj_tmp ) ;
         obj_tmp = builder_tmp.obj() ;
         obj = BSON( ((Item_field *)field2)->field_name << obj_tmp ) ;
      }
      else
      {
         if ( !field2->const_item() )
         {
            rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
            goto error ;
         }
         if ( 0 == strcmp(func->func_name(), "+") )
         {
            rc = get_item_val( "$add", field2, ((Item_field *)field1)->field, obj_tmp ) ;
         }
         else if ( 0 == strcmp(func->func_name(), "-") )
         {
            rc = get_item_val( "$subtract", field2, ((Item_field *)field1)->field, obj_tmp ) ;
         }
         else if ( 0 == strcmp(func->func_name(), "*") )
         {
            rc = get_item_val( "$multiply", field2, ((Item_field *)field1)->field, obj_tmp ) ;
         }
         else if ( 0 == strcmp(func->func_name(), "/") )
         {
            rc = get_item_val( "$divide", field2, ((Item_field *)field1)->field, obj_tmp ) ;
         }
         else
         {
            rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
         }
         if ( rc != SDB_ERR_OK )
         {
            goto error ;
         }
         builder_tmp.appendElements( obj_tmp ) ;
         if ( Item::FIELD_ITEM == field3->type() )
         {
            // field1 - num < field3
            obj_tmp = BSON( (cmp_inverse ?  this->inverse_name() : this->name())
                            << BSON( "$field" << ((Item_field *)field3)->field->field_name ) ) ;
         }
         else
         {
            // field1 - num1 < num3
            rc = get_item_val( (cmp_inverse ?  this->inverse_name() : this->name()),
                                field3, ((Item_field *)field1)->field, obj_tmp ) ;
            if ( rc != SDB_ERR_OK )
            {
               goto error ;
            }
         }
         builder_tmp.appendElements( obj_tmp ) ;
         obj_tmp = builder_tmp.obj() ;
         obj = BSON( ((Item_field *)field1)->field->field_name << obj_tmp ) ;
      }
   }
   else
   {
      if ( !field1->const_item() )
      {
         rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
         goto error ;
      }
      if ( Item::FIELD_ITEM == field2->type() )
      {
         if ( Item::FIELD_ITEM == field3->type() )
         {
            // num + field2 < field3
            if ( 0 == strcmp(func->func_name(), "+") )
            {
               rc = get_item_val( "$add", field1, ((Item_field *)field2)->field, obj_tmp ) ;
            }
            else if (0 == strcmp(func->func_name(), "*") )
            {
               rc = get_item_val( "$multiply", field1, ((Item_field *)field2)->field, obj_tmp ) ;
            }
            else
            {
               rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
            }
            if ( rc != SDB_ERR_OK )
            {
               goto error ;
            }
            builder_tmp.appendElements( obj_tmp ) ;
            obj_tmp = BSON( (cmp_inverse ? this->inverse_name() : this->name())
                            << BSON( "$field" << ((Item_field *)field3)->field->field_name ) ) ;
            builder_tmp.appendElements( obj_tmp ) ;
            obj = BSON( ((Item_field *)field2)->field->field_name << builder_tmp.obj() ) ;
         }
         else
         {
            if ( !field3->const_item() )
            {
               rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
               goto error ;
            }
            if ( 0 == strcmp( func->func_name(), "+") )
            {
               // num1 + field2 < num3
               rc = get_item_val( "$add", field1, ((Item_field *)field2)->field, obj_tmp ) ;
               if ( rc !=  SDB_ERR_OK )
               {
                  goto error ;
               }
               builder_tmp.appendElements( obj_tmp ) ;
               rc = get_item_val( (cmp_inverse ? this->inverse_name() : this->name()),
                                  field3, ((Item_field *)field2)->field, obj_tmp ) ;
               if ( rc !=  SDB_ERR_OK )
               {
                  goto error ;
               }
               builder_tmp.appendElements( obj_tmp ) ;
               obj = BSON( ((Item_field *)field2)->field->field_name << builder_tmp.obj() ) ;
            }
            else if ( 0 == strcmp( func->func_name(), "-") )
            {
               // num1 - field2 < num3   =>   num1 < num3 + field2
               rc = get_item_val( "$add", field3, ((Item_field *)field2)->field, obj_tmp ) ;
               if ( rc !=  SDB_ERR_OK )
               {
                  goto error ;
               }
               builder_tmp.appendElements( obj_tmp ) ;
               rc = get_item_val( (cmp_inverse ? this->name() : this->inverse_name()),
                                  field1, ((Item_field *)field2)->field, obj_tmp ) ;
               if ( rc !=  SDB_ERR_OK )
               {
                  goto error ;
               }
               builder_tmp.appendElements( obj_tmp ) ;
               obj = BSON( ((Item_field *)field2)->field->field_name << builder_tmp.obj() ) ;
            }
            else if ( 0 == strcmp( func->func_name(), "*") )
            {
               // num1 * field2 < num3
               rc = get_item_val( "$multiply", field1, ((Item_field *)field2)->field, obj_tmp ) ;
               if ( rc !=  SDB_ERR_OK )
               {
                  goto error ;
               }
               builder_tmp.appendElements( obj_tmp ) ;
               rc = get_item_val( (cmp_inverse ? this->inverse_name() : this->name()),
                                  field3, ((Item_field *)field2)->field, obj_tmp ) ;
               if ( rc !=  SDB_ERR_OK )
               {
                  goto error ;
               }
               builder_tmp.appendElements( obj_tmp ) ;
               obj = BSON( ((Item_field *)field2)->field->field_name << builder_tmp.obj() ) ;            
            }
            else if ( 0 == strcmp( func->func_name(), "/") )
            {
               // num1 / field2 < num3   =>   num1 < num3 + field2
               rc = get_item_val( "$multiply", field3, ((Item_field *)field2)->field, obj_tmp ) ;
               if ( rc !=  SDB_ERR_OK )
               {
                  goto error ;
               }
               builder_tmp.appendElements( obj_tmp ) ;
               rc = get_item_val( (cmp_inverse ? this->name() : this->inverse_name()),
                                  field1, ((Item_field *)field2)->field, obj_tmp ) ;
               if ( rc !=  SDB_ERR_OK )
               {
                  goto error ;
               }
               builder_tmp.appendElements( obj_tmp ) ;
               obj = BSON( ((Item_field *)field2)->field->field_name << builder_tmp.obj() ) ;
            }
            else
            {
               rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
               goto error ;
            }
         }
      }
      else
      {
         rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
         goto error ;
      }
   }

done:
   return rc ;
error:
   goto done ;
}

int sdb_func_cmp::to_bson( bson::BSONObj &obj )
{
   int rc = SDB_ERR_OK ;
   bool inverse = FALSE ;
   bool cmp_with_field = FALSE ;
   Item *item_tmp = NULL, *item_val = NULL ;
   Item_field *item_field = NULL ;
   const char *name_tmp = NULL ;
   bson::BSONObj obj_tmp ;

   if ( !is_finished || para_list.elements != para_num_max )
   {
      rc = SDB_ERR_COND_INCOMPLETED ;
      goto error ;
   }

   if ( l_child != NULL || r_child != NULL )
   {
      rc = to_bson_with_child( obj ) ;
      if ( rc != SDB_ERR_OK )
      {
         goto error ;
      }
      goto done ;
   }

   while( !para_list.is_empty() )
   {
      item_tmp = para_list.pop() ;
      if ( Item::FIELD_ITEM != item_tmp->type() )
      {
         if ( NULL == item_field )
         {
            inverse = TRUE ;
         }
         if ( item_val != NULL )
         {
            rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
            goto error ;
         }
         item_val = item_tmp ;
      }
      else
      {
         if ( item_field != NULL )
         {
            if ( item_val != NULL )
            {
               rc = SDB_ERR_COND_PART_UNSUPPORTED ;
               goto error ;
            }
            item_val = item_tmp ;
            cmp_with_field = TRUE ;
         }
         else
         {
            item_field = (Item_field *)item_tmp ;
         }
      }
   }

   if ( inverse )
   {
      name_tmp = this->inverse_name() ;
   }
   else
   {
      name_tmp = this->name() ;
   }

   if ( cmp_with_field )
   {
      obj = BSON( item_field->field_name
                  << BSON( name_tmp
                           << BSON( "$field"
                                    << ((Item_field *)item_val)->field_name )));
      goto done ;
   }

   rc = get_item_val( name_tmp, item_val,
                      item_field->field, obj_tmp ) ;
   if ( rc )
   {
      goto error ;
   }
   obj = BSON( item_field->field_name
               << obj_tmp ) ;

done:
   return rc ;
error:
   if ( SDB_ERR_OVF == rc )
   {
      rc = SDB_ERR_COND_PART_UNSUPPORTED ;
   }
   goto done ;
}

sdb_func_between::sdb_func_between( bool has_not )
   : sdb_func_neg( has_not )
{
   para_num_max = 3 ;
}

sdb_func_between::~sdb_func_between()
{
}

int sdb_func_between::to_bson( bson::BSONObj &obj )
{
   int rc = SDB_ERR_OK ;
   Item_field *item_field = NULL ;
   Item *item_start = NULL, *item_end = NULL, *item_tmp = NULL ;
   bson::BSONObj obj_start, obj_end, obj_tmp ;
   bson::BSONArrayBuilder arr_builder ;

   if ( !is_finished || para_list.elements != para_num_max )
   {
      rc = SDB_ERR_COND_INCOMPLETED ;
      goto error ;
   }

   if ( l_child != NULL || r_child != NULL )
   {
      rc = SDB_ERR_COND_UNKOWN_ITEM ;
      goto error ;
   }

   item_tmp = para_list.pop() ;
   if ( Item::FIELD_ITEM != item_tmp->type() )
   {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
      goto error ;
   }
   item_field = (Item_field *)item_tmp ;

   item_start = para_list.pop() ;

   item_end = para_list.pop() ;

   if ( negated )
   {
      rc = get_item_val( "$lt", item_start,
                         item_field->field, obj_tmp ) ;
      if ( rc )
      {
         goto error ;
      }
      obj_start = BSON( item_field->field_name << obj_tmp ) ;

      rc = get_item_val( "$gt", item_end,
                         item_field->field, obj_tmp ) ;
      if ( rc )
      {
         goto error ;
      }
      obj_end = BSON( item_field->field_name << obj_tmp ) ;

      arr_builder.append( obj_start) ;
      arr_builder.append( obj_end) ;
      obj = BSON( "$or" << arr_builder.arr() ) ;
   }
   else
   {
      rc = get_item_val( "$gte", item_start,
                         item_field->field, obj_tmp ) ;
      if ( rc )
      {
         goto error ;
      }
      obj_start = BSON( item_field->field_name << obj_tmp ) ;

      rc = get_item_val( "$lte", item_end,
                         item_field->field, obj_tmp ) ;
      if ( rc )
      {
         goto error ;
      }
      obj_end = BSON( item_field->field_name << obj_tmp ) ;

      arr_builder.append( obj_start) ;
      arr_builder.append( obj_end) ;
      obj = BSON( "$and" << arr_builder.arr() ) ;
   }

done:
   return rc ;
error:
   if ( SDB_ERR_OVF == rc )
   {
      rc = SDB_ERR_COND_PART_UNSUPPORTED ;
   }
   goto done ;
}

sdb_func_in::sdb_func_in( bool has_not, uint args_num )
   : sdb_func_neg( has_not )
{
   para_num_max = args_num ;
}

sdb_func_in::~sdb_func_in()
{
}

int sdb_func_in::to_bson( bson::BSONObj &obj )
{
   int rc = SDB_ERR_OK ;
   Item_field *item_field = NULL ;
   Item *item_tmp = NULL ;
   bson::BSONArrayBuilder arr_builder ;
   bson::BSONObj obj_tmp ;

   if ( !is_finished || para_list.elements != para_num_max )
   {
      rc = SDB_ERR_COND_INCOMPLETED ;
      goto error ;
   }

   if ( l_child != NULL || r_child != NULL )
   {
      rc = SDB_ERR_COND_UNKOWN_ITEM ;
      goto error ;
   }

   item_tmp = para_list.pop() ;
   if ( Item::FIELD_ITEM != item_tmp->type() )
   {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM ;
      goto error ;
   }
   item_field = (Item_field *)item_tmp ;

   while( !para_list.is_empty() )
   {
      item_tmp = para_list.pop() ;
      rc = get_item_val( "", item_tmp,
                         item_field->field,
                         obj_tmp, &arr_builder ) ;
      if ( rc )
      {
         goto error ;
      }
   }

   if ( negated )
   {
      obj = BSON( item_field->field_name
                  << BSON( "$nin" << arr_builder.arr() ) ) ;
   }
   else
   {
      obj = BSON( item_field->field_name
                  << BSON( "$in" << arr_builder.arr() ) ) ;
   }

done:
   return rc ;
error:
   if ( SDB_ERR_OVF == rc )
   {
      rc = SDB_ERR_COND_PART_UNSUPPORTED ;
   }
   goto done ;
}

