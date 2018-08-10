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

#ifndef SDB_UTIL__H
#define SDB_UTIL__H

#include <pthread.h>
#include <time.h>
#include "sql_class.h"
#include <mysql/psi/mysql_file.h>

int sdb_parse_table_name( const char * from,
                          char *db_name, int db_name_size,
                          char *table_name, int table_name_size ) ;

int sdb_get_db_name_from_path( const char * path,
                               char *db_name, int db_name_size ) ;

int sdb_convert_charset( const String &src_str, String &dst_str, 
                         const CHARSET_INFO *dst_charset ) ;

class sdb_lock_time_out
{
public:

   ~sdb_lock_time_out(){}

   static sdb_lock_time_out *get_instance()
   {
      static sdb_lock_time_out _time_out ;
      return &_time_out ;
   }

   const struct timespec *get_time()
   {
      return &time_out ;
   }

private:
   sdb_lock_time_out()
   {
      clock_gettime( CLOCK_REALTIME, &time_out ) ;
      time_out.tv_sec += 3600*24*365*10 ;
   }
   sdb_lock_time_out( const sdb_lock_time_out &rh ){}
   sdb_lock_time_out & operator = ( const sdb_lock_time_out & rh)
   {
      return *this ;
   }

private:
   struct timespec                  time_out ;
};

#define SDB_LOCK_TIMEOUT sdb_lock_time_out::get_instance()->get_time()

class sdb_rw_lock_r
{
private:
   pthread_rwlock_t*      rw_mutex ;

public:
   sdb_rw_lock_r( pthread_rwlock_t *var_lock )
      :rw_mutex(NULL)
   {
      if ( var_lock )
      {
         while( TRUE )
         {
            int rc = pthread_rwlock_timedrdlock( var_lock,
                                                 SDB_LOCK_TIMEOUT ) ;
            if ( 0 == rc )
            {
               rw_mutex = var_lock ;
            }
            else if ( EDEADLK != rc )
            {
               continue ;
            }
            else
            {
               assert( FALSE ) ;
            }
            break ;
         }
      }
   }

   ~sdb_rw_lock_r()
   {
      if ( rw_mutex )
      {
         pthread_rwlock_unlock( rw_mutex ) ;
      }
   }
};

class sdb_rw_lock_w
{
private:
   pthread_rwlock_t*      rw_mutex ;

public:
   sdb_rw_lock_w( pthread_rwlock_t *var_lock )
      :rw_mutex(NULL)
   {
      if ( var_lock )
      {
         while( TRUE )
         {
            int rc = pthread_rwlock_timedwrlock( var_lock,
                                                 SDB_LOCK_TIMEOUT ) ;
            if ( 0 == rc )
            {
               rw_mutex = var_lock ;
            }
            else if ( EDEADLK != rc )
            {
               continue ;
            }
            else
            {
               assert( FALSE ) ;
            }
            break ;
         }
      }
   }

   ~sdb_rw_lock_w()
   {
      if ( rw_mutex )
      {
         pthread_rwlock_unlock( this->rw_mutex ) ;
      }
   }
};

#endif
