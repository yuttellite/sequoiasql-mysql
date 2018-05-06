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

#include "sdb_util.h"
#include <string.h>

int sdb_parse_table_name( const char * from,
                          char *db_name, int db_name_size,
                          char *table_name, int table_name_size )
{
   int rc = 0, len = 0 ;
   const char *pBegin , *pEnd ;
   pBegin = from + 2 ; //skip "./"
   pEnd = strchr( pBegin, '/' ) ;
   if ( NULL == pEnd )
   {
      rc = -1 ;
      goto error ;
   }
   len = pEnd - pBegin ;
   if ( len >= db_name_size )
   {
      rc = -1 ;
      goto error ;
   }
   memcpy( db_name, pBegin, len ) ;
   db_name[len] = 0 ;

   pBegin = pEnd + 1 ;
   pEnd = strchrnul( pBegin, '/' ) ;
   len = pEnd - pBegin ;
   if ( *pEnd != 0 || len >= table_name_size )
   {
      rc = -1 ;
      goto error ;
   }
   memcpy( table_name, pBegin, len ) ;
   table_name[len] = 0 ;

done:
   return rc ;
error:
   goto done ;
}
