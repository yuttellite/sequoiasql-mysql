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

#ifndef SDB_CL_PTR__H
#define SDB_CL_PTR__H

class sdb_cl_auto_ptr ;
class sdb_cl ;
class sdb_cl_ref_ptr
{
public:

   friend class sdb_cl_auto_ptr ;

protected:

   sdb_cl_ref_ptr( sdb_cl *collection ) ;

   virtual ~sdb_cl_ref_ptr() ;

protected:
   sdb_cl                           *sdb_collection ;
   Atomic_int32                     ref ; 
} ;

class sdb_cl_auto_ptr
{
public:

   sdb_cl_auto_ptr() ;

   virtual ~sdb_cl_auto_ptr() ;

   sdb_cl_auto_ptr( sdb_cl *collection ) ;

   sdb_cl_auto_ptr( const sdb_cl_auto_ptr &other ) ;

   sdb_cl_auto_ptr & operator = ( sdb_cl_auto_ptr &other ) ;

   sdb_cl& operator *() ;

   sdb_cl* operator ->() ;

   int ref() ;

   void clear() ;

private:
   sdb_cl_ref_ptr                   *ref_ptr ;
} ;
#endif
