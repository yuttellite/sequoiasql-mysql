# Copyright (c) 2006, 2016, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA



# Usage: cmake -DSDB_BUILT_IN=ON -DWITH_SDB_DRIVER=/opt/sequoiadb
#
# [SDB_BUILT_IN]: compile as built-in plugin or module plugin.
# [WITH_SDB_DRIVER]: the path to SequoiaDB cpp driver, which must 
#     contain include/ and lib/libsdbcpp.so

IF(SDB_VER)
  ADD_DEFINITIONS(-DSDB_VER=\"${SDB_VER}\")
ENDIF()
IF(CMAKE_BUILD_TYPE MATCHES "Debug")
  ADD_DEFINITIONS(-DDEBUG)
ENDIF()
ADD_DEFINITIONS(-Wno-error)
 
SET(SEQUOIADB_SOURCES
ha_sdb.cc
sdb_conf.cc
sdb_util.cc
sdb_conn.cc
sdb_condition.cc
sdb_item.cc
sdb_cl.cc
sdb_adaptor.cc
sdb_err_code.cc
sdb_log.cc
sdb_idx.cc)
   
SET(WITH_SDB_DRIVER "" CACHE PATH "Path to SequoiaDB cpp driver")
SET(SDB_DRIVER_PATH ${WITH_SDB_DRIVER})

FIND_PATH(SDB_INCLUDE_DIR
  NAMES client.hpp
  HINTS ${SDB_DRIVER_PATH}/include
)

IF(${SDB_INCLUDE_DIR} STREQUAL "SDB_INCLUDE_DIR-NOTFOUND")
  MESSAGE(SEND_ERROR
    "Cannot find appropriate headers of SequoiaDB cpp driver from ${SDB_DRIVER_PATH}")
ENDIF()
MESSAGE(STATUS "SDB_INCLUDE_DIR = ${SDB_INCLUDE_DIR}")

INCLUDE_DIRECTORIES(
  ${SDB_INCLUDE_DIR}
)

FIND_LIBRARY(SDB_LIBRARY
  NAMES libsdbcpp.so
  HINTS ${SDB_DRIVER_PATH}/lib
)

IF(${SDB_LIBRARY} STREQUAL "SDB_LIBRARY-NOTFOUND")
  MESSAGE(SEND_ERROR
    "Cannot find libsdbcpp.so from ${SDB_DRIVER_PATH}")
ENDIF()
MESSAGE(STATUS "SDB_LIBRARY = ${SDB_LIBRARY}")

SET(SDB_BUILT_IN ON CACHE BOOL "Build SequoiaDB storage engine in mysqld")
MARK_AS_ADVANCED(SDB_BUILT_IN)
SET(SDB_LDFLAGS "-Wl,-rpath,${CMAKE_INSTALL_PREFIX}/lib")

IF(SDB_BUILT_IN)
  MYSQL_ADD_PLUGIN(sequoiadb ${SEQUOIADB_SOURCES} STORAGE_ENGINE MANDATORY RECOMPILE_FOR_EMBEDDED
      LINK_LIBRARIES ${SDB_LIBRARY})

  STRING(FIND "${WITH_MYSQLD_LDFLAGS}" "${SDB_LDFLAGS}" POS)
  IF(${POS} EQUAL -1)
    SET(WITH_MYSQLD_LDFLAGS "${WITH_MYSQLD_LDFLAGS} ${SDB_LDFLAGS}" 
        CACHE STRING "Additional linker flags for mysqld" FORCE)
  ENDIF()
ELSE()
  MYSQL_ADD_PLUGIN(sequoiadb ${SEQUOIADB_SOURCES} STORAGE_ENGINE MODULE_ONLY
      LINK_LIBRARIES ${SDB_LIBRARY})
  SET_TARGET_PROPERTIES(sequoiadb PROPERTIES LINK_FLAGS ${SDB_LDFLAGS})
ENDIF()
 
INSTALL(FILES ${SDB_LIBRARY} DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)

