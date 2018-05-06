SequoiaDB Storage Engine
========================
The SequoiaDB Storage Engine Project provides support for SequoiaDB community within the MySQL 5.7.18. It is a plugin for the Mysql which use SequoiaDB as storage engine.


Building
--------
1. Get boost-1.59.0 and the source code of mysql-5.7.18 before building.
2. Copy the plugin-code to the folder of mysql-5.7.18.
   # mkdir -p mysql-5.7.18/storage/sequoiadb
   # cp sequoiasql-mysql/* mysql-5.7.18/storage/sequoiadb
3. Copy the lib and include file of SequoiaDB C++ driver to the folder of mysql-5.7.18.
   # mkdir -p mysql-5.7.18/storage/sequoiadb/sequoiadb
   # cp -r /opt/sequoiadb/include mysql-5.7.18/storage/sequoiadb/sequoiadb
   # mkdir -p mysql-5.7.18/storage/sequoiadb/sequoiadb/lib
   # cp /opt/sequoiadb/lib/libstaticsdbcpp.a mysql-5.7.18/storage/sequoiadb/sequoiadb/lib
4. Build the plugin
		# cd mysql-5.7.18
		# cmake . -DWITH_BOOST=../thirdparty/boost/boost_1_59_0/ -DCMAKE_INSTALL_PREFIX=/opt/mysql -DMYSQL_DATADIR=/opt/mysql/data -DCMAKE_BUILD_TYPE=Release
		# make -j 4
		# make install


License
-------
License under the GPL License, Version 2.0. See LICENSE for more information.
Copyright (c) 2018, SequoiaDB and/or its affiliates. All rights reserved.