SequoiaSQL - MySQL Storage Engine
========================
The SequoiaSQL - MySQL Storage Engine is a distributed MySQL storage engine. It currently supports SequoiaDB 3.0 as the backend database, and it will be extended to multiple databases such like MongoDB/Redis and etc...
In order to take advantages of scalability and performance, SequoiaSQL - MySQL Storage Engine can be used to replace InnoDB and store user data/index/lob in the backend distributed database.


Building
--------
1. Get boost-1.59.0 and the source code of mysql-5.7.18.
2. Copy the plugin-code to the storage directory.
 ```lang-javascript
   # mkdir -p mysql-5.7.18/storage/sequoiadb
   # cp sequoiasql-mysql/* mysql-5.7.18/storage/sequoiadb
 ```
3. Copy the SequoiaDB C++ driver to the storage directory.
 ```lang-javascript
   # mkdir -p mysql-5.7.18/storage/sequoiadb/sequoiadb
   # cp -r /opt/sequoiadb/include mysql-5.7.18/storage/sequoiadb/sequoiadb
   # mkdir -p mysql-5.7.18/storage/sequoiadb/sequoiadb/lib
   # cp /opt/sequoiadb/lib/libstaticsdbcpp.a mysql-5.7.18/storage/sequoiadb/sequoiadb/lib
 ```
4. Build the plugin
 ```lang-javascript
   # cd mysql-5.7.18
   # cmake . -DWITH_BOOST=../thirdparty/boost/boost_1_59_0/ -DCMAKE_INSTALL_PREFIX=/opt/mysql -DMYSQL_DATADIR=/opt/mysql/data -DCMAKE_BUILD_TYPE=Release
   # make -j 4
   # make install
 ```


License
-------
License under the GPL License, Version 2.0. See LICENSE for more information.
Copyright (c) 2018, SequoiaDB and/or its affiliates. All rights reserved.
