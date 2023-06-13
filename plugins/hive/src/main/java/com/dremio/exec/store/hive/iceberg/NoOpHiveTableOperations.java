/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.hive.iceberg;

import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.FileIO;

import com.dremio.exec.store.hive.HiveClient;
import com.dremio.hive.thrift.TException;

/**
 * Implementation of HiveTableOperations which doesn't acquires lock on the Hive metastore
 * to perform operations.
 * This class is only used for running tests.
 */
public class NoOpHiveTableOperations extends HiveTableOperations {
  public NoOpHiveTableOperations(Configuration conf, HiveClient client, FileIO fileIO, String catalogName, String database, String table) {
    super(conf, client, fileIO, catalogName, database, table);
  }

  @Override
  protected long acquireLock() throws UnknownHostException, TException, InterruptedException {
    // no-op
    return 0L;
  }

  @Override
  protected void doUnlock(long lockId) throws TException {
    //no-op
  }
}
