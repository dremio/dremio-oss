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
package com.dremio.exec.store.hive;

import com.dremio.common.util.Closeable;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.pool2.impl.GenericObjectPool;

/**
 * Wrapper over HiveClient to ensure proper lifecycle management with and without object pools.
 * Non-final for testing purposes.
 */
@SuppressWarnings("FinalClass")
public class ManagedHiveClient implements Closeable {

  private final GenericObjectPool<HiveClient> pool;
  private final HiveClient hiveClient;
  private boolean isClosed = false;

  private ManagedHiveClient(GenericObjectPool<HiveClient> pool, HiveClient hiveClient) {
    this.pool = pool;
    this.hiveClient = hiveClient;
  }

  public static ManagedHiveClient wrapClient(HiveClient hiveClient) {
    return new ManagedHiveClient(null, hiveClient);
  }

  public static ManagedHiveClient wrapClientFromPool(GenericObjectPool<HiveClient> pool) {
    try {
      return new ManagedHiveClient(pool, pool.borrowObject());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() {
    if (pool != null) {
      pool.returnObject(hiveClient);
      isClosed = true;
    }
  }

  public HiveClient client() {
    Preconditions.checkState(!isClosed);
    return hiveClient;
  }
}
