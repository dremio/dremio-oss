/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.service.coordinator.zk;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;

import com.dremio.service.coordinator.DistributedSemaphore;

class ZkDistributedSemaphore implements DistributedSemaphore{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZkDistributedSemaphore.class);

  private final InterProcessSemaphoreV2 semaphore;

  public ZkDistributedSemaphore(CuratorFramework client, String path, int numberOfLeases) {
    this.semaphore = new InterProcessSemaphoreV2(client, path, numberOfLeases);
  }

  @Override
  public DistributedLease acquire(long time, TimeUnit unit) throws Exception {
    Lease lease = semaphore.acquire(time, unit);
    if(lease != null){
      return new LeaseHolder(lease);
    }else{
      return null;
    }
  }

  private class LeaseHolder implements DistributedLease{
    private Lease lease;

    public LeaseHolder(Lease lease) {
      super();
      this.lease = lease;
    }

    @Override
    public void close() throws Exception {
      lease.close();
    }

  }
}
