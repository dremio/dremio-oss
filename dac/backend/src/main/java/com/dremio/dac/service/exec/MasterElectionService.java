/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.service.exec;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.CatastrophicFailure;
import com.dremio.service.Service;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ElectionListener;
import com.dremio.service.coordinator.ServiceSet.RegistrationHandle;
import com.google.common.base.Preconditions;

/**
 * Maintains status of master node using zookeeper.
 */
public class MasterElectionService implements Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MasterElectionService.class);

  private static final long INITIAL_WAIT_TIME_MILLIS = TimeUnit.SECONDS.toMillis(5);

  private final Provider<ClusterCoordinator> clusterCoordinator;

  private RegistrationHandle registrationHandle = null;

  public MasterElectionService(Provider<ClusterCoordinator> clusterCoordinator) {
    this.clusterCoordinator = Preconditions.checkNotNull(clusterCoordinator);
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting MasterElectionService");
    final CountDownLatch elected = new CountDownLatch(1);
    registrationHandle = clusterCoordinator.get().joinElection("master", new ElectionListener() {

      @Override
      public void onElected() {
        elected.countDown();
      }

      @Override
      public void onCancelled() {
        // Connection has been lost with ZooKeeper or election has been cancelled
        // Let's abort
        abort();
      }
    });

    if (!elected.await(INITIAL_WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS)) {
      logger.info("Waiting until being elected as master node");
      elected.await();
    }
    logger.info("Elected as master node.");
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(registrationHandle);
  }

  protected void abort() {
    CatastrophicFailure.exit("Node lost its master status. Exiting", 3);
  }
}
