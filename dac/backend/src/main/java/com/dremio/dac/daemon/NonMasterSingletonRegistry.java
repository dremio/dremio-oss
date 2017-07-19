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
package com.dremio.dac.daemon;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.dac.service.exec.MasterStatusListener;
import com.dremio.datastore.DatastoreFatalException;
import com.dremio.exec.rpc.ConnectionFailedException;
import com.dremio.exec.rpc.RpcException;
import com.dremio.service.Service;
import com.dremio.service.SingletonRegistry;
import com.google.common.collect.Lists;

/**
 * Service registry which waits for master in case of remote exception.
 */
public class NonMasterSingletonRegistry extends SingletonRegistry {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NonMasterSingletonRegistry.class);

  private final List<Service> servicesStarted = new ArrayList<>();
  private final Provider<MasterStatusListener> masterStatusListener;

  public NonMasterSingletonRegistry(Provider<MasterStatusListener> masterStatusListener) {
    this.masterStatusListener = masterStatusListener;
  }

  @Override
  public void start() throws Exception {
    final MasterStatusListener masterStatusListener = this.masterStatusListener.get();
    for(Service service : getServices()) {
      if (!masterStatusListener.isMasterUp()) {
        masterStatusListener.waitForMaster();
      }
      while (true) {
        try {
          service.start();
          servicesStarted.add(service);
          break;
        } catch (ConnectionFailedException connectionFailedException) {
          logger.error("Service {} failed to start due to connection failure, waiting for master", service.getClass().getName(), connectionFailedException);
          masterStatusListener.waitForMaster();
        } catch (RpcException rpcException) {
          logger.error("Service {} failed to start", service.getClass().getName(), rpcException);
          throw rpcException;
        } catch (DatastoreFatalException remoteException) {
          logger.error("Service {} failed to start", service.getClass().getName(), remoteException);
          if (remoteException.getCause() instanceof ConnectionFailedException) {
            masterStatusListener.waitForMaster();
          } else {
            throw remoteException;
          }
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(Lists.reverse(servicesStarted));
  }
}
