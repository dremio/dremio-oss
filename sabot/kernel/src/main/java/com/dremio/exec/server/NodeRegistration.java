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
package com.dremio.exec.server;


import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.CoordinationProtos.Roles;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.sabot.exec.FragmentWorkManager;
import com.dremio.service.Service;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ServiceSet.RegistrationHandle;

/**
 * Register node to cluster.
 */
public class NodeRegistration implements Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NodeRegistration.class);

  private final Provider<FragmentWorkManager> fragmentManager;
  private final Provider<ForemenWorkManager> foremenManager;
  private final Provider<ClusterCoordinator> coord;
  private final Provider<SabotContext> context;
  private final List<RegistrationHandle> registrationHandles = new ArrayList<>();;

  private String endpointName;
  private volatile boolean closed = false;

  public NodeRegistration(Provider<SabotContext> context, Provider<FragmentWorkManager> fragmentManager, Provider<ForemenWorkManager> foremenManager, Provider<ClusterCoordinator> coord) {
    this.context = context;
    this.fragmentManager = fragmentManager;
    this.foremenManager = foremenManager;
    this.coord = coord;
  }

  @Override
  public void start() throws Exception {
    final NodeEndpoint endpoint = context.get().getEndpoint();
    endpointName = endpoint.getAddress() + ":" + endpoint.getUserPort();
    logger.info("Starting NodeRegistration for {}", endpointName);
    Roles roles = endpoint.getRoles();
    if (roles.getSqlQuery()) {
      registrationHandles.add(coord.get().getServiceSet(ClusterCoordinator.Role.COORDINATOR).register(endpoint));
    }
    if (roles.getJavaExecutor()) {
      registrationHandles.add(coord.get().getServiceSet(ClusterCoordinator.Role.EXECUTOR).register(endpoint));
    }
    logger.info("NodeRegistration is up for {}", endpointName);
  }

  @Override
  public synchronized void close() throws Exception {
    if (!closed) {
      closed = true;
      logger.info("Waiting for work to complete before shutdown.");
      final Thread t1 = new Thread(){
        @Override
        public void run() {
          FragmentWorkManager frag;
          try {
            frag = fragmentManager.get();
          } catch (Exception ex){
            // ignore since this means the fragment manager wasn't running on this node.
            return;
          }
          frag.waitToExit();
        }
      };

      final Thread t2 = new Thread(){
        @Override
        public void run() {
          ForemenWorkManager fore;
          try{
            fore = foremenManager.get();
          } catch (Exception ex){
            // ignore since this means the fragment manager wasn't running on this node.
            return;
          }
          fore.waitToExit();
        }
      };

      t1.start();
      t2.start();
      t1.join();
      t2.join();

      logger.info("Unregistering node {}", endpointName);
      if (!registrationHandles.isEmpty()) {
        AutoCloseables.close(registrationHandles);

        try {
          Thread.sleep(context.get().getConfig().getInt(ExecConstants.ZK_REFRESH) * 2);
        } catch (final InterruptedException e) {
          logger.warn("Interrupted while sleeping during coordination deregistration.");
          // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
          // interruption and respond to it if it wants to.
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
