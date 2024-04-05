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
package com.dremio.exec.server;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.CoordinationProtos.Roles;
import com.dremio.exec.work.SafeExit;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.sabot.exec.FragmentWorkManager;
import com.dremio.service.Service;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.RegistrationHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import javax.inject.Inject;
import javax.inject.Provider;

/** Register node to cluster. */
public class NodeRegistration implements Service {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(NodeRegistration.class);

  private final Provider<FragmentWorkManager> fragmentManager;
  private final Provider<ForemenWorkManager> foremenManager;
  private final Provider<ClusterCoordinator> coord;
  private final Provider<NodeEndpoint> nodeEndpoint;
  private final Provider<DremioConfig> dremioConfig;
  private final List<RegistrationHandle> registrationHandles = new ArrayList<>();

  private String endpointName;
  private volatile boolean closed = false;

  @Inject
  public NodeRegistration(
      Provider<NodeEndpoint> nodeEndpoint,
      Provider<FragmentWorkManager> fragmentManager,
      Provider<ForemenWorkManager> foremenManager,
      Provider<ClusterCoordinator> coord,
      Provider<DremioConfig> dremioConfig) {
    this.nodeEndpoint = nodeEndpoint;
    this.fragmentManager = fragmentManager;
    this.foremenManager = foremenManager;
    this.coord = coord;
    this.dremioConfig = dremioConfig;
  }

  @Override
  public void start() throws Exception {
    final NodeEndpoint endpoint = nodeEndpoint.get();
    endpointName = endpoint.getAddress() + ":" + endpoint.getUserPort();
    logger.info("Starting NodeRegistration for {}", endpointName);
    Roles roles = endpoint.getRoles();
    if (roles.getMaster()) {
      registrationHandles.add(
          coord.get().getServiceSet(ClusterCoordinator.Role.MASTER).register(endpoint));
    }
    if (roles.getSqlQuery()) {
      registrationHandles.add(
          coord.get().getServiceSet(ClusterCoordinator.Role.COORDINATOR).register(endpoint));
    }
    if (roles.getJavaExecutor()) {
      registrationHandles.add(
          coord.get().getServiceSet(ClusterCoordinator.Role.EXECUTOR).register(endpoint));
    }
    logger.info("NodeRegistration is up for {}", endpointName);
  }

  @Override
  public synchronized void close() throws Exception {
    if (!closed) {
      closed = true;
      logger.info("Waiting for work to complete before shutdown.");
      ThreadFactory threadFactory = new NamedThreadFactory("noderegistration-shutdown-");
      Thread t1 = waitToExit(threadFactory, fragmentManager);
      Thread t2 = waitToExit(threadFactory, foremenManager);

      t1.start();
      t2.start();

      t1.join();
      t2.join();

      logger.info("Unregistering node {}", endpointName);
      if (!registrationHandles.isEmpty()) {
        Thread t =
            threadFactory.newThread(
                new Runnable() {
                  @Override
                  public void run() {
                    try {
                      AutoCloseables.close(registrationHandles);
                    } catch (Exception e) {
                      logger.warn("Exception while closing registration handle", e);
                    }
                  }
                });

        t.start();
        try {
          t.join(dremioConfig.get().getSabotConfig().getInt(ExecConstants.ZK_REFRESH) * 2);
          if (t.isAlive()) {
            logger.warn("Timeout expired while trying to unregister node");
          }
        } catch (final InterruptedException e) {
          logger.warn("Interrupted while sleeping during coordination deregistration.");
          // Preserve evidence that the interruption occurred so that code higher up on the call
          // stack can learn of the
          // interruption and respond to it if it wants to.
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private Thread waitToExit(
      ThreadFactory threadFactory, final Provider<? extends SafeExit> provider) {
    return threadFactory.newThread(
        new Runnable() {
          @Override
          public void run() {
            SafeExit safeExit;
            try {
              safeExit = provider.get();
            } catch (Exception ex) {
              // ignore since this means no instance wasn't running on this node.
              return;
            }
            safeExit.waitToExit();
          }
        });
  }
}
