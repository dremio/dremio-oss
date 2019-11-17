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
package com.dremio.metrics;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.inject.Provider;

import com.codahale.metrics.MetricRegistry;
import com.dremio.common.DeferredException;
import com.google.common.annotations.VisibleForTesting;

/**
 * Manages which Reporters are configured for the Metrics system. Primarily used with ClasspathScanning to determine at
 * runtime which reporters are available and register them. Will refresh based on provided options to update current
 * configured items as required.
 */
class ReporterManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReporterManager.class);

  private volatile Long refreshMillis;
  private volatile Provider<Collection<ParentConfigurator>> configurationProvider;
  private volatile Set<ParentConfigurator> configurators = Collections.emptySet();
  private volatile Thread refreshThread = new Thread(() -> refreshContinually(), "metrics-config-refresh");
  private final MetricRegistry registry;

  public ReporterManager(MetricRegistry registry) {
    this.registry = registry;
  }

  synchronized void startIfNot(long refreshMillis, Provider<Collection<ParentConfigurator>> configurationProvider) {
    try {
      if(this.configurationProvider != null) {
        logger.debug("Ignoring request to start as already started.");
        return;
      }
      this.configurationProvider = configurationProvider;
      this.refreshMillis = refreshMillis;
      refreshOnce();
      refreshThread.setDaemon(true);
      refreshThread.start();
    } catch (Exception ex) {
      logger.error("Failure while attempting to start metrics system.", ex);
    }
  }

  private void refreshContinually() {
    try {
      while(true) {
        Thread.sleep(refreshMillis);
        refreshOnce();
      }
    } catch (InterruptedException ex) {
      logger.info("Refresh thread interrupted, exiting.", ex);
      return;
    }
  }

  private void refreshOnce() {
    try {
      final Collection<ParentConfigurator> latestConfig = configurationProvider.get();
      final Set<ParentConfigurator> current = new HashSet<>(configurators);
      final Set<ParentConfigurator> toStart = new HashSet<>();
      final Set<ParentConfigurator> totalSet = new HashSet<>();
      for(ParentConfigurator c : latestConfig) {
        if (current.remove(c)) {
          totalSet.add(c);
        } else {
          toStart.add(c);
          totalSet.add(c);
        }
      }

      for(ParentConfigurator c : toStart) {
        try {
          c.start(registry);
        } catch (Exception ex) {
          logger.error("Failure while starting configuration {} metric reporter.", c.getName(), ex);

          // don't save this as something running. instead, try again on next refresh.
          totalSet.remove(c);
        }
      }

      @SuppressWarnings("resource")
      final DeferredException deferred = new DeferredException();

      for(ParentConfigurator c : current) {
        deferred.suppressingClose(c);
      }

      if(deferred.getException() != null) {
        logger.warn("Failure while closing one or more metric configurations.", deferred.getException());
      }

      if(toStart.size() > 0 || current.size() > 0) {
        logger.info("Found {} new metric configurations. Found {} no longer valid metric configurations and closed.", toStart.size(), current.size());
      }

      configurators = totalSet;
    } catch (Exception ex) {
      logger.error("Metrics reporter refresh failed.", ex);
    }
  }

  @VisibleForTesting
  Collection<ParentConfigurator> getParentConfigurators() {
    return Collections.unmodifiableCollection(configurators);
  }
}
