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
package com.dremio.telemetry.api.metrics;

import com.codahale.metrics.MetricRegistry;
import com.dremio.common.DeferredException;
import com.dremio.telemetry.api.config.MetricsConfigurator;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Manages which Reporters are configured for the Metrics system. Primarily used with
 * ClasspathScanning to determine at runtime which reporters are available and register them. Will
 * refresh based on provided options to update current configured items as required.
 */
class ReporterManager {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ReporterManager.class);

  private volatile Set<MetricsConfigurator> configurators = Collections.emptySet();
  private final MetricRegistry registry;

  public ReporterManager(MetricRegistry registry) {
    this.registry = registry;
  }

  public synchronized void onChange(Collection<MetricsConfigurator> newConfs) {
    try {
      final Set<MetricsConfigurator> current = new HashSet<>(configurators);
      final Set<MetricsConfigurator> toStart = new HashSet<>();
      final Set<MetricsConfigurator> totalSet = new HashSet<>();
      for (MetricsConfigurator c : newConfs) {
        if (current.remove(c)) {
          totalSet.add(c);
        } else {
          toStart.add(c);
          totalSet.add(c);
        }
      }

      for (MetricsConfigurator c : toStart) {
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

      for (MetricsConfigurator c : current) {
        deferred.suppressingClose(c);
      }

      if (deferred.getException() != null) {
        logger.warn(
            "Failure while closing one or more metric configurations.", deferred.getException());
      }

      if (toStart.size() > 0 || current.size() > 0) {
        logger.info(
            "Found {} new metric configurations. Found {} no longer valid metric configurations and closed.",
            toStart.size(),
            current.size());
      }

      configurators = totalSet;
    } catch (Exception ex) {
      logger.error("Metrics reporter refresh failed.", ex);
    }
  }

  @VisibleForTesting
  Collection<MetricsConfigurator> getParentConfigurators() {
    return Collections.unmodifiableCollection(configurators);
  }
}
