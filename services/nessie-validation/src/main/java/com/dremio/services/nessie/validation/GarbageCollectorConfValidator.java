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
package com.dremio.services.nessie.validation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.projectnessie.model.GarbageCollectorConfig;

/**
 * A utility class to validate Nessie-Rest objects
 */
public final class GarbageCollectorConfValidator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GarbageCollectorConfValidator.class);
  public static final String PER_REF_CUTOFF_POLICIES_ERROR_MESSAGE = "perRefCutoffPolicies is not empty.";
  public static final String DEFAULT_CUTOFF_POLICY_EMPTY_ERROR_MESSAGE = "defaultCutoffPolicy is Empty or Null.";
  public static final String DEFAULT_CUTOFF_POLICY_WITH_COMMITS_ERROR_MESSAGE = "defaultCutoffPolicy is only supported with duration and timestamp.";
  public static final String NEW_FILES_GRACE_PERIOD = "newFilesGracePeriod is less than 24 hours.";

  private GarbageCollectorConfValidator() {
  }

  /**
   * Validate nessie-gc cut-off-policy with dremio constraints.
   * Dremio Vacuum command does not allow the following cut-off-policy:
   * 1. Retain No. of commits
   * 2. PerRefCutOffPolicy.
   * 3. new files grace period duration less than 24 hours.
   */
  public static List<GCInvalidConfig> validateCompliance(GarbageCollectorConfig garbageCollectorConfig) {
    List<GCInvalidConfig> gcConfigStatuses = new ArrayList<>();
    if (garbageCollectorConfig.getPerRefCutoffPolicies() != null && !garbageCollectorConfig.getPerRefCutoffPolicies().isEmpty()) {
      logger.warn(PER_REF_CUTOFF_POLICIES_ERROR_MESSAGE);
      gcConfigStatuses.add(new GCInvalidConfig("PerRefCutoffPolicies", PER_REF_CUTOFF_POLICIES_ERROR_MESSAGE));
    }
    String defaultPolicy = garbageCollectorConfig.getDefaultCutoffPolicy();
    if (defaultPolicy == null || defaultPolicy.isEmpty()) {
      logger.warn(DEFAULT_CUTOFF_POLICY_EMPTY_ERROR_MESSAGE);
      gcConfigStatuses.add(new GCInvalidConfig("DefaultCutoffPolicy", DEFAULT_CUTOFF_POLICY_EMPTY_ERROR_MESSAGE));
    } else {
      try {
        Integer.parseInt(defaultPolicy);
        logger.warn(DEFAULT_CUTOFF_POLICY_WITH_COMMITS_ERROR_MESSAGE);
        gcConfigStatuses.add(new GCInvalidConfig("DefaultCutoffPolicy", DEFAULT_CUTOFF_POLICY_WITH_COMMITS_ERROR_MESSAGE));
      } catch (NumberFormatException nfe) {
        // Ignore this, as it is expected for timestamp/duration.
      }
    }

    //newFilesGracePeriod should be more than 24 hours if present.
    if (garbageCollectorConfig.getNewFilesGracePeriod() != null && System.currentTimeMillis() - Instant.now().minusMillis(garbageCollectorConfig.getNewFilesGracePeriod().toMillis()).toEpochMilli() < 24 * 60 * 59 * 1000) {
      logger.warn(NEW_FILES_GRACE_PERIOD);
      gcConfigStatuses.add(new GCInvalidConfig("NewFilesGracePeriod", NEW_FILES_GRACE_PERIOD));
    }

    return gcConfigStatuses;
  }

  public static class GCInvalidConfig {
    private final String configName;
    private final String errorMessage;

    public GCInvalidConfig(String configName, String errorMessage) {
      this.configName = configName;
      this.errorMessage = errorMessage;
    }

    public String getConfigName() {
      return configName;
    }

    public String getErrorMessage() {
      return errorMessage;
    }
  }
}
