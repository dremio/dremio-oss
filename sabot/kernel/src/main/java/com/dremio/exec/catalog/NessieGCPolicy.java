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
package com.dremio.exec.catalog;



import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.rest.NessieServiceException;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.model.GarbageCollectorConfig;
import org.projectnessie.model.RepositoryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.services.nessie.validation.GarbageCollectorConfValidator;

/**
 * Parse defaultCutoffPolicy from Nessie GC config.
 * Nessie validates the string of defaultCutoffPolicy before saving it.
 * We can expect nessie is always returning expected format.
 */
public class NessieGCPolicy {

  private static final Logger logger = LoggerFactory.getLogger(NessieGCPolicy.class);
  private long olderThanInMillis;
  private Long gracePeriodInMillis = 0L;
  private boolean isDefaultRetention = false;

  public NessieGCPolicy(NessieApiV2 nessieApi, long defaultRetentionMins) {
    Optional<GarbageCollectorConfig> gcConfig = Optional.empty();
    try {
      gcConfig =  nessieApi
        .getRepositoryConfig()
        .type(RepositoryConfig.Type.GARBAGE_COLLECTOR)
        .get()
        .getConfigs()
        .stream()
        .filter(c -> c instanceof GarbageCollectorConfig)
        .map(GarbageCollectorConfig.class::cast)
        .findAny();
    } catch (NessieServiceException | NessieBadRequestException e) {
      logger.error("Could not retrieve vacuum repository configuration, using defaults", e);
    }

    if (gcConfig.isPresent()) {
      GarbageCollectorConfig garbageCollectorConfig = gcConfig.get();
      String defaultPolicy = garbageCollectorConfig.getDefaultCutoffPolicy();
      List<GarbageCollectorConfValidator.GCInvalidConfig> gcInvalidConfigs = GarbageCollectorConfValidator.validateCompliance(garbageCollectorConfig);
      if (!gcInvalidConfigs.isEmpty()) {
        AtomicInteger count = new AtomicInteger();
        String errorMessage = "VACUUM command is not allowed with the following Nessie Garbage Collector Configurations: "
          + gcInvalidConfigs.stream().map(c -> count.incrementAndGet()+") "+c.getErrorMessage()).collect(Collectors.joining());
        throw UserException.validationError().message(errorMessage).buildSilently();
      }
      if ("NONE".equalsIgnoreCase(defaultPolicy)) {
        olderThanInMillis = 0L; //If it's NONE, then it should be NO-OP.
      } else {
        this.gracePeriodInMillis = garbageCollectorConfig.getNewFilesGracePeriod() == null ? 0L : garbageCollectorConfig.getNewFilesGracePeriod().toMillis();
        try {
          Duration duration = Duration.parse(defaultPolicy);
          olderThanInMillis = Instant.now().minusMillis(duration.toMillis()).toEpochMilli();
        } catch (DateTimeException dte) {
          olderThanInMillis = ZonedDateTime.parse(defaultPolicy).toInstant().toEpochMilli();
        }
      }
      isDefaultRetention = false;
    } else {
      this.olderThanInMillis = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(defaultRetentionMins);
      logger.warn("Nessie GC has been not configured. Vacuum is using default value {} from system option {} ",
        this.olderThanInMillis, "dremio.iceberg.vacuum.catalog.default_retention.mins");
      isDefaultRetention = true;
    }
  }

  public long getOlderThanInMillis() {
    return olderThanInMillis;
  }

  public int getRetainLast() {
      return 1;
  }

  public Long getGracePeriodInMillis() {
    return gracePeriodInMillis;
  }

  public boolean isDefaultRetention() {
    return isDefaultRetention;
  }
}
