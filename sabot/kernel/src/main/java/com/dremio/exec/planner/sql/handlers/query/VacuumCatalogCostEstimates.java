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
package com.dremio.exec.planner.sql.handlers.query;

import static com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates.ESTIMATED_RECORDS_PER_MANIFEST;

import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.Optional;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.FetchOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VacuumCatalogCostEstimates {
  private static final Logger LOGGER = LoggerFactory.getLogger(VacuumCatalogCostEstimates.class);
  private static final long DEFAULT_COMMIT_ESTIMATE_PER_BRANCH = 10;
  private static final long DEFAULT_COMMIT_ESTIMATE = 1_000_000;

  @VisibleForTesting static final long DEFAULT_MANIFESTS_PER_SNAPSHOT = 2;

  private VacuumCatalogCostEstimates() {
    // not to be instantiated
  }

  /**
   * Attempts at computing cost for each different layer in the VACUUM plan. In the current state,
   * Nessie server doesn't keep enough stats (pending DX-67285).
   *
   * <p>Hence, the strategy for now is to assume liberal defaults for iceberg level files, and
   * multiply them with the number of commits; with an assumption that there is generally 1 snapshot
   * per commit.
   *
   * <p>Since the VACUUM command only parses through the live commits, the code tries to factor-in
   * the cut-off time by assuming commits are equally distributed across the time-frame. Thereby
   * dividing the number of commits per branch in the same ratio as window for liveness.
   *
   * <p>The retain last configuration is honoured in case estimated live commits are less than the
   * value.
   */
  public static IcebergCostEstimates find(NessieApiV2 nessieApi, VacuumOptions vacuumOptions) {
    long snapshotsCount;

    try {
      Instant initialTimestamp = nessieApi.getConfig().getOldestPossibleCommitTimestamp();
      snapshotsCount =
          nessieApi.getAllReferences().fetch(FetchOption.ALL).get().getReferences().stream()
              .mapToLong(
                  ref ->
                      Optional.ofNullable(ref.getMetadata())
                          .map(
                              referenceMetadata -> {
                                // Assuming commits are evenly divided across the time.
                                // Also assuming maximum size of the branch to keep the estimates
                                // liberal.
                                Instant headTimestamp =
                                    referenceMetadata.getCommitMetaOfHEAD().getCommitTime();

                                if (initialTimestamp.toEpochMilli()
                                    > vacuumOptions.getOlderThanInMillis()) {
                                  // All commits are live because initial timestamp is after the
                                  // cut-off
                                  return referenceMetadata.getNumTotalCommits();
                                }

                                long cutOffWindow =
                                    headTimestamp.toEpochMilli()
                                        - vacuumOptions.getOlderThanInMillis();
                                long totalTimeWindow =
                                    headTimestamp.toEpochMilli() - initialTimestamp.toEpochMilli();
                                long estimatedLiveCommits =
                                    (cutOffWindow
                                        * referenceMetadata.getNumTotalCommits()
                                        / totalTimeWindow);

                                return Math.max(
                                    estimatedLiveCommits, vacuumOptions.getRetainLast());
                              })
                          .orElse(DEFAULT_COMMIT_ESTIMATE_PER_BRANCH))
              .sum();
    } catch (Exception e) {
      LOGGER.warn("Error while querying Nessie for estimations", e);
      snapshotsCount = DEFAULT_COMMIT_ESTIMATE;
    }
    long tablesCount = snapshotsCount;
    long datafileEstimatedCount = snapshotsCount * ESTIMATED_RECORDS_PER_MANIFEST;
    long manifestFileEstimatedCount = DEFAULT_MANIFESTS_PER_SNAPSHOT * snapshotsCount;

    return new IcebergCostEstimates(
        tablesCount, snapshotsCount, datafileEstimatedCount, manifestFileEstimatedCount);
  }
}
