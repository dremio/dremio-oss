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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.concurrent.ThreadSafe;
import org.immutables.value.Value;

@ThreadSafe
public class CatalogAccessStats {

  private final ConcurrentMap<CatalogEntityKey, Stats> stats;

  public CatalogAccessStats() {
    this.stats = new ConcurrentHashMap<>();
  }

  private static String formatVersionContext(CatalogEntityKey key) {
    if (key.getTableVersionContext() != null) {
      return String.format(" [%s] ", key.getTableVersionContext());
    }
    return " ";
  }

  public void add(
      CatalogEntityKey key, long accessTimeMillis, int resolutionCount, DatasetType datasetType) {
    stats.compute(
        key,
        (k, v) ->
            v == null
                ? Stats.builder()
                    .setAccessTimeMillis(accessTimeMillis)
                    .setResolutionCount(resolutionCount)
                    .setDatasetType(datasetType)
                    .build()
                : Stats.builder()
                    .setAccessTimeMillis(v.accessTimeMillis() + accessTimeMillis)
                    .setResolutionCount(v.resolutionCount() + resolutionCount)
                    .setDatasetType(v.datasetType())
                    .build());
  }

  public CatalogAccessStats merge(CatalogAccessStats other) {
    CatalogAccessStats merged = new CatalogAccessStats();
    stats.forEach(
        (k, v) -> merged.add(k, v.accessTimeMillis(), v.resolutionCount(), v.datasetType()));
    other.stats.forEach(
        (k, v) -> merged.add(k, v.accessTimeMillis(), v.resolutionCount(), v.datasetType()));
    return merged;
  }

  public List<UserBitShared.PlanPhaseProfile> toPlanPhaseProfiles() {
    List<UserBitShared.PlanPhaseProfile> phaseProfiles = new ArrayList<>(stats.size() + 1);

    double avgTime = stats.values().stream().mapToLong(Stats::accessTimeMillis).average().orElse(0);
    long totalResolutions = stats.values().stream().mapToInt(Stats::resolutionCount).sum();

    phaseProfiles.add(
        UserBitShared.PlanPhaseProfile.newBuilder()
            .setPhaseName(
                String.format(
                    "Average Catalog Access for %d Total Dataset(s): using %d resolved key(s)",
                    stats.size(), totalResolutions))
            .setDurationMillis((long) avgTime)
            .build());

    Comparator<Map.Entry<CatalogEntityKey, Stats>> orderByTime =
        Comparator.comparingLong(e -> e.getValue().accessTimeMillis());

    stats.entrySet().stream()
        .sorted(orderByTime.reversed())
        .forEach(
            entry -> {
              CatalogEntityKey catalogEntityKey = entry.getKey();
              phaseProfiles.add(
                  UserBitShared.PlanPhaseProfile.newBuilder()
                      .setPhaseName(
                          String.format(
                              "Catalog Access for %s%s(%s): using %d resolved key(s)",
                              PathUtils.constructFullPath(catalogEntityKey.getKeyComponents()),
                              formatVersionContext(catalogEntityKey),
                              entry.getValue().datasetType(),
                              entry.getValue().resolutionCount()))
                      .setDurationMillis(entry.getValue().accessTimeMillis())
                      .build());
            });

    return phaseProfiles;
  }

  @Value.Immutable
  interface Stats {

    static ImmutableStats.Builder builder() {
      return new ImmutableStats.Builder();
    }

    /** Time spent (in milliseconds) accessing a particular table or view by canonical key. */
    long accessTimeMillis();

    /**
     * Number of resolutions for a particular canonical key. For example, a query may reference a
     * table with canonical key source.table but if a context ctx is set in the query or view
     * definition then we need to be able to look up the table as both ctx.source.table and
     * source.table.
     */
    int resolutionCount();

    /** The dataset type for a particular key. */
    DatasetType datasetType();
  }
}
