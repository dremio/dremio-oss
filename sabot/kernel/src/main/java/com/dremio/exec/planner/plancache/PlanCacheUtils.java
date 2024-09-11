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
package com.dremio.exec.planner.plancache;

import static com.dremio.exec.planner.physical.PlannerSettings.CURRENT_ICEBERG_DATA_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlanCacheUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PlanCacheUtils.class);

  public static boolean supportPlanCache(
      SqlHandlerConfig config,
      SqlNode sqlNode,
      PlannerCatalog catalog,
      List<SqlOperator> uncacheableFunctions) {
    if (!config.getContext().getPlannerSettings().isPlanCacheEnabled()) {
      LOGGER.debug("Physical plan not cached: Plan cache not enabled.");
      return false;
    }

    for (DremioTable table : catalog.getAllRequestedTables()) {
      if (CatalogUtil.requestedPluginSupportsVersionedTables(
          table.getPath(), config.getContext().getCatalog())) {
        // Versioned tables don't have a mtime - they have snapshot ids.  Since we don't have a way
        // to invalidate
        // cache entries containing versioned datasets, don't allow these plans to enter the cache.
        LOGGER.debug("Physical plan not cached: Query contains a versioned table.");
        return false;
      }
    }

    if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
        sqlNode.toString(), "external_query")) {
      LOGGER.debug("Physical plan not cached: Query contains an external_query.");
      return false;
    } else if (!uncacheableFunctions.isEmpty()) {
      LOGGER.debug(
          String.format(
              "Physical plan not cached: Query contains dynamic or non-deterministic function(s): %s.",
              uncacheableFunctions.stream()
                  .map(SqlOperator::getName)
                  .distinct()
                  .collect(Collectors.joining(", "))));
      return false;
    } else if (config.getMaterializations().isPresent()
        && !config.getMaterializations().get().isMaterializationCacheInitialized()) {
      LOGGER.debug("Physical plan not cached: Materialization cache not initialized.");
      return false;
    } else if (config.getContext().getOptions().getOption(CURRENT_ICEBERG_DATA_ONLY)) {
      // Avoid plan cache if "reflections.planning.current_iceberg_data_only" is specified.
      // Otherwise,
      // A plan cache entry may get created before Reflection Manager is able to detect reflection
      // is
      // outdated, and same query submitted later will use cached plan that includes outdated
      // reflection(s).
      LOGGER.debug(
          "Physical plan not cached: 'reflections.planning.current_iceberg_data_only' option is specified.");
      return false;
    } else {
      return true;
    }
  }

  public static PlanCacheKey generateCacheKey(
      SqlNode sqlNode, RelNode relNode, QueryContext context) {
    Hasher hasher = Hashing.sha256().newHasher();

    hasher
        .putString(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql(), UTF_8)
        .putString(RelOptUtil.toString(relNode), UTF_8)
        .putString(context.getWorkloadType().name(), UTF_8)
        .putString(context.getContextInformation().getCurrentDefaultSchema(), UTF_8);

    if (context.getPlannerSettings().isPlanCacheEnableSecuredUserBasedCaching()) {
      hasher.putString(context.getQueryUserName(), UTF_8);
    }

    hashNonDefaultOptions(hasher, context, null);

    Optional.ofNullable(context.getGroupResourceInformation())
        .ifPresent(
            v -> {
              hasher.putInt(v.getExecutorNodeCount());
              hasher.putLong(v.getAverageExecutorCores(context.getOptions()));
            });

    return new PlanCacheKey(hasher.hash().toString());
  }

  /** Put non-default options into plan cache key hash. */
  public static void hashNonDefaultOptions(
      Hasher hasher, QueryContext context, Set<String> excludeOptionNames) {
    context.getOptions().getNonDefaultOptions().stream()
        // A sanity filter in case an option with default value is put into non-default options
        .filter(optionValue -> !context.getOptions().getDefaultOptions().contains(optionValue))
        .filter(
            optionValue ->
                excludeOptionNames == null || !excludeOptionNames.contains(optionValue.getName()))
        .sorted()
        .forEach(
            (v) -> {
              hasher.putString(v.getName(), UTF_8);
              switch (v.getKind()) {
                case BOOLEAN:
                  hasher.putBoolean(v.getBoolVal());
                  break;
                case DOUBLE:
                  hasher.putDouble(v.getFloatVal());
                  break;
                case LONG:
                  hasher.putLong(v.getNumVal());
                  break;
                case STRING:
                  hasher.putString(v.getStringVal(), UTF_8);
                  break;
                default:
                  throw new AssertionError("Unsupported OptionValue kind: " + v.getKind());
              }
            });
  }
}
