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
package com.dremio.exec.work.user;

import static com.dremio.exec.server.options.OptionValue.createBoolean;
import static com.dremio.exec.server.options.OptionValue.createLong;
import static com.dremio.exec.server.options.OptionValue.createString;
import static com.dremio.exec.server.options.OptionValue.OptionType.QUERY;

import java.util.List;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.observer.QueryObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.server.options.OptionManager;
import com.google.common.base.Preconditions;

/**
 * Will submit a query locally without going through the client
 */
public interface LocalQueryExecutor {

  /**
   * Will submit a query locally without going through the client.
   * @param observer QueryObserver used to get notifications about the queryJob.
   *                    Overrides the use of QueryObserverFactory defined in the context
   * @param query the query definition
   * @param prepare whether this is a prepared statement
   * @param config local execution config
   */
  void submitLocalQuery(
      ExternalId externalId,
      QueryObserver observer,
      Object query,
      boolean prepare,
      LocalExecutionConfig config);

  /**
   * Cancel a locally running query.
   * @param externalId QueryId of the query to cancel.
   */
  void cancelLocalQuery(ExternalId externalId);

  /**
   * settings to modify local query execution
   */
  class LocalExecutionConfig implements OptionProvider {
    private final boolean enableLeafLimits;
    private final long maxQueryWidth;
    private final boolean allowPartitionPruning;
    private final boolean failIfNonEmptySent;
    private final String username;
    private final List<String> sqlContext;
    private final boolean storeQueryResults;
    private final boolean internalSingleThreaded;
    private final String queryResultsStorePath;
    private final SubstitutionSettings materializationSettings;
    private final boolean exposeInternalSources;

    /**
     * @param enableLeafLimits to reduce the size of the input of a query
     * @param maxQueryWidth maximum query paralleization width. Pass non-positive number to consider the system option.
     * @param failIfNonEmptySent whether to fail the query rather than reattempting when some data has
     *                           already been sent to client. Applicable only when reattempt is possible.
     * @param username current user
     * @param sqlContext default schema to 'use' when querying
     * @param storeQueryResults to store the query results instead of returning. When enabled metadata about the stored
     *                          results is returned as job results. Actual job results are stored in given table
     *                          path <code>queryResultsStorePath</code>
     * @param internalSingleThreaded if query should be run as single threaded (used for internal queries)
     * @param queryResultsStorePath table path where to store the query results.
     *                              Must be non-null when <code>storeQueryResults</code> is true.
     * @param allowPartitionPruning if partition pruning is allowed
     * @param exposeInternalSources if internal schemas should be exposed (used for internal queries)
     * @param materializationsettings settings related to materialization.
     */
    public LocalExecutionConfig(final boolean enableLeafLimits, final long maxQueryWidth,
                                final boolean failIfNonEmptySent, final String username, final List<String> sqlContext,
                                final boolean storeQueryResults, final boolean internalSingleThreaded, final String queryResultsStorePath,
                                final boolean allowPartitionPruning, final boolean exposeInternalSources,
                                final SubstitutionSettings materializationSettings) {
      this.enableLeafLimits = enableLeafLimits;
      this.maxQueryWidth = maxQueryWidth;
      this.failIfNonEmptySent = failIfNonEmptySent;
      this.username = username;
      this.sqlContext = sqlContext;
      this.storeQueryResults = storeQueryResults;
      this.internalSingleThreaded = internalSingleThreaded;
      Preconditions.checkArgument(!storeQueryResults || (storeQueryResults && queryResultsStorePath != null));
      this.queryResultsStorePath = queryResultsStorePath;
      this.materializationSettings = materializationSettings == null ? SubstitutionSettings.of() : materializationSettings;
      this.allowPartitionPruning = allowPartitionPruning;
      this.exposeInternalSources = exposeInternalSources;
    }

    public String getUsername() {
      return username;
    }

    public boolean exposeInternalSources() {
      return exposeInternalSources;
    }

    public List<String> getSqlContext() {
      return sqlContext;
    }

    public SubstitutionSettings getMaterializationSettings() {
      return materializationSettings;
    }

    public boolean isFailIfNonEmptySent() {
      return failIfNonEmptySent;
    }

    public void applyOptions(OptionManager manager){
      if(enableLeafLimits){
        manager.setOption(createBoolean(QUERY, PlannerSettings.ENABLE_LEAF_LIMITS.getOptionName(), true));
      }

      if (maxQueryWidth > 0L) {
        manager.setOption(createLong(QUERY, ExecConstants.MAX_WIDTH_GLOBAL_KEY, maxQueryWidth));
      }

      if (storeQueryResults) {
        manager.setOption(createBoolean(QUERY, PlannerSettings.STORE_QUERY_RESULTS.getOptionName(), true));
        manager.setOption(createString(QUERY,
            PlannerSettings.QUERY_RESULTS_STORE_TABLE.getOptionName(), queryResultsStorePath));
      }

      if(!allowPartitionPruning) {
        manager.setOption(createBoolean(QUERY, PlannerSettings.ENABLE_PARTITION_PRUNING.getOptionName(), false));
      }

      if (internalSingleThreaded) {
        manager.setOption(createBoolean(QUERY, ExecConstants.SORT_FILE_BLOCKS.getOptionName(), true));
        manager.setOption(createBoolean(QUERY, PlannerSettings.EXCHANGE.getOptionName(), true));
      }
    }
  }
}
