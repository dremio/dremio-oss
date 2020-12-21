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
package com.dremio.exec.work.user;

import static com.dremio.options.OptionValue.OptionType.QUERY;
import static com.dremio.options.OptionValue.createBoolean;
import static com.dremio.options.OptionValue.createLong;
import static com.dremio.options.OptionValue.createString;

import java.util.List;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PlannerSettings.StoreQueryResultsPolicy;
import com.dremio.options.OptionManager;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Configuration to modify local query execution.
 */
public class LocalExecutionConfig implements OptionProvider {
  private final boolean enableLeafLimits;
  private final boolean enableOutputLimits;
  private final boolean allowPartitionPruning;
  private final boolean failIfNonEmptySent;
  private final String username;
  private final List<String> sqlContext;
  private final boolean internalSingleThreaded;
  private final String queryResultsStorePath;
  private final SubstitutionSettings substitutionSettings;
  private final boolean exposeInternalSources;
  private final String engineName;

  LocalExecutionConfig(final boolean enableLeafLimits,
                       final boolean enableOutputLimits,
                       final boolean failIfNonEmptySent,
                       final String username,
                       final List<String> sqlContext,
                       final boolean internalSingleThreaded,
                       final String queryResultsStorePath,
                       final boolean allowPartitionPruning,
                       final boolean exposeInternalSources,
                       final SubstitutionSettings substitutionSettings,
                       final String engineName) {
    this.enableLeafLimits = enableLeafLimits;
    this.enableOutputLimits = enableOutputLimits;
    this.failIfNonEmptySent = failIfNonEmptySent;
    this.username = username;
    this.sqlContext = sqlContext;
    this.internalSingleThreaded = internalSingleThreaded;
    this.queryResultsStorePath = Preconditions.checkNotNull(queryResultsStorePath);
    this.substitutionSettings = MoreObjects.firstNonNull(substitutionSettings, SubstitutionSettings.of());
    this.allowPartitionPruning = allowPartitionPruning;
    this.exposeInternalSources = exposeInternalSources;
    this.engineName = engineName;
  }

  public String getUsername() {
    return username;
  }

  public boolean isExposingInternalSources() {
    return exposeInternalSources;
  }

  public List<String> getSqlContext() {
    return sqlContext;
  }

  public SubstitutionSettings getSubstitutionSettings() {
    return substitutionSettings;
  }

  public boolean isFailIfNonEmptySent() {
    return failIfNonEmptySent;
  }

  public String getEngineName() {
    return engineName;
  }

  @Override
  public void applyOptions(OptionManager manager) {
    if (enableLeafLimits) {
      manager.setOption(createBoolean(QUERY, PlannerSettings.ENABLE_LEAF_LIMITS.getOptionName(), true));
      manager.setOption(createLong(QUERY, ExecConstants.MAX_WIDTH_GLOBAL_KEY, manager.getOption(PlannerSettings.LEAF_LIMIT_MAX_WIDTH)));
      manager.setOption(createLong(QUERY, ExecConstants.SLICE_TARGET, 1));
    }

    if (enableOutputLimits) {
      manager.setOption(createBoolean(QUERY, PlannerSettings.ENABLE_OUTPUT_LIMITS.getOptionName(), true));
    }

    // always store results
    manager.setOption(createString(QUERY, PlannerSettings.STORE_QUERY_RESULTS.getOptionName(),
        StoreQueryResultsPolicy.PATH_AND_ATTEMPT_ID.name()));
    manager.setOption(createString(QUERY,
        PlannerSettings.QUERY_RESULTS_STORE_TABLE.getOptionName(), queryResultsStorePath));

    if (!allowPartitionPruning) {
      manager.setOption(createBoolean(QUERY, PlannerSettings.ENABLE_PARTITION_PRUNING.getOptionName(), false));
    }

    if (internalSingleThreaded) {
      manager.setOption(createBoolean(QUERY, ExecConstants.SORT_FILE_BLOCKS.getOptionName(), true));
      manager.setOption(createBoolean(QUERY, PlannerSettings.EXCHANGE.getOptionName(), true));
    }
  }

  public static class Builder {
    private boolean enableLeafLimits;
    private boolean enableOutputLimits;
    private long maxQueryWidth;
    private boolean failIfNonEmptySent;
    private String username;
    private List<String> sqlContext;
    private boolean internalSingleThreaded;
    private String queryResultsStorePath;
    private boolean allowPartitionPruning;
    private boolean exposeInternalSources;
    private SubstitutionSettings substitutionSettings;
    private String engineName;

    private Builder() {
    }

    /**
     * Sets the flag to limit the size of data that will be scanned.
     *
     * @param enableLeafLimits flag to enable leaf limits
     * @return this builder
     */
    public Builder setEnableLeafLimits(boolean enableLeafLimits) {
      this.enableLeafLimits = enableLeafLimits;
      return this;
    }

    /**
     * Sets the flag to limit the size of data that will be output.
     *
     * @param enableOutputLimits flag to enable leaf limits
     * @return this builder
     */
    public Builder setEnableOutputLimits(boolean enableOutputLimits) {
      this.enableOutputLimits = enableOutputLimits;
      return this;
    }

    /**
     * Sets the flag to fail the query rather than reattempt when some data has already been sent to client. This is
     * applicable only when reattempt is possible.
     *
     * @param failIfNonEmptySent fail if non empty batch sent
     * @return this builder
     */
    public Builder setFailIfNonEmptySent(boolean failIfNonEmptySent) {
      this.failIfNonEmptySent = failIfNonEmptySent;
      return this;
    }

    /**
     * Sets the username for the query.
     *
     * @param username username
     * @return this builder
     */
    public Builder setUsername(String username) {
      this.username = username;
      return this;
    }

    /**
     * Sets the default schema to 'use' for the query.
     *
     * @param sqlContext sql context
     * @return this builder
     */
    public Builder setSqlContext(List<String> sqlContext) {
      this.sqlContext = sqlContext;
      return this;
    }

    /**
     * Sets the flag to run the query as single threaded (used for internal queries).
     *
     * @param internalSingleThreaded single threaded
     * @return this builder
     */
    public Builder setInternalSingleThreaded(boolean internalSingleThreaded) {
      this.internalSingleThreaded = internalSingleThreaded;
      return this;
    }

    /**
     * Sets the table path where the query results will be stored.
     *
     * @param queryResultsStorePath query result store path
     * @return this builder
     */
    public Builder setQueryResultsStorePath(String queryResultsStorePath) {
      this.queryResultsStorePath = queryResultsStorePath;
      return this;
    }

    /**
     * Sets the flag to enable partition pruning.
     *
     * @param allowPartitionPruning allow partition pruning
     * @return this builder
     */
    public Builder setAllowPartitionPruning(boolean allowPartitionPruning) {
      this.allowPartitionPruning = allowPartitionPruning;
      return this;
    }

    /**
     * Sets the flag to expose internal schemas (used for internal queries).
     *
     * @param exposeInternalSources expose internal schemas
     * @return this builder
     */
    public Builder setExposeInternalSources(boolean exposeInternalSources) {
      this.exposeInternalSources = exposeInternalSources;
      return this;
    }

    /**
     * Sets the settings related to substitution.
     *
     * @param substitutionSettings substitution settings
     * @return this builder
     */
    public Builder setSubstitutionSettings(SubstitutionSettings substitutionSettings) {
      this.substitutionSettings = substitutionSettings;
      return this;
    }

    /**
     * set engine name.
     * @param engineName
     * @return
     */
    public Builder setEngineName(String engineName) {
      this.engineName = engineName;
      return this;
    }

    public LocalExecutionConfig build() {
      return new LocalExecutionConfig(
          enableLeafLimits,
          enableOutputLimits,
          failIfNonEmptySent,
          username,
          sqlContext,
          internalSingleThreaded,
          queryResultsStorePath,
          allowPartitionPruning,
          exposeInternalSources,
          substitutionSettings,
          engineName);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
