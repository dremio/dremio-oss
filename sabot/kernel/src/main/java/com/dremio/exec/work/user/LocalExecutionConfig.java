/*
 * Copyright (C) 2017 Dremio Corporation. This file is confidential and private property.
 */
package com.dremio.exec.work.user;

import static com.dremio.exec.server.options.OptionValue.OptionType.QUERY;
import static com.dremio.exec.server.options.OptionValue.createBoolean;
import static com.dremio.exec.server.options.OptionValue.createLong;
import static com.dremio.exec.server.options.OptionValue.createString;

import java.util.List;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.server.options.OptionManager;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Configuration to modify local query execution.
 */
public class LocalExecutionConfig implements OptionProvider {
  private final boolean enableLeafLimits;
  private final boolean allowPartitionPruning;
  private final boolean failIfNonEmptySent;
  private final String username;
  private final List<String> sqlContext;
  private final boolean internalSingleThreaded;
  private final String queryResultsStorePath;
  private final SubstitutionSettings substitutionSettings;
  private final boolean exposeInternalSources;

  LocalExecutionConfig(final boolean enableLeafLimits,
                       final boolean failIfNonEmptySent,
                       final String username,
                       final List<String> sqlContext,
                       final boolean internalSingleThreaded,
                       final String queryResultsStorePath,
                       final boolean allowPartitionPruning,
                       final boolean exposeInternalSources,
                       final SubstitutionSettings substitutionSettings) {
    this.enableLeafLimits = enableLeafLimits;
    this.failIfNonEmptySent = failIfNonEmptySent;
    this.username = username;
    this.sqlContext = sqlContext;
    this.internalSingleThreaded = internalSingleThreaded;
    this.queryResultsStorePath = Preconditions.checkNotNull(queryResultsStorePath);
    this.substitutionSettings = MoreObjects.firstNonNull(substitutionSettings, SubstitutionSettings.of());
    this.allowPartitionPruning = allowPartitionPruning;
    this.exposeInternalSources = exposeInternalSources;
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

  @Override
  public void applyOptions(OptionManager manager) {
    if (enableLeafLimits) {
      manager.setOption(createBoolean(QUERY, PlannerSettings.ENABLE_LEAF_LIMITS.getOptionName(), true));
      manager.setOption(createLong(QUERY, ExecConstants.MAX_WIDTH_GLOBAL_KEY, manager.getOption(PlannerSettings.LEAF_LIMIT_MAX_WIDTH)));
      manager.setOption(createLong(QUERY, ExecConstants.SLICE_TARGET, 1));
    }

    // always store results
    manager.setOption(createBoolean(QUERY, PlannerSettings.STORE_QUERY_RESULTS.getOptionName(), true));
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
    private long maxQueryWidth;
    private boolean failIfNonEmptySent;
    private String username;
    private List<String> sqlContext;
    private boolean internalSingleThreaded;
    private String queryResultsStorePath;
    private boolean allowPartitionPruning;
    private boolean exposeInternalSources;
    private SubstitutionSettings substitutionSettings;

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

    public LocalExecutionConfig build() {
      return new LocalExecutionConfig(
          enableLeafLimits,
          failIfNonEmptySent,
          username,
          sqlContext,
          internalSingleThreaded,
          queryResultsStorePath,
          allowPartitionPruning,
          exposeInternalSources,
          substitutionSettings);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
