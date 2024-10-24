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
package com.dremio;

import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.exec.server.SimpleJobRunner;
import com.dremio.exec.sql.TestStoreQueryResults;
import com.dremio.exec.work.user.LocalExecutionConfig;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.exec.work.user.SubstitutionSettings;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.platform.commons.util.Preconditions;

/**
 * SimpleJobRunner implementation with LocalQueryExecutor to allow querying tables through jobs.
 * This also allows querying internal tables. It saves the result of the query on the local
 * filesystem, the table name can be used to access the result as a table.
 *
 * <p>It saves the result under tempSchema/tblName.
 */
public final class LocalSimpleJobRunner implements SimpleJobRunner {

  private String resultTablePath = null;
  private final String tempSchema;
  private final String tblName;
  private final boolean checkWriterDistributionTrait;
  private final LocalQueryExecutor localQueryExecutor;

  private LocalSimpleJobRunner(
      String tempSchema,
      String tblName,
      boolean checkWriterDistributionTrait,
      LocalQueryExecutor localQueryExecutor) {
    this.tempSchema = tempSchema;
    this.tblName = tblName;
    this.checkWriterDistributionTrait = checkWriterDistributionTrait;
    this.localQueryExecutor = localQueryExecutor;
  }

  public String getResultTablePath() {
    return resultTablePath;
  }

  @Override
  public void runQueryAsJob(String query, String userName, String queryType, String queryLabel) {
    try {
      UserProtos.RunQuery queryCmd =
          UserProtos.RunQuery.newBuilder()
              .setType(UserBitShared.QueryType.SQL)
              .setSource(UserProtos.SubmissionSource.LOCAL)
              .setPlan(query)
              .setQueryLabel(queryLabel)
              .build();
      String queryResultsStorePath = String.format("%s.\"%s\"", tempSchema, tblName);
      LocalExecutionConfig config =
          LocalExecutionConfig.newBuilder()
              .setEnableLeafLimits(false)
              .setFailIfNonEmptySent(false)
              .setUsername(userName)
              .setSqlContext(Collections.emptyList())
              .setInternalSingleThreaded(false)
              .setQueryResultsStorePath(queryResultsStorePath)
              .setAllowPartitionPruning(true)
              .setExposeInternalSources(true)
              .setSubstitutionSettings(SubstitutionSettings.of())
              .build();
      TestStoreQueryResults.TestQueryObserver queryObserver =
          new TestStoreQueryResults.TestQueryObserver(checkWriterDistributionTrait);
      localQueryExecutor.submitLocalQuery(
          ExternalIdHelper.generateExternalId(),
          queryObserver,
          queryCmd,
          false,
          config,
          false,
          null,
          System.currentTimeMillis());
      queryObserver.waitForCompletion();
      resultTablePath = toTableName(queryObserver.getAttemptId());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public List<RecordBatchHolder> runQueryAsJobForResults(
      String query, String userName, String queryType, String queryLabel, int offset, int limit) {
    throw new UnsupportedOperationException("runQueryAsJobForResults() is not implemented");
  }

  private String toTableName(final AttemptId id) {
    return PathUtils.constructFullPath(
        Arrays.asList(tempSchema, tblName, QueryIdHelper.getQueryId(id.toQueryId())));
  }

  public static class LocalSimpleJobRunnerBuilder {
    private String tempSchema = "";
    private String tblName = "";
    private boolean checkWriterDistributionTrait = false;
    private LocalQueryExecutor localQueryExecutor = null;

    public LocalSimpleJobRunnerBuilder setTempSchema(String tempSchema) {
      this.tempSchema = tempSchema;
      return this;
    }

    public LocalSimpleJobRunnerBuilder setTblName(String tblName) {
      this.tblName = tblName;
      return this;
    }

    public LocalSimpleJobRunnerBuilder setCheckWriterDistributionTrait(
        boolean checkWriterDistributionTrait) {
      this.checkWriterDistributionTrait = checkWriterDistributionTrait;
      return this;
    }

    public LocalSimpleJobRunnerBuilder setLocalQueryExecutor(
        LocalQueryExecutor localQueryExecutor) {
      this.localQueryExecutor = localQueryExecutor;
      return this;
    }

    public LocalSimpleJobRunner build() {
      Preconditions.notBlank(tempSchema, "tempSchema should not be blank!");
      Preconditions.notBlank(tblName, "tblName should not be blank!");
      Preconditions.notNull(localQueryExecutor, "LocalQueryExecutor should not be null!");
      return new LocalSimpleJobRunner(
          tempSchema, tblName, checkWriterDistributionTrait, localQueryExecutor);
    }
  }
}
