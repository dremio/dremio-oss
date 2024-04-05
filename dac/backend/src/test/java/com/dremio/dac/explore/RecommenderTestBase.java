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
package com.dremio.dac.explore;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.dremio.dac.explore.Recommender.TransformRuleWrapper;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.proto.model.dataset.CardExamplePosition;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.DACSecurityContext;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.HybridJobsService;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * Base class for all recommender test classes. Currently it has a utility method to launch queries
 * to generate card samples for recommender rules. Change this to use proper unittest once operator
 * patch is checked-in
 */
public class RecommenderTestBase extends BaseTestServer {

  @ClassRule public static final TemporaryFolder temp = new TemporaryFolder();

  private static final DatasetPath datasetPath = new DatasetPath("test.dataset");
  private BufferAllocator allocator;

  private QueryExecutor executor;
  private DatasetVersion version;

  @Before
  public void setupTest() throws Exception {
    HybridJobsService jobsService = (HybridJobsService) l(JobsService.class);
    allocator =
        l(ContextService.class)
            .get()
            .getAllocator()
            .newChildAllocator("RecommenderTestBase", 0, Long.MAX_VALUE);
    executor = new QueryExecutor(jobsService, l(CatalogService.class), DACSecurityContext.system());
    version = DatasetVersion.newVersion();
  }

  @After
  public void cleanUpTest() {
    allocator.close();
  }

  protected <T> void validate(
      String dataFile,
      TransformRuleWrapper<T> ruleWrapper,
      Object[] functionArgs,
      List<Object> outputs,
      List<Boolean> matches,
      List<List<CardExamplePosition>> highlights) {
    validate(dataFile, ruleWrapper, functionArgs, outputs, matches, highlights, null);
  }

  protected <T> void validate(
      String dataFile,
      TransformRuleWrapper<T> ruleWrapper,
      Object[] functionArgs,
      List<Object> outputs,
      List<Boolean> matches,
      List<List<CardExamplePosition>> highlights,
      String additionalConditions) {
    StringBuilder queryB = new StringBuilder();
    queryB.append("SELECT ");

    if (highlights != null) {
      queryB.append(ruleWrapper.getExampleFunctionExpr("col")).append(" AS e, ");
    }
    queryB.append(ruleWrapper.getFunctionExpr("col", functionArgs)).append(" AS f, ");
    queryB.append(ruleWrapper.getMatchFunctionExpr("col")).append(" AS m ");
    queryB.append(" FROM dfs.\"").append(dataFile).append("\"");
    if (additionalConditions != null && !additionalConditions.isEmpty()) {
      queryB.append(" ").append(additionalConditions);
    }

    SqlQuery sqlQuery = new SqlQuery(queryB.toString(), DEFAULT_USERNAME);
    try (JobDataFragment data =
        executor
            .runQueryAndWaitForCompletion(sqlQuery, QueryType.UI_INTERNAL_RUN, datasetPath, version)
            .truncate(allocator, outputs.size())) {

      assertEquals(outputs.size(), data.getReturnedRowCount());
      for (int i = 0; i < data.getReturnedRowCount(); i++) {
        if (outputs.get(i) == null) {
          assertNull(data.extractString("f", i));
        } else {
          assertEquals(outputs.get(i).toString(), data.extractString("f", i));
        }

        final boolean actualMatch =
            data.extractValue("m", i) != null && (Boolean) data.extractValue("m", i);
        assertEquals(matches.get(i).booleanValue(), actualMatch);

        if (highlights != null) {
          Object ex = data.extractValue("e", i);
          if (highlights.get(i) == null) {
            assertNull(ex);
          } else {
            List<Map<String, Integer>> poss = (List<Map<String, Integer>>) ex;
            List<CardExamplePosition> positions = Lists.newArrayList();
            for (Map<String, Integer> pos : poss) {
              positions.add(new CardExamplePosition(pos.get("offset"), pos.get("length")));
            }

            assertEquals(highlights.get(i), positions);
          }
        }
      }
    }
  }

  @SafeVarargs
  protected final <T> List<T> list(T... elements) {
    List<T> list = new JsonStringArrayList<>(); // toString is JSON
    list.addAll(asList(elements));
    return list;
  }
}
