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
package com.dremio.service.jobs;

import static com.dremio.exec.util.TestUtilities.DFS_TEST_PLUGIN_NAME;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.JobsServiceTestUtils;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.planner.physical.WriterCommitterPrel;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.util.TestUtilities;
import com.dremio.service.Pointer;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.namespace.dataset.DatasetVersion;

/**
 * WriterCommitter tests
 */
public class TestWriter extends BaseTestServer {


  public JobsService getJobsService() {
    return l(JobsService.class);
  }

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();


  @Before
  public void setup() throws Exception {
    TestUtilities.updateDfsTestTmpSchemaLocation((CatalogServiceImpl) l(CatalogService.class), temp.getRoot().toString());
  }

  @Test
  public void testDistributionInPlan() throws Exception {
    String query = String.format("create table %s.\"%s\" hash partition by (\"position\") as select \"position\", isFte, " +
      "count(rating) as \"agg-7-0\", " +
      "sum(rating) as \"agg-7-1\", " +
      "min(rating) as \"agg-7-2\", " +
      "max(rating) as \"agg-7-3\", " +
      "count(1) as convert_count_star " +
      "from cp.acceleration.\"employees.json\" " +
      "group by \"position\", isFte", DFS_TEST_PLUGIN_NAME, "hashDistribute");
    final RelNode physical = getPlan(query);

    final AtomicBoolean hashDistributedWriter = new AtomicBoolean(false);
    physical.accept(new StatelessRelShuttleImpl() {

      @Override
      public RelNode visit(final RelNode other) {
        if (other instanceof WriterPrel) {
          for (RelTrait trait : other.getTraitSet()) {
            if (trait instanceof DistributionTrait) {
              List<DistributionField> distributionFieldList = ((DistributionTrait) trait).getFields();
              if (distributionFieldList.size() != 1) {
                continue;
              }
              int fieldId = distributionFieldList.get(0).getFieldId();
              String fieldName = ((WriterPrel) other).getInput().getRowType().getFieldNames().get(fieldId);
              if ("position".equals(fieldName)) {
                hashDistributedWriter.set(true);
              }
            }
          }
        }
        return super.visit(other);
      }
    });

    Assert.assertTrue("Physical plan must have a HashExchange", hashDistributedWriter.get());
  }


  /**
   * Tests if {@link WriterCommitterPrel} is prepended in CTAS queries
   */
  @Test
  public void testWriterCommitterInPlan() throws Exception {
    String query = String.format("create table %s.\"%s\" as select * from cp.acceleration.\"employees.json\"", DFS_TEST_PLUGIN_NAME, "xyz");
    final RelNode physical = getPlan(query);

    final Pointer<Boolean> seenWriter = new Pointer<>(false);

    physical.accept(new StatelessRelShuttleImpl() {
      private boolean haveWriterCommiterAvailable = false;

      @Override
      public RelNode visit(final RelNode other) {
        if(other instanceof WriterCommitterPrel){
          haveWriterCommiterAvailable = true;
        } else if (other instanceof WriterPrel) {
          seenWriter.value = true;
          if(haveWriterCommiterAvailable){
            haveWriterCommiterAvailable = false;
          }else {
            Assert.fail("WriterPrel must have a ancestor of WriterCommitterPrel. \n"+ RelOptUtil.toString(physical));
          }
        }

        return super.visit(other);
      }
    });

    Assert.assertTrue("Physical plan must have a WriterPrel", seenWriter.value);
  }

  private RelNode getPlan(final String queryString) {
    final AtomicReference<RelNode> physical = new AtomicReference<>(null);

    final SqlQuery query = new SqlQuery(queryString, DEFAULT_USERNAME);

    final PlanTransformationListener planTransformationListener = new PlanTransformationListener() {
      @Override
      public void onPhaseCompletion(PlannerPhase phase, RelNode before, RelNode after, long millisTaken) {
        if (phase == PlannerPhase.PHYSICAL) {
          physical.set(after);
        }
      }
    };

    final JobRequest request = JobRequest.newBuilder()
      .setSqlQuery(query)
      .setQueryType(QueryType.ACCELERATOR_CREATE)
      .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
      .setDatasetVersion(DatasetVersion.NONE)
      .build();

    JobsServiceTestUtils.submitJobAndWaitUntilCompletion(l(LocalJobsService.class), request,
        planTransformationListener);
    return physical.get();
  }
}
