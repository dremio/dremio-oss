/*
 * Copyright © 2017 Dremio Inc.
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
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.planner.physical.WriterCommitterPrel;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.store.StoragePluginRegistry;
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

  public StoragePluginRegistry getPluginRegistry() {
    return l(StoragePluginRegistry.class);
  }

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();


  @Before
  public void setup() throws Exception {
    final StoragePluginRegistry registry = getPluginRegistry();
    TestUtilities.updateDfsTestTmpSchemaLocation(registry, temp.getRoot().toString());
  }

  @Test
  public void testDistributionInPlan() {
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
  public void testWriterCommitterInPlan() {
    String query = String.format("create table %s.\"%s\" as select * from cp.acceleration.\"employees.json\"", DFS_TEST_PLUGIN_NAME, "xyz");
    final RelNode physical = getPlan(query);

    final Pointer<Boolean> seenWriter = new Pointer<Boolean>(false);

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
    final Job job = getJobsService().submitJob(query, QueryType.ACCELERATOR_CREATE, DatasetPath.NONE.toNamespaceKey(), DatasetVersion.NONE, new NoOpJobStatusListener() {
      @Override
      public void planRelTansform(final PlannerPhase phase, final RelNode before, final RelNode after, final long millisTaken) {
        if (phase == PlannerPhase.PHYSICAL) {
          physical.set(after);
        }

        super.planRelTansform(phase, before, after, millisTaken);
      }
    });

    job.getData().truncate(1);
    return physical.get();
  }
}
