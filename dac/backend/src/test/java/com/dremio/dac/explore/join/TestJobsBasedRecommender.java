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
package com.dremio.dac.explore.join;

import static com.dremio.service.job.proto.JobState.COMPLETED;
import static com.dremio.service.job.proto.JoinType.FullOuter;
import static com.dremio.service.job.proto.JoinType.Inner;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.dremio.dac.explore.join.JobsBasedRecommender.ParentJobsProvider;
import com.dremio.dac.explore.model.Dataset;
import com.dremio.dac.explore.model.DatasetName;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetVersionResourcePath;
import com.dremio.dac.explore.model.JoinRecommendation;
import com.dremio.dac.explore.model.JoinRecommendations;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.JoinType;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JoinConditionInfo;
import com.dremio.service.job.proto.JoinInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;

/**
 * tests JobsBasedRecommender
 */
public class TestJobsBasedRecommender {

  private static final List<String> TABLE_A1 = ImmutableList.of("a", "a1");
  private static final List<String> TABLE_A2 = ImmutableList.of("a", "a2");
  private static final List<String> TABLE_B1 = ImmutableList.of("a", "b1");
  private static final List<String> TABLE_B2 = ImmutableList.of("a", "b2");

  /**
   * more concise builder for FieldOrigin
   * @param field name of the field in the query
   * @param table name of the referred table
   * @param col name of the referred column
   * @return the FieldOrigin
   */
  private FieldOrigin orig(String field, List<String> table, String col) {
    return new FieldOrigin(field)
        .setOriginsList(asList(
            new Origin(col, false)
            .setTableList(table)
            ));
  }

  @Test
  public void testMock() {
    JoinInfo join1 = new JoinInfo(FullOuter, 0)
        .setLeftTablePathList(TABLE_A1)
        .setRightTablePathList(TABLE_A2)
        .setConditionsList(asList(
            new JoinConditionInfo("a", "b").setTableAList(TABLE_A1).setTableBList(TABLE_A2)
        ));
    MockParentJobsProvider mock = new MockParentJobsProvider(asList(job("j1", join1)));

    assertEquals(1, ImmutableList.copyOf(mock.getJobsForParent(TABLE_A1)).size());
    assertEquals(1, ImmutableList.copyOf(mock.getJobsForParent(TABLE_A2)).size());
  }

  @Test
  public void simpleTest() {
    JoinInfo join1 = new JoinInfo(FullOuter, 0)
        .setLeftTablePathList(TABLE_A1)
        .setRightTablePathList(TABLE_B2)
        .setConditionsList(asList(
            new JoinConditionInfo("a", "b").setTableAList(TABLE_A1).setTableBList(TABLE_B2)
        ));
    Dataset dataset = newDataset("fooDS", orig("a", TABLE_A1, "a"), orig("b", TABLE_A2, "b"));
    JobsBasedRecommender reco = recommender(asList(job("j1", join1)));

    JoinRecommendations recos = reco.recommendJoins(dataset);
    assertEquals(recos.getRecommendations().toString(), 1, recos.getRecommendations().size());
    validate(recos.getRecommendations().get(0), JoinType.FullOuter, TABLE_B2, "a", "b");
  }

  @Test
  public void nullOriginTest() {
    JoinInfo join1 = new JoinInfo(FullOuter, 0)
        .setLeftTablePathList(TABLE_A1)
        .setRightTablePathList(TABLE_B2)
        .setConditionsList(asList(
            new JoinConditionInfo("a", "b").setTableAList(TABLE_A1).setTableBList(TABLE_B2)
        ));
    Dataset dataset = newDataset("fooDS",
        new FieldOrigin("a"),
        new FieldOrigin("b")
        );
    JobsBasedRecommender reco = recommender(asList(job("j1", join1)));

    JoinRecommendations recos = reco.recommendJoins(dataset);
    assertEquals(recos.getRecommendations().toString(), 0, recos.getRecommendations().size());
  }

  @Test
  public void testMultiple() {
    JoinInfo join1 = new JoinInfo(FullOuter, 0)
        .setLeftTablePathList(TABLE_A1)
        .setRightTablePathList(TABLE_A2)
        .setConditionsList(asList(
            new JoinConditionInfo("a", "b").setTableAList(TABLE_A1).setTableBList(TABLE_A2)
        ));
    JoinInfo join2 = new JoinInfo(Inner, 0)
        .setLeftTablePathList(TABLE_B1)
        .setRightTablePathList(TABLE_B2)
        .setConditionsList(asList(
            new JoinConditionInfo("a", "b").setTableAList(TABLE_B1).setTableBList(TABLE_B2)
        ));
    Dataset dataset = newDataset("fooDS", orig("a", TABLE_A1, "a"), orig("b", TABLE_A2, "b"));
    JobsBasedRecommender reco = recommender(
        asList(
            job("j1", join1),
            job("j2", join2),
            job("j3", join1))
    );
    JoinRecommendations recos = reco.recommendJoins(dataset);
    assertEquals(recos.getRecommendations().toString(), 2, recos.getRecommendations().size());
    // first join gets recommended both ways
    validate(recos.getRecommendations().get(0), JoinType.FullOuter, TABLE_A2, "a", "b");
    validate(recos.getRecommendations().get(1), JoinType.FullOuter, TABLE_A1, "b", "a");
  }

  @Test
  public void testRenameMultiple() {
    JoinInfo join1 = new JoinInfo(Inner, 0)
        .setLeftTablePathList(TABLE_A1)
        .setRightTablePathList(TABLE_A2)
        .setConditionsList(asList(
            new JoinConditionInfo("a1_a", "a2_b").setTableAList(TABLE_A1).setTableBList(TABLE_A2)
        ));
    JoinInfo join2 = new JoinInfo(FullOuter, 0)
        .setLeftTablePathList(TABLE_B1)
        .setRightTablePathList(TABLE_B2)
        .setConditionsList(asList(
            new JoinConditionInfo("b1_a", "b2_b").setTableAList(TABLE_B1).setTableBList(TABLE_B2)
        ));
    Dataset dataset = newDataset("fooDS",
        orig("aliased_a1_a", TABLE_A1, "a1_a"), orig("aliased_b1_a", TABLE_B1, "b1_a"));
    JobsBasedRecommender reco = recommender(
        asList(
            job("j1", join1),
            job("j2", join2),
            job("j3", join1))
    );
    JoinRecommendations recos = reco.recommendJoins(dataset);
    assertEquals(recos.getRecommendations().toString(), 2, recos.getRecommendations().size());
    validate(recos.getRecommendations().get(0), JoinType.Inner, TABLE_A2, "aliased_a1_a", "a2_b");
    validate(recos.getRecommendations().get(1), JoinType.FullOuter, TABLE_B2, "aliased_b1_a", "b2_b");
  }

  private void validate(JoinRecommendation rec, JoinType joinType, List<String> rightTable, String... conditions) {
    assertEquals(rec.toString(), joinType, rec.getJoinType());
    assertEquals(rec.toString(), conditions.length / 2, rec.getMatchingKeys().size());
    for (int i = 0; i < conditions.length; i += 2) {
      String a = conditions[i];
      String b = conditions[i + 1];
      assertEquals(rec.toString(), b, rec.getMatchingKeys().get(a));
    }
    assertEquals(rec.toString(), rightTable, rec.getRightTableFullPathList());
  }

  private Job job(List<String> dataset, DatasetVersion version, List<JoinInfo> joins) {
    JobId jobId = new JobId(UUID.randomUUID().toString());
    return new Job(jobId,
        new JobAttempt()
        .setState(COMPLETED)
        .setInfo(
            new JobInfo(jobId, "select dummy from dummy", version.getVersion(), QueryType.UI_INTERNAL_PREVIEW)
            .setDatasetPathList(dataset)
            .setQueryType(QueryType.UI_INTERNAL_PREVIEW)
            .setStartTime(System.currentTimeMillis())
            .setJoinsList(joins)));
  }

  private Job job(String datasetName, JoinInfo... joins) {
    return job(
        asList("jobSpace", datasetName),
        DatasetVersion.newVersion(),
        asList(joins));
  }

  private JobsBasedRecommender recommender(List<Job> jobs) {
    for (Job job : jobs) {
      List<JoinInfo> joinsList = job.getJobAttempt().getInfo().getJoinsList();
      for (JoinInfo joinInfo : joinsList) {
        List<JoinConditionInfo> conditionsList = joinInfo.getConditionsList();
        for (JoinConditionInfo joinConditionInfo : conditionsList) {
          Preconditions.checkNotNull(joinConditionInfo.getTableAList());
          Preconditions.checkNotNull(joinConditionInfo.getTableBList());
        }
      }
    }
    ParentJobsProvider parentJobsProvider = new MockParentJobsProvider(jobs);
    JobsBasedRecommender reco = new JobsBasedRecommender(parentJobsProvider);
    return reco;
  }

  private Dataset newDataset(String name, FieldOrigin... fieldOrigins) {
    DatasetName datasetName = new DatasetName(name);
    DatasetVersion version = DatasetVersion.newVersion();
    DatasetVersionResourcePath versionedResourcePath = new DatasetVersionResourcePath(new DatasetPath(new SpaceName("FooSpace"), datasetName), version);
    String sql = "select * from foo";
    VirtualDatasetUI datasetConfig = new VirtualDatasetUI();
    datasetConfig.setState(new VirtualDatasetState(new FromTable("foo").wrap()));
    datasetConfig.setFieldOriginsList(asList(fieldOrigins));
    Dataset dataset = new Dataset(null, versionedResourcePath.truncate(), versionedResourcePath, datasetName, sql, datasetConfig, null, 0, 0);
    return dataset;
  }

  private static final class MockParentJobsProvider implements ParentJobsProvider {
    private final Multimap<List<String>, Job> index;

    public MockParentJobsProvider(List<Job> parentJobs) {
      ImmutableSetMultimap.Builder<List<String>, Job> builder = ImmutableSetMultimap.builder();
      for (Job job : parentJobs) {
        List<JoinInfo> joinList = job.getJobAttempt().getInfo().getJoinsList();
        for (JoinInfo joinInfo : joinList) {
          builder.put(joinInfo.getLeftTablePathList(), job);
          builder.put(joinInfo.getRightTablePathList(), job);
        }
      }
      this.index = builder.build();
    }

    @Override
    public Iterable<Job> getJobsForParent(List<String> parentDataset) {
      return index.get(parentDataset);
    }
  }
}
