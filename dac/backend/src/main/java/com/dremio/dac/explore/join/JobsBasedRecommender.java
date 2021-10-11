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
package com.dremio.dac.explore.join;

import static com.dremio.common.utils.Protos.listNotNull;
import static com.dremio.service.job.proto.JobState.COMPLETED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.explore.model.Dataset;
import com.dremio.dac.explore.model.JoinRecommendation;
import com.dremio.dac.explore.model.JoinRecommendations;
import com.dremio.dac.proto.model.dataset.JoinType;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobsWithParentDatasetRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JoinAnalysis;
import com.dremio.service.job.proto.JoinCondition;
import com.dremio.service.job.proto.JoinStats;
import com.dremio.service.job.proto.JoinTable;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableListMultimap.Builder;

/**
 * Recommends joins based on job history
 */
public class JobsBasedRecommender implements JoinRecommender {
  private static final Logger logger = LoggerFactory.getLogger(JobsBasedRecommender.class);

  private static final int MAX_JOBS = 5000;

  private final ParentJobsProvider parentJobsProvider;
  private final NamespaceService namespaceService;

  @Inject
  public JobsBasedRecommender(JobsService jobsService, NamespaceService namespaceService) {
    this.parentJobsProvider = new JobsServiceParentJobsProvider(jobsService);
    this.namespaceService = namespaceService;
  }

  @Override
  public JoinRecommendations recommendJoins(Dataset dataset) {
    try {
      final List<FieldOrigin> fieldOriginsList = listNotNull(dataset.getDatasetConfig().getFieldOriginsList());
      if (fieldOriginsList.isEmpty()) {
        // we can't help at this point
        logger.warn("Could not find field origins in the provided dataset: " + dataset);
        return new JoinRecommendations();
      }

      Set<List<String>> parents = new HashSet<>();
      // index origin columns by their names
      Map<Origin, String> refs = new HashMap<>();
      indexOrigins(fieldOriginsList, parents, refs);

      final long now = System.currentTimeMillis();
      final List<JoinRecoForScoring> recommendations = new ArrayList<>();
      // we cache namespace checks since we are very likely to see repeated datasets
      final Map<NamespaceKey, Boolean> existsMap = new HashMap<>();

      // 2nd step: for each parent table we look for existing joins.
      for (List<String> parentDataset : parents) {

        // find all jobs that refer to this parent
        Iterable<JobDetails> jobsForParent = parentJobsProvider.getJobsForParent(parentDataset);
        for (JobDetails jobDetails : jobsForParent) {
          JobAttempt jobAttempt = JobsProtoUtil.getLastAttempt(jobDetails);
          Long startTime = jobAttempt.getInfo().getStartTime();
          //startTime of 0 means startTime was initially null
          if (startTime == 0 || jobAttempt.getState() != COMPLETED) {
            continue;
          }
          long recency = now - startTime;
          JoinAnalysis joinAnalysis = jobAttempt.getInfo().getJoinAnalysis();

          if (joinAnalysis != null && joinAnalysis.getJoinStatsList() != null && joinAnalysis.getJoinTablesList() != null) {
            Map<Integer, JoinTable> joinTables = FluentIterable.from(joinAnalysis.getJoinTablesList())
              .uniqueIndex(new Function<JoinTable, Integer>() {
                @Override
                public Integer apply(JoinTable joinTable) {
                  return joinTable.getTableId();
                }
              });

            for (final JoinStats join : joinAnalysis.getJoinStatsList()) {
              // ignore if join analysis is missing join conditions
              if (join.getJoinConditionsList() == null || join.getJoinConditionsList().isEmpty()) {
                continue;
              }
              final List<String> leftTablePathList = joinTables.get(join.getJoinConditionsList().get(0).getProbeSideTableId()).getTableSchemaPathList();
              final List<String> rightTablePathList = joinTables.get(join.getJoinConditionsList().get(0).getBuildSideTableId()).getTableSchemaPathList();

              // if any of the join sides no longer exists in namespace, skip it
              if (!checkIfExists(existsMap, leftTablePathList) || !checkIfExists(existsMap, rightTablePathList)) {
                continue;
              }

              if (parents.contains(leftTablePathList)) {
                addJoinReco(refs, recommendations, recency, join, rightTablePathList, leftTablePathList, joinTables);
              }
              // we can add it both ways if both tables are there
              if (parents.contains(rightTablePathList)) {
                addJoinReco(refs, recommendations, recency, join, leftTablePathList, rightTablePathList, joinTables);
              }
            }
          }
        }
      }
      // sum up

      Builder<JoinRecommendation, JoinRecoForScoring> builder = ImmutableListMultimap.builder();
      for (JoinRecoForScoring joinReco : recommendations) {
        builder.put(joinReco.joinReco, joinReco);
      }
      ImmutableListMultimap<JoinRecommendation, JoinRecoForScoring> index = builder.build();
      List<JoinRecoForScoring> mergedRecommendations = new ArrayList<>();
      for (Entry<JoinRecommendation, Collection<JoinRecoForScoring>> recos : index.asMap().entrySet()) {
        JoinRecommendation key = recos.getKey();
        long recency = Long.MAX_VALUE;
        int jobCount = 0;
        for (JoinRecoForScoring joinReco : recos.getValue()) {
          recency = Math.min(recency, joinReco.recency);
          jobCount += joinReco.jobCount;
        }
        mergedRecommendations.add(new JoinRecoForScoring(key, jobCount, recency));
      }
      Collections.sort(mergedRecommendations);
      return recos(mergedRecommendations);
    } catch (Exception e) {
      logger.warn("Caught exception", e);
      return new JoinRecommendations();
    }
  }

  private boolean checkIfExists(final Map<NamespaceKey, Boolean> existsMap, List<String> path) {
    final NamespaceKey key = new NamespaceKey(path);
    if (existsMap.containsKey(key)) {
      return existsMap.get(key);
    }

    final boolean exists = namespaceService.exists(key);
    existsMap.put(key, exists);
    return exists;
  }

  private void indexOrigins(final List<FieldOrigin> fieldOriginsList, Set<List<String>> parents,
      Map<Origin, String> refs) {
    for (FieldOrigin fieldOrigin : fieldOriginsList) {
      List<Origin> originsList = listNotNull(fieldOrigin.getOriginsList());
      // if size != 1 then the field is derived.
      if (originsList.size() == 1) {
        Origin origin = originsList.get(0);
        // if the field was derived we can not use it
        // or we would need to be able to invert the expression
        // it has to be straight unchanged
        if (!origin.getDerived()) {
          parents.add(origin.getTableList());
          // TODO(Julien): if the same column is referred more than once we could recommend all combinations by making refs a multimap
          refs.put(origin, fieldOrigin.getName());
        }
      }
    }
  }

  private void addJoinReco(
      Map<Origin, String> refs, List<JoinRecoForScoring> recommendations, long recency,
      JoinStats joinStats, List<String> rightTable, List<String> leftTable, Map<Integer,JoinTable> tableMap) {
    Map<String, String> j = translateConditions(joinStats, refs, leftTable, rightTable, tableMap);
    if (j != null) {
      recommendations.add(new JoinRecoForScoring(new JoinRecommendation(toJoinType(joinStats.getJoinType()), rightTable, j), 1, recency));
    }
  }

  private JoinType toJoinType(com.dremio.service.job.proto.JoinType joinType) {
    if (joinType == null) {
      return null;
    }
    switch(joinType) {
    case Inner:
      return JoinType.Inner;
    case LeftOuter:
      return JoinType.LeftOuter;
    case RightOuter:
      return JoinType.RightOuter;
    case FullOuter:
      return JoinType.FullOuter;
    default:
      throw new AssertionError(String.format("Unknown join type: %s", joinType));
    }
  }

  private JoinRecommendations recos(List<JoinRecoForScoring> recommendations) {
    JoinRecommendations joinRecommendations = new JoinRecommendations();
    for (JoinRecoForScoring joinReco : recommendations) {
      joinRecommendations.add(joinReco.joinReco);
    }
    return joinRecommendations;
  }

  private Map<String, String> translateConditions(JoinStats stats, Map<Origin,String> refs, List<String> leftTable, List<String> rightTable, Map<Integer,JoinTable> tableMap) {
    SortedMap<String, String> joinConditions = new TreeMap<>();
    // translate names if needed
    for (JoinCondition condition : stats.getJoinConditionsList()) {
      String leftColumn;
      String rightColumn;
      // figure out which is which
      if (
        leftTable.equals(tableMap.get(condition.getProbeSideTableId()).getTableSchemaPathList()) &&
          rightTable.equals(tableMap.get(condition.getBuildSideTableId()).getTableSchemaPathList())
        ) {
        leftColumn = condition.getProbeSideColumn();
        rightColumn = condition.getBuildSideColumn();
      } else if (
        rightTable.equals(tableMap.get(condition.getProbeSideTableId()).getTableSchemaPathList()) &&
          leftTable.equals(tableMap.get(condition.getBuildSideTableId()).getTableSchemaPathList())
        ) {
        leftColumn = condition.getBuildSideColumn();
        rightColumn = condition.getProbeSideColumn();
      } else {
        // this join condition refers a col that is not in the dataset
        return null;
      }
      String leftColumAlias = refs.get(new Origin(leftColumn, false).setTableList(leftTable));
      if (leftColumAlias != null) {
        // if this column is actually in the dataset, translate the name
        leftColumn = leftColumAlias;
      } else {
        // this join condition refers a col that is not in the dataset
        return null;
      }
      joinConditions.put(leftColumn, rightColumn);
    }
    return joinConditions;
  }

  private static class JoinRecoForScoring implements Comparable<JoinRecoForScoring> {
    private final JoinRecommendation joinReco;
    // for scoring
    private final int jobCount;
    private final long recency;

    public JoinRecoForScoring(
        JoinRecommendation joinReco,
        int jobCount,
        long recency) {
      this.joinReco = joinReco;
      this.jobCount = jobCount;
      this.recency = recency;
    }

    @Override
    public int compareTo(JoinRecoForScoring other) {
      // we want more
      int c = - Integer.compare(jobCount, other.jobCount);
      if (c != 0) {
        return c;
      }
      // we want less
      c = Long.compare(recency, other.recency);
      return c;
    }
  }

  /**
   * Allows simple mocking of dependency
   */
  interface ParentJobsProvider {
    Iterable<JobDetails> getJobsForParent(List<String> parentDataset);
  }

  /**
   * Actual dependency
   */
  private static class JobsServiceParentJobsProvider implements ParentJobsProvider {
    private final JobsService jobsService;

    public JobsServiceParentJobsProvider(JobsService jobsService) {
      this.jobsService = jobsService;
    }

    @Override
    public Iterable<JobDetails> getJobsForParent(List<String> parentDataset) {
      JobsWithParentDatasetRequest jobsWithParentDatasetRequest = JobsWithParentDatasetRequest.newBuilder()
        .setDataset(VersionedDatasetPath.newBuilder().addAllPath(parentDataset))
        .setLimit(MAX_JOBS)
        .build();
      return jobsService.getJobsForParent(jobsWithParentDatasetRequest);
    }
  }

}
