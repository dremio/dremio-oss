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
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.JoinRecommendation;
import com.dremio.dac.explore.model.JoinRecommendations;
import com.dremio.dac.proto.model.dataset.JoinType;
import com.dremio.service.job.proto.JoinConditionInfo;
import com.dremio.service.job.proto.JoinInfo;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableListMultimap.Builder;

/**
 * Recommends joins based on job history
 */
public class JobsBasedRecommender implements JoinRecommender {
  private static final Logger logger = LoggerFactory.getLogger(JobsBasedRecommender.class);

  private static final int MAX_JOBS = 5000;

  private final ParentJobsProvider parentJobsProvider;

  @Inject
  public JobsBasedRecommender(JobsService jobsService) {
    this(new JobsServiceParentJobsProvider(jobsService));
  }

  @VisibleForTesting
  JobsBasedRecommender(ParentJobsProvider parentJobsProvider) {
    this.parentJobsProvider = parentJobsProvider;
  }

  @Override
  public JoinRecommendations recommendJoins(Dataset dataset) {
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

    long now = System.currentTimeMillis();

    final List<JoinRecoForScoring> recommendations = new ArrayList<>();

    // 2nd step: for each parent table we look for existing joins.
    for (List<String> parentDataset : parents) {

      // find all jobs that refer to this parent
      Iterable<Job> jobsForParent = parentJobsProvider.getJobsForParent(parentDataset);
      for (Job job : jobsForParent) {
        Long startTime = job.getJobAttempt().getInfo().getStartTime();
        if (startTime == null || job.getJobAttempt().getState() != COMPLETED) {
          continue;
        }
        long recency = now - startTime;
        List<JoinInfo> joins = getJoins(job);

        if (joins != null) {
          for (final JoinInfo join : joins) {
            if (parents.contains(join.getLeftTablePathList())) {
              addJoinReco(refs, recommendations, recency, join, join.getRightTablePathList(), join.getLeftTablePathList());
            }
            // we can add it both ways if both tables are there
            if (parents.contains(join.getRightTablePathList())) {
              addJoinReco(refs, recommendations, recency, join, join.getLeftTablePathList(), join.getRightTablePathList());
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
      int degreesOfSeparation = Integer.MAX_VALUE;
      long recency = Long.MAX_VALUE;
      int jobCount = 0;
      for (JoinRecoForScoring joinReco : recos.getValue()) {
        degreesOfSeparation = Math.min(degreesOfSeparation, joinReco.degreesOfSeparation);
        recency = Math.min(degreesOfSeparation, joinReco.recency);
        jobCount += joinReco.jobCount;
      }
      mergedRecommendations.add(new JoinRecoForScoring(key, degreesOfSeparation, jobCount, recency));
    }
    Collections.sort(mergedRecommendations);
    return recos(mergedRecommendations);
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
      JoinInfo join, List<String> rightTable, List<String> leftTable) {
    Map<String, String> j = translateConditions(refs, leftTable, rightTable, join.getConditionsList());
    if (j != null) {
      recommendations.add(new JoinRecoForScoring(new JoinRecommendation(toJoinType(join.getJoinType()), rightTable, j), join.getDegreesOfSeparation(), 1, recency));
    }
  }

  private JoinType toJoinType(com.dremio.service.job.proto.JoinType joinType) {
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

  private Map<String, String> translateConditions(
      Map<Origin, String> refs,
      List<String> leftTable, List<String> rightTable,
      List<JoinConditionInfo> conditions) {
    SortedMap<String, String> joinConditions = new TreeMap<>();
    // translate names if needed
    for (JoinConditionInfo condition : conditions) {
      String leftColumn;
      String rightColumn;
      // figure out which is which
      if (condition.getTableAList().equals(leftTable) &&
          condition.getTableBList().equals(rightTable)) {
        leftColumn = condition.getColumnA();
        rightColumn = condition.getColumnB();
      } else if (
          condition.getTableBList().equals(leftTable) &&
          condition.getTableAList().equals(rightTable)) {
        leftColumn = condition.getColumnB();
        rightColumn = condition.getColumnA();
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

  private List<JoinInfo> getJoins(Job job) {
    return job.getJobAttempt().getInfo().getJoinsList();
  }


  private static class JoinRecoForScoring implements Comparable<JoinRecoForScoring> {
    private final JoinRecommendation joinReco;
    // for scoring
    private final int jobCount;
    private final int degreesOfSeparation;
    private final long recency;

    public JoinRecoForScoring(
        JoinRecommendation joinReco,
        int degreesOfSeparation,
        int jobCount,
        long recency) {
      this.joinReco = joinReco;
      this.degreesOfSeparation = degreesOfSeparation;
      this.jobCount = jobCount;
      this.recency = recency;
    }

    @Override
    public int compareTo(JoinRecoForScoring other) {
      // we want less
      int c = Integer.compare(degreesOfSeparation, other.degreesOfSeparation);
      if (c != 0) {
        return c;
      }
      // we want more
      c = - Integer.compare(jobCount, other.jobCount);
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
    Iterable<Job> getJobsForParent(List<String> parentDataset);
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
    public Iterable<Job> getJobsForParent(List<String> parentDataset) {
      return jobsService.getJobsForParent(new DatasetPath(parentDataset).toNamespaceKey(), MAX_JOBS);
    }
  }

}
