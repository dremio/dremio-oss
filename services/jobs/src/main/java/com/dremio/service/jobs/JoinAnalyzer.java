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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.dremio.exec.proto.UserBitShared.MajorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.MetricValue;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.sabot.op.join.vhash.HashJoinStats.Metric;
import com.dremio.service.job.proto.JoinAnalysis;
import com.dremio.service.job.proto.JoinCondition;
import com.dremio.service.job.proto.JoinStats;
import com.dremio.service.job.proto.JoinTable;
import com.dremio.service.job.proto.JoinType;
import com.dremio.service.jobs.JoinPreAnalyzer.JoinPreAnalysisInfo;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Finds the statistics for join from the query profile
 */
public final class JoinAnalyzer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinAnalyzer.class);

  private final QueryProfile profile;
  private final JoinPreAnalyzer preAnalyzer;
  private final List<JoinStats> joinStatsList = new ArrayList<>();

  public JoinAnalyzer(QueryProfile profile, JoinPreAnalyzer preAnalyzer) {
    this.profile = profile;
    this.preAnalyzer = preAnalyzer;
  }

  public JoinAnalysis computeJoinAnalysis() {
    try {
      for (JoinPreAnalysisInfo joinInfo : preAnalyzer.getJoinInfos()) {
        updateStatsForJoin(joinInfo);
      }

      JoinAnalysis joinAnalysis = new JoinAnalysis()
        .setJoinStatsList(joinStatsList)
        .setJoinTablesList(preAnalyzer.getJoinTables());
      logger.debug("Join analysis: {}", joinAnalysis);
      return joinAnalysis;
    } catch (Exception e) {
      logger.debug("Caught exception while finding joinStats", e);
      return null;
    }
  }

  private void updateStatsForJoin(JoinPreAnalysisInfo joinInfo) {
    int fragmentId = joinInfo.getOpId().getFragmentId();
    final int operatorId = joinInfo.getOpId().getOpId();

    FluentIterable<OperatorProfile> operators = FluentIterable.from(findFragmentProfileWithId(profile, fragmentId).getMinorFragmentProfileList())
      .transform(new Function<MinorFragmentProfile, OperatorProfile>() {
        @Override
        public OperatorProfile apply(MinorFragmentProfile minorFragmentProfile) {
          return findOperatorProfileWithId(minorFragmentProfile, operatorId);
        }
      });

    boolean swapped = joinInfo.getSwapped();
    long unmatchedBuildKeyCount = getSumOfMetric(operators, swapped ? Metric.UNMATCHED_PROBE_COUNT.metricId() : Metric.UNMATCHED_BUILD_KEY_COUNT.metricId());
    long unmatchedProbeCount = getSumOfMetric(operators, swapped ? Metric.UNMATCHED_BUILD_KEY_COUNT.metricId() : Metric.UNMATCHED_PROBE_COUNT.metricId());
    long buildInputRecords = getSumOfInputStream(operators, swapped ? 0 : 1);
    long probeInputRecords = getSumOfInputStream(operators, swapped ? 1 : 0);
    long outputRecords = getSumOfMetric(operators, Metric.OUTPUT_RECORDS.metricId());

    JoinStats stats = new JoinStats()
      .setJoinType(toJoinType(joinInfo.getJoinType()))
      .setUnmatchedBuildCount(unmatchedBuildKeyCount)
      .setUnmatchedProbeCount(unmatchedProbeCount)
      .setBuildInputCount(buildInputRecords)
      .setProbeInputCount(probeInputRecords)
      .setOutputRecords(outputRecords)
      .setJoinConditionsList(joinInfo.getJoinConditions());

    joinStatsList.add(stats);
  }

  private Long getSumOfMetric(FluentIterable<OperatorProfile> operators, final int metricId) {
    long totalCount = 0;
    for (Long count : operators
      .transform(new Function<OperatorProfile, Long>() {
        @Override
        public Long apply(OperatorProfile operatorProfile) {
          try {
            return findMetric(operatorProfile, metricId);
          } catch (Exception ex) {
            logger.debug("Failed to get metric value from operator id: {} and metric id: {}", operatorProfile.getOperatorId(), metricId);
            return 0L;
          }
        }
      })) {
      totalCount += count;
    }
    return totalCount;
  }

  private JoinType toJoinType(JoinRelType joinRelType) {
    switch(joinRelType) {
    case LEFT:
      return JoinType.LeftOuter;
    case RIGHT:
      return JoinType.RightOuter;
    case FULL:
      return JoinType.FullOuter;
    case INNER:
      return JoinType.Inner;
    default:
      throw new RuntimeException(String.format("Unknown join type: %s", joinRelType));
    }
  }

  private Long getSumOfInputStream(FluentIterable<OperatorProfile> operators, final int inputStream) {
    long totalCount = 0;
    for (Long count : operators
      .transform(new Function<OperatorProfile, Long>() {
        @Override
        public Long apply(OperatorProfile operatorProfile) {
          return operatorProfile.getInputProfile(inputStream).getRecords();
        }
      })) {
      totalCount += count;
    }
    return totalCount;
  }

  private static MajorFragmentProfile findFragmentProfileWithId(QueryProfile profile, final int id) {
    return FluentIterable.from(profile.getFragmentProfileList()).firstMatch(new Predicate<MajorFragmentProfile>() {
      @Override
      public boolean apply(@Nullable MajorFragmentProfile input) {
        return input.getMajorFragmentId() == id;
      }
    }).get();
  }

  private static OperatorProfile findOperatorProfileWithId(MinorFragmentProfile minorFragmentProfile, final int id) {
    return FluentIterable.from(minorFragmentProfile.getOperatorProfileList()).firstMatch(new Predicate<OperatorProfile>() {
      @Override
      public boolean apply(OperatorProfile input) {
        return input.getOperatorId() == id;
      }
    }).get();
  }
  private static long findMetric(OperatorProfile operatorProfile, final int id) {
    return FluentIterable.from(operatorProfile.getMetricList()).firstMatch(new Predicate<MetricValue>() {
      @Override
      public boolean apply(@Nullable MetricValue input) {
        return input.getMetricId() == id;
      }
    }).get().getLongValue();
  }

  public static JoinAnalysis merge(JoinAnalysis left, JoinAnalysis right, final RelNode rightPlan, final String materializationId) {
    try {
      int leftMax = Integer.MIN_VALUE;
      for (JoinTable table : Optional.fromNullable(left.getJoinTablesList()).or(Collections.<JoinTable>emptyList())) {
        leftMax = Math.max(leftMax, table.getTableId());
      }

      int rightMin = Integer.MAX_VALUE;
      for (JoinTable table : Optional.fromNullable(right.getJoinTablesList()).or(Collections.<JoinTable>emptyList())) {
        rightMin = Math.min(rightMin, table.getTableId());
      }

      JoinAnalysis newRight = remapJoinAnalysis(right, leftMax - rightMin + 1);

      final Map<List<String>, JoinTable> newTableMapping = FluentIterable.from(newRight.getJoinTablesList())
        .uniqueIndex(new Function<JoinTable, List<String>>() {
          @Override
          public List<String> apply(JoinTable joinTable) {
            return joinTable.getTableSchemaPathList();
          }
        });

      List<JoinTable> combinedJoinTableList = ImmutableList.<JoinTable>builder()
        .addAll(Optional.fromNullable(left.getJoinTablesList()).or(Collections.<JoinTable>emptyList()))
        .addAll(Optional.fromNullable(newRight.getJoinTablesList()).or(Collections.<JoinTable>emptyList()))
        .build();

      List<JoinStats> combinedJoinStatsList = ImmutableList.<JoinStats>builder()
        .addAll(Optional.fromNullable(left.getJoinStatsList()).or(Collections.<JoinStats>emptyList()))
        .addAll(Optional.fromNullable(newRight.getJoinStatsList()).or(Collections.<JoinStats>emptyList()))
        .build();

      final Set<Integer> materializationTableIds = FluentIterable.from(combinedJoinTableList)
        .filter(new Predicate<JoinTable>() {
          @Override
          public boolean apply(JoinTable input) {
            return input.getTableSchemaPathList().size() == 3 && input.getTableSchemaPathList().get(2).equals(materializationId);
          }
        })
        .transform(new Function<JoinTable, Integer>() {
          @Override
          public Integer apply(JoinTable input) {
            return input.getTableId();
          }
        })
        .toSet();

      final RelMetadataQuery metadataQuery = rightPlan.getCluster().getMetadataQuery();

      combinedJoinStatsList = FluentIterable.from(combinedJoinStatsList)
        .transform(new Function<JoinStats, JoinStats>() {
          @Override
          public JoinStats apply(JoinStats joinStats) {
            List<JoinCondition> newConditions = FluentIterable.from(Optional.fromNullable(joinStats.getJoinConditionsList()).or(Collections.emptyList()))
              .transform(new Function<JoinCondition, JoinCondition>() {
                @Override
                public JoinCondition apply(JoinCondition condition) {
                  String newBuildColumn;
                  Integer newBuildTableId;
                  if (materializationTableIds.contains(condition.getBuildSideTableId())) {
                    String col = condition.getBuildSideColumn();
                    RelDataTypeField field = rightPlan.getRowType().getField(col, false, false);
                    if (field == null) {
                      return null;
                    }
                    RelColumnOrigin columnOrigin = metadataQuery.getColumnOrigin(rightPlan, field.getIndex());
                    RelOptTable originTable = columnOrigin.getOriginTable();
                    newBuildTableId = newTableMapping.get(originTable.getQualifiedName()).getTableId();
                    newBuildColumn = originTable.getRowType().getFieldList().get(columnOrigin.getOriginColumnOrdinal()).getName();
                  } else {
                    newBuildTableId = condition.getBuildSideTableId();
                    newBuildColumn = condition.getBuildSideColumn();
                  }
                  String newProbeColumn;
                  Integer newProbeTableId;
                  if (materializationTableIds.contains(condition.getProbeSideTableId())) {
                    String col = condition.getProbeSideColumn();
                    RelDataTypeField field = rightPlan.getRowType().getField(col, false, false);
                    if (field == null) {
                      return null;
                    }
                    RelColumnOrigin columnOrigin = metadataQuery.getColumnOrigin(rightPlan, field.getIndex());
                    RelOptTable originTable = columnOrigin.getOriginTable();
                    newProbeTableId = newTableMapping.get(originTable.getQualifiedName()).getTableId();
                    newProbeColumn = originTable.getRowType().getFieldList().get(columnOrigin.getOriginColumnOrdinal()).getName();
                  } else {
                    newProbeTableId = condition.getProbeSideTableId();
                    newProbeColumn = condition.getProbeSideColumn();
                  }
                  return new JoinCondition()
                    .setProbeSideColumn(newProbeColumn)
                    .setProbeSideTableId(newProbeTableId)
                    .setBuildSideColumn(newBuildColumn)
                    .setBuildSideTableId(newBuildTableId);
                }
              })
              .toList();

            return joinStats.setJoinConditionsList(newConditions);
          }
        }).toList();

      return new JoinAnalysis().setJoinTablesList(combinedJoinTableList).setJoinStatsList(combinedJoinStatsList);
    } catch (Exception e) {
      logger.debug("Exception while merging join analysis", e);
      return new JoinAnalysis();
    }
  }

  private static JoinAnalysis remapJoinAnalysis(final JoinAnalysis joinAnalysis, final int offset) {
    List<JoinTable> newJoinTables = FluentIterable.from(joinAnalysis.getJoinTablesList())
      .transform(new Function<JoinTable, JoinTable>() {
        @Override
        public JoinTable apply(JoinTable joinTable) {
          int newId = joinTable.getTableId() + offset;
          return new JoinTable().setTableId(newId).setTableSchemaPathList(joinTable.getTableSchemaPathList());
        }
      })
      .toList();

    List<JoinStats> joinStatsList = null;
    if (joinAnalysis.getJoinStatsList() != null) {
     joinStatsList = FluentIterable.from(joinAnalysis.getJoinStatsList())
      .transform(new Function<JoinStats, JoinStats>() {
        @Override
        public JoinStats apply(JoinStats joinStats) {
          return new JoinStats()
            .setJoinType(joinStats.getJoinType())
            .setJoinConditionsList(remapJoinConditions(joinStats.getJoinConditionsList(), offset))
            .setProbeInputCount(joinStats.getProbeInputCount())
            .setBuildInputCount(joinStats.getBuildInputCount())
            .setUnmatchedBuildCount(joinStats.getUnmatchedBuildCount())
            .setUnmatchedProbeCount(joinStats.getUnmatchedProbeCount())
            .setOutputRecords(joinStats.getOutputRecords());
        }
      })
      .toList();
    }

    return new JoinAnalysis().setJoinStatsList(joinStatsList).setJoinTablesList(newJoinTables);
  }

  private static List<JoinCondition> remapJoinConditions(List<JoinCondition> joinConditions, final int offset) {
    return FluentIterable.from(joinConditions)
      .transform(new Function<JoinCondition, JoinCondition>() {
        @Override
        public JoinCondition apply(JoinCondition condition) {
          return condition
            .setBuildSideTableId(condition.getBuildSideTableId() + offset)
            .setProbeSideTableId(condition.getProbeSideTableId() + offset);
        }
      })
      .toList();
  }
}
