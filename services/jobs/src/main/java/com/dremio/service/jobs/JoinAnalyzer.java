/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.common.ContainerRel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.planner.physical.explain.PrelSequencer.OpId;
import com.dremio.exec.planner.physical.visitor.BasePrelVisitor;
import com.dremio.exec.proto.UserBitShared.MajorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.MetricValue;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.sabot.op.common.hashtable.HashTableStats.Metric;
import com.dremio.service.Pointer;
import com.dremio.service.job.proto.JoinAnalysis;
import com.dremio.service.job.proto.JoinCondition;
import com.dremio.service.job.proto.JoinStats;
import com.dremio.service.job.proto.JoinTable;
import com.dremio.service.job.proto.JoinType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Finds the statistics for join from the query profile
 */
public final class JoinAnalyzer extends BasePrelVisitor<Void,Void,RuntimeException> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinAnalyzer.class);

  private final QueryProfile profile;
  private final Prel prel;
  private final Map<List<String>, Integer> tables = new HashMap<>();
  private final List<JoinStats> joinStatsList = new ArrayList<>();
  private Map<Prel,OpId> opIdMap;

  public JoinAnalyzer(QueryProfile profile, Prel prel) {
    this.profile = profile;
    this.prel = prel;
  }

  public JoinAnalysis computeJoinAnalysis() {
    try {
      opIdMap = PrelSequencer.getIdMap(prel);

      TableMapBuilder tableMapBuilder = new TableMapBuilder();
      tableMapBuilder.visit(prel);

      List<JoinTable> joinTables = FluentIterable.from(tables.entrySet())
        .transform(new Function<Entry<List<String>,Integer>, JoinTable>() {
          @Override
          public JoinTable apply(Entry<List<String>, Integer> entry) {
            return new JoinTable()
              .setTableId(entry.getValue())
              .setTableSchemaPathList(entry.getKey());
          }
        })
        .toList();

      prel.accept(this, null);

      JoinAnalysis joinAnalyis = new JoinAnalysis().setJoinStatsList(joinStatsList).setJoinTablesList(joinTables);
      logger.debug("Join analysis: {}", joinAnalyis);
      return joinAnalyis;
    } catch (Exception e) {
      logger.debug("Caught exception while finding joinStats", e);
      return null;
    }
  }

  private class TableMapBuilder extends RoutingShuttle {
    private final Pointer<Integer> counter = new Pointer<>(0);

    @Override
    public RelNode visit(TableScan scan) {
      List<String> table = scan.getTable().getQualifiedName();
      tables.put(table, counter.value++);
      return scan;
    }

    @Override
    public RelNode visit(RelNode other) {
      if ((other instanceof ContainerRel)) {
        ((ContainerRel) other).getSubTree().accept(this);
      }
      return super.visit(other);
    }
  }

  @Override
  public Void visitPrel(final Prel prel, Void dummy) {
    for (Prel child : prel) {
      child.accept(this, dummy);
    }
    return null;
  }

  @Override
  public Void visitJoin(final JoinPrel joinPrel, Void dummy) {
    super.visitJoin(joinPrel, dummy);
    OpId opId = opIdMap.get(joinPrel);
    int fragmentId = opId.getFragmentId();
    final int operatorId = opId.getOpId();

    FluentIterable<OperatorProfile> operators = FluentIterable.from(findFragmentProfileWithId(profile, fragmentId).getMinorFragmentProfileList())
      .transform(new Function<MinorFragmentProfile, OperatorProfile>() {
        @Override
        public OperatorProfile apply(MinorFragmentProfile minorFragmentProfile) {
          return findOperatorProfileWithId(minorFragmentProfile, operatorId);
        }
      });

    boolean swapped = (joinPrel instanceof HashJoinPrel) && ((HashJoinPrel) joinPrel).isSwapped();

    long unmatchedBuildCount = getSumOfMetric(operators, swapped ? Metric.UNMATCHED_PROBE_COUNT.metricId() : Metric.UNMATCHED_BUILD_COUNT.metricId());
    long unmatchedProbeCount = getSumOfMetric(operators, swapped ? Metric.UNMATCHED_BUILD_COUNT.metricId() : Metric.UNMATCHED_PROBE_COUNT.metricId());
    long buildInputRecords = getSumOfInputStream(operators, swapped ? 0 : 1);
    long probeInputRecords = getSumOfInputStream(operators, swapped ? 1 : 0);
    long outputRecords = getSumOfMetric(operators, Metric.OUTPUT_RECORDS.metricId());

    JoinStats stats = new JoinStats()
      .setJoinType(toJoinType(joinPrel.getJoinType()))
      .setUnmatchedBuildCount(unmatchedBuildCount)
      .setUnmatchedProbeCount(unmatchedProbeCount)
      .setBuildInputCount(buildInputRecords)
      .setProbeInputCount(probeInputRecords)
      .setOutputRecords(outputRecords);

    final RelMetadataQuery relMetadataQuery = joinPrel.getCluster().getMetadataQuery();
    final JoinInfo joinInfo = joinPrel.analyzeCondition();

    try {
      List<JoinCondition> joinConditions = FluentIterable.from(Pair.zip(joinInfo.leftKeys, joinInfo.rightKeys))
        .transform(new Function<Pair<Integer, Integer>, JoinCondition>() {
          @Override
          public JoinCondition apply(Pair<Integer, Integer> pair) {
            final RelColumnOrigin leftColumnOrigin = Iterables.getOnlyElement(relMetadataQuery.getColumnOrigins(joinPrel.getLeft(), pair.left));
            final RelColumnOrigin rightColumnOrigin = Iterables.getOnlyElement(relMetadataQuery.getColumnOrigins(joinPrel.getRight(), pair.right));
            final RelOptTable leftTable = leftColumnOrigin.getOriginTable();
            final RelOptTable rightTable = rightColumnOrigin.getOriginTable();

            int leftOrdinal = leftColumnOrigin.getOriginColumnOrdinal();
            int rightOrdinal = rightColumnOrigin.getOriginColumnOrdinal();
            return new JoinCondition()
              .setBuildSideColumn(rightTable.getRowType().getFieldList().get(rightOrdinal).getName())
              .setProbeSideColumn(leftTable.getRowType().getFieldList().get(leftOrdinal).getName())
              .setBuildSideTableId(tables.get(rightTable.getQualifiedName()))
              .setProbeSideTableId(tables.get(leftTable.getQualifiedName()));
          }
        })
        .toList();


      stats.setJoinConditionsList(joinConditions);
    } catch (Exception e) {
      logger.debug("Caught exception while finding join conditions", e);
    }

    joinStatsList.add(stats);

    return null;
  }

  private Long getSumOfMetric(FluentIterable<OperatorProfile> operators, final int metricId) {
    long totalCount = 0;
    for (Long count : operators
      .transform(new Function<OperatorProfile, Long>() {
        @Override
        public Long apply(OperatorProfile operatorProfile) {
          return findMetric(operatorProfile, metricId);
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
    int leftMax = Integer.MIN_VALUE;
    for (JoinTable table : Optional.fromNullable(left.getJoinTablesList()).or(Collections.<JoinTable>emptyList())) {
      leftMax = Math.max(leftMax, table.getTableId());
    }

    int rightMin = Integer.MAX_VALUE;
    for (JoinTable table : Optional.fromNullable(right.getJoinTablesList()).or(Collections.<JoinTable>emptyList())) {
      rightMin = Math.min(rightMin, table.getTableId());
    }

    JoinAnalysis newRight = remapJoinAnalysis(right, leftMax - rightMin + 1);

    final Map<List<String>,JoinTable> newTableMapping = FluentIterable.from(newRight.getJoinTablesList())
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
          List<JoinCondition> newConditions = FluentIterable.from(joinStats.getJoinConditionsList())
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
