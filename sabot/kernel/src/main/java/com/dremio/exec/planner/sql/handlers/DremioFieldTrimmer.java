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
package com.dremio.exec.planner.sql.handlers;

import com.dremio.common.collections.Tuple;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.calcite.logical.TableModifyCrel;
import com.dremio.exec.planner.common.FlattenRelBase;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.FlattenVisitors;
import com.dremio.exec.planner.logical.LimitRel;
import com.dremio.exec.planner.logical.SampleRel;
import com.dremio.exec.planner.logical.WindowRel;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.store.dfs.FilterableScan;
import com.dremio.service.Pointer;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

public final class DremioFieldTrimmer extends RelFieldTrimmer {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DremioFieldTrimmer.class);

  private static final Double ONE = 1.0D;

  private final RelBuilder builder;
  private final DremioFieldTrimmerParameters parameters;

  public DremioFieldTrimmer(RelBuilder builder, DremioFieldTrimmerParameters parameters) {
    super(null, builder);
    this.builder = builder;
    this.parameters = parameters;
  }

  @Override
  public RelNode trim(RelNode root) {
    if (!logger.isDebugEnabled()) {
      return super.trim(root);
    }

    Stopwatch stopwatch = Stopwatch.createStarted();
    RelNode trimmedNode = super.trim(root);
    stopwatch.stop();

    String plan = RelOptUtil.toString(root, SqlExplainLevel.ALL_ATTRIBUTES);
    long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    String logMessage =
        String.format("FieldTrimmer took %dms for the following RelNode:\n%s", duration, plan);
    logger.debug(logMessage);

    return trimmedNode;
  }

  public TrimResult trimFields(
      TableModifyCrel tableModifyCrel,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {

    // the output fields of TableModifyCrel (i.e., row count column) are not directly related to its
    // input
    // TableModifyCrel's input is trimmed independently here
    final RelNode input = tableModifyCrel.getInput();
    RelNode newInput = trim(input);

    TableModifyCrel newTableModifyCrel =
        new TableModifyCrel(
            tableModifyCrel.getCluster(),
            tableModifyCrel.getTraitSet(),
            tableModifyCrel.getTable(),
            tableModifyCrel.getCatalogReader(),
            newInput,
            tableModifyCrel.getOperation(),
            tableModifyCrel.getUpdateColumnList(),
            tableModifyCrel.getSourceExpressionList(),
            tableModifyCrel.isFlattened(),
            tableModifyCrel.getCreateTableEntry(),
            tableModifyCrel.getMergeUpdateColumnList(),
            tableModifyCrel.hasSource(),
            tableModifyCrel.getOutdatedTargetColumns(),
            tableModifyCrel.getDmlWriteMode());

    final int fieldCount = tableModifyCrel.getRowType().getFieldCount();
    return result(newTableModifyCrel, Mappings.createIdentity(fieldCount));
  }

  /** Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for {@link Aggregate}. */
  @Override
  public TrimResult trimFields(
      Aggregate aggregate, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    int fieldCount = aggregate.getRowType().getFieldCount();
    if (fieldCount == 0) {
      // If the input has no fields, we cannot trim anything.
      return new TrimResult(aggregate, Mappings.createIdentity(fieldCount));
    }
    return super.trimFields(aggregate, fieldsUsed, extraFields);
  }

  /** Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for {@link SampleRel}. */
  public TrimResult trimFields(
      SampleRel sampleRel, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    TrimResult result = dispatchTrimFields(sampleRel.getInput(), fieldsUsed, extraFields);
    return result(SampleRel.create(result.left), result.right);
  }

  /** Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for {@link ScanCrel}. */
  @SuppressWarnings("unused")
  public TrimResult trimFields(
      ScanCrel crel, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {

    if (fieldsUsed.cardinality() == crel.getRowType().getFieldCount()
        || !parameters.trimProjectedColumn()) {
      return result(crel, Mappings.createIdentity(crel.getRowType().getFieldCount()));
    }

    if (fieldsUsed.cardinality() == 0) {
      // do something similar to dummy project but avoid using a scan field. This ensures the scan
      // does a skipAll operation rather than projectin a useless column.
      final RelOptCluster cluster = crel.getCluster();
      final Mapping mapping =
          Mappings.create(MappingType.INVERSE_SURJECTION, crel.getRowType().getFieldCount(), 1);
      final RexLiteral expr = cluster.getRexBuilder().makeExactLiteral(BigDecimal.ZERO);
      builder.push(crel);
      builder.project(ImmutableList.<RexNode>of(expr), ImmutableList.of("DUMMY"));
      return result(builder.build(), mapping);
    }

    final List<SchemaPath> paths = new ArrayList<>();
    final Mapping m =
        Mappings.create(
            MappingType.PARTIAL_FUNCTION,
            crel.getRowType().getFieldCount(),
            fieldsUsed.cardinality());
    int index = 0;
    for (int i : fieldsUsed) {
      paths.add(SchemaPath.getSimplePath(crel.getRowType().getFieldList().get(i).getName()));
      m.set(i, index);
      index++;
    }

    ScanCrel newCrel = crel.cloneWithProject(paths);
    return result(newCrel, m);
  }

  /** Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for {@link ScanRelBase}. */
  @SuppressWarnings("unused")
  public TrimResult trimFields(
      ScanRelBase drel, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {

    // if we've already pushed down projection of nested columns, we don't want to trim anymore
    if (drel.getProjectedColumns().stream().anyMatch(c -> !c.isSimplePath())
        || !parameters.trimProjectedColumn()) {
      return result(drel, Mappings.createIdentity(drel.getRowType().getFieldCount()));
    }

    // Required fields include all fields used by downstream operators,
    // which gets passed in via fieldsUsed, plus all fields used for native
    // scan filtering. Fields used for native scan filtering might not
    // necessarily be used by upstream operators.
    ImmutableBitSet.Builder requiredFieldsBuilder = fieldsUsed.rebuild();

    if (drel instanceof FilterableScan) {
      FilterableScan filterableScan = (FilterableScan) drel;

      Pointer<Boolean> failed = new Pointer<>(false);
      if (filterableScan.getFilter() != null) {
        filterableScan
            .getFilter()
            .getPaths()
            .forEach(
                path -> {
                  String pathString = path.getAsUnescapedPath();
                  RelDataTypeField field = drel.getRowType().getField(pathString, false, false);
                  if (field != null) {
                    requiredFieldsBuilder.set(field.getIndex());
                  } else {
                    failed.value = true;
                  }
                });
      }

      if (failed.value) {
        return result(drel, Mappings.createIdentity(drel.getRowType().getFieldCount()));
      }

      final PruneFilterCondition pruneFilterCondition = filterableScan.getPartitionFilter();
      if (pruneFilterCondition != null && pruneFilterCondition.getPartitionExpression() != null) {
        pruneFilterCondition
            .getPartitionExpression()
            .accept(
                new RexShuttle() {
                  @Override
                  public RexNode visitInputRef(final RexInputRef inputRef) {
                    requiredFieldsBuilder.set(inputRef.getIndex());
                    return super.visitInputRef(inputRef);
                  }
                });
      }

      if (pruneFilterCondition != null && pruneFilterCondition.getPartitionRange() != null) {
        pruneFilterCondition
            .getPartitionRange()
            .accept(
                new RexShuttle() {
                  @Override
                  public RexNode visitInputRef(final RexInputRef inputRef) {
                    requiredFieldsBuilder.set(inputRef.getIndex());
                    return super.visitInputRef(inputRef);
                  }
                });
      }
    }

    ImmutableBitSet requiredFields = requiredFieldsBuilder.build();

    if (requiredFields.cardinality() == drel.getRowType().getFieldCount()) {
      // Delegate to super class handler for TableScan nodes.
      // This will ensure that we project away any unused fields from native scan filters.
      return super.trimFields(drel, fieldsUsed, extraFields);
    }

    if (requiredFields.cardinality() == 0) {
      // do something similar to dummy project but avoid using a scan field. This ensures the scan
      // does a skipAll operation rather than projecting a useless column.
      final RelOptCluster cluster = drel.getCluster();
      final Mapping mapping =
          Mappings.create(MappingType.INVERSE_SURJECTION, drel.getRowType().getFieldCount(), 1);
      final RexLiteral expr = cluster.getRexBuilder().makeExactLiteral(BigDecimal.ZERO);
      builder.push(drel);
      builder.project(ImmutableList.<RexNode>of(expr), ImmutableList.of("DUMMY"));
      return result(builder.build(), mapping);
    }

    // Create new column mapping from original scan columns to trimmed scan columns
    final List<SchemaPath> paths = new ArrayList<>();
    final Mapping m1 =
        Mappings.create(
            MappingType.PARTIAL_FUNCTION,
            drel.getRowType().getFieldCount(),
            requiredFields.cardinality());
    int index = 0;
    for (int i : requiredFields) {
      paths.add(SchemaPath.getSimplePath(drel.getRowType().getFieldList().get(i).getName()));
      m1.set(i, index);
      index++;
    }

    // Create trimmed scan and delegate to super class handler for TableScan nodes.
    // This will ensure that we project away any unused fields from native scan filters.
    ScanRelBase trimmedScan = drel.cloneWithProject(paths);
    ImmutableBitSet upstreamFieldsUsed = Mappings.apply(m1, fieldsUsed);
    TrimResult trimResult = super.trimFields(trimmedScan, upstreamFieldsUsed, extraFields);
    Mapping m2 = trimResult.right;
    return result(trimResult.left, multiply(m1, m2));
  }

  private static Mapping multiply(Mapping mapping1, Mapping mapping2) {
    if (mapping1.getTargetCount() != mapping2.getSourceCount()) {
      throw new IllegalArgumentException();
    }
    Mapping product =
        Mappings.create(
            MappingType.INVERSE_SURJECTION, mapping1.getSourceCount(), mapping2.getTargetCount());
    for (int source = 0; source < mapping1.getSourceCount(); ++source) {
      int x = mapping1.getTargetOpt(source);
      if (x >= 0) {
        int target = mapping2.getTargetOpt(x);
        if (target >= 0) {
          product.set(source, target);
        }
      }
    }
    return product;
  }

  // Overridden until CALCITE-2260 is fixed.
  @Override
  public TrimResult trimFields(
      SetOp setOp, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    if (!setOp.all) {
      return super.trimFields(
          setOp, ImmutableBitSet.range(setOp.getRowType().getFieldCount()), extraFields);
    }
    return super.trimFields(setOp, fieldsUsed, extraFields);
  }

  /**
   * Handler method to trim fields for @FlattenCrel
   *
   * @param flatten
   * @param fieldsUsed
   * @param extraFields
   * @return
   */
  public TrimResult trimFields(
      FlattenRelBase flatten, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = flatten.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelNode input = flatten.getInput();

    // We use the fields used by the consumer, plus any fields used in the
    // flatten.
    final ImmutableBitSet inputFieldsUsed =
        fieldsUsed.union(ImmutableBitSet.of(flatten.getFlattenedIndices()));

    TrimResult trimResult = trimChild(flatten, input, inputFieldsUsed, extraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // When input didn't change and we have to project all the columns as before.
    if (newInput == input && fieldsUsed.cardinality() == fieldCount) {
      return result(flatten, Mappings.createIdentity(fieldCount));
    }

    final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(inputMapping, newInput);

    final List<RexInputRef> flattenFields = new ArrayList<>();

    // Update flatten fields in case if the input fields have changed the mapping.
    for (final RexInputRef flattenField : flatten.getToFlatten()) {
      flattenFields.add((RexInputRef) flattenField.accept(shuttle));
    }

    return result(flatten.copy(Arrays.asList(newInput), flattenFields), inputMapping);
  }

  @Override
  public TrimResult trimFields(
      Project project, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    int count = FlattenVisitors.count(project.getProjects());

    // if there are no flatten, trim is fine.
    TrimResult result = super.trimFields(project, fieldsUsed, extraFields);

    // if it is rel planning mode, removing top project would generate wrong sql query.
    // make sure to have a top project after trimming
    if (parameters.isRelPlanning() && !(result.left instanceof Project)) {
      List<RexNode> identityProject = new ArrayList<>();
      for (int i = 0; i < result.left.getRowType().getFieldCount(); i++) {
        identityProject.add(
            new RexInputRef(i, result.left.getRowType().getFieldList().get(i).getType()));
      }
      builder.push(result.left);
      result =
          result(
              builder
                  .project(identityProject, result.left.getRowType().getFieldNames(), true)
                  .build(),
              result.right);
    }

    if (count == 0) {
      return result;
    }

    // start by trimming based on super.

    if (result.left.getRowType().getFieldCount() != fieldsUsed.cardinality()) {
      // we got a partial trim, which we don't handle. Skip the optimization.
      return result(project, Mappings.createIdentity(project.getRowType().getFieldCount()));
    }
    final RelNode resultRel = result.left;
    final Mapping finalMapping = result.right;

    if (resultRel instanceof Project
        && FlattenVisitors.count(((Project) resultRel).getProjects()) == count) {
      // flatten count didn't change.
      return result;
    }

    /*
     * Flatten count changed. To solve, we'll actually increase the required fields to include the
     * flattens and then put another project on top that drops the extra fields, returning the
     * previously generated mapping.
     */
    ImmutableBitSet.Builder flattenColumnsBuilder = ImmutableBitSet.builder();
    {
      int i = 0;
      for (RexNode n : project.getProjects()) {
        try {
          if (fieldsUsed.get(i)) {
            continue;
          }

          if (!FlattenVisitors.hasFlatten(n)) {
            continue;
          }

          // we have a flatten in an unused field.
          flattenColumnsBuilder.set(i);

        } finally {
          i++;
        }
      }
    }

    ImmutableBitSet unreferencedFlattenProjects = flattenColumnsBuilder.build();

    if (unreferencedFlattenProjects.isEmpty()) {
      // this should be impossible. fall back to using the base case (no column trim) as it means we
      // just optimize less.
      logger.info(
          "Failure while trying to trim flatten expression. Expressions {}, Columns to trim to: {}",
          project.getProjects(),
          fieldsUsed);
      return result(project, Mappings.createIdentity(project.getRowType().getFieldCount()));
    }

    final ImmutableBitSet fieldsIncludingFlattens = fieldsUsed.union(unreferencedFlattenProjects);
    final TrimResult result2 = super.trimFields(project, fieldsIncludingFlattens, extraFields);

    List<RexNode> finalProj = new ArrayList<>();
    builder.push(result2.left);

    int i = 0;
    for (int index : fieldsIncludingFlattens) {
      if (fieldsUsed.get(index)) {
        finalProj.add(builder.field(i));
      }
      i++;
    }

    // drop the flatten columns in a subsequent projection.
    return result(builder.project(finalProj).build(), finalMapping);
  }

  public TrimResult trimFields(
      LimitRel limit, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = limit.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelNode input = limit.getInput();

    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult = trimChild(limit, input, fieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    if (newInput == input && inputMapping.isIdentity() && fieldsUsed.cardinality() == fieldCount) {
      return result(limit, Mappings.createIdentity(fieldCount));
    }
    return result(limit.copy(newInput.getTraitSet(), ImmutableList.of(newInput)), inputMapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for {@link
   * org.apache.calcite.rel.logical.LogicalJoin}. Checks to see that we are projecting from a single
   * side of the JOIN that doesn't modify the output rows. If so, then we remove the join and
   * recurse onto the side we are projecting from.
   */
  @Override
  public TrimResult trimFields(
      Join join, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    if (!parameters.trimJoinBranch()) {
      return super.trimFields(join, fieldsUsed, extraFields);
    }

    Optional<Tuple<RelNode, Mapping>> optionalTrimResult = tryTrimJoin(join, fieldsUsed);
    if (!optionalTrimResult.isPresent()) {
      return super.trimFields(join, fieldsUsed, extraFields);
    }

    Tuple<RelNode, Mapping> trimResult = optionalTrimResult.get();
    return result(trimResult.first, trimResult.second);
  }

  private static Optional<Tuple<RelNode, Mapping>> tryTrimJoin(
      Join join, ImmutableBitSet fieldsUsed) {
    /**
     * The internal algorithm checks to see if we have a JOIN that can be removed. It does so by
     * asking: 1) Are we projecting from the side that is being JOINed on? 2) Is the JOIN NOT
     * modifying the rows emitted?
     *
     * <p>If we answer yes to all of these, then we can remove the JOIN
     */
    if (!join.getSystemFieldList().isEmpty()) {
      // If there are system fields, then we can't apply the optimization
      return Optional.empty();
    }

    ProjectJoinType projectJoinType = getProjectJoinType(join, fieldsUsed);
    if (projectJoinType == ProjectJoinType.BOTH) {
      return Optional.empty();
    }

    if (projectJoinType == ProjectJoinType.NEITHER) {
      // The rest of this code assumes either a LEFT or RIGHT project
      // To make this optimization work we want to pick the side that has the least work
      // while also preserving the cardinality.
      boolean leftCardinalityOne = hasCardinalityOne(join.getLeft());
      boolean rightCardinalityOne = hasCardinalityOne(join.getRight());
      if (leftCardinalityOne && rightCardinalityOne) {
        // Pick the non agg side, since that is less work
        projectJoinType =
            join.getLeft() instanceof Aggregate ? ProjectJoinType.RIGHT : ProjectJoinType.LEFT;
      } else if (leftCardinalityOne && !rightCardinalityOne) {
        // Right side preserves cardinality
        projectJoinType = ProjectJoinType.RIGHT;
      } else if (!leftCardinalityOne && rightCardinalityOne) {
        // Left side preserves cardinality
        projectJoinType = ProjectJoinType.LEFT;
      } else {
        // A trim is not possible
        return Optional.empty();
      }
    }

    if (!joinTypeCompatibleWithProjectType(join, projectJoinType)) {
      return Optional.empty();
    }

    // At this point we know the JOIN will not DECREASE the cardinality
    // We need to also check that the JOIN will not INCREASE the cardinality
    // This is done by making sure the JOIN condition matches the elements from the left table
    // with at most one element in the right table.
    if (!isJoinConditionOneToOne(join, projectJoinType)) {
      return Optional.empty();
    }

    final RelNode mainBranch =
        projectJoinType == ProjectJoinType.LEFT ? join.getLeft() : join.getRight();
    Mapping mapping =
        Mappings.create(
            MappingType.SURJECTION,
            join.getRowType().getFieldCount(),
            mainBranch.getRowType().getFieldCount());
    if (projectJoinType == ProjectJoinType.LEFT) {
      for (int i = 0; i < mainBranch.getRowType().getFieldCount(); i++) {
        mapping.set(i, i);
      }
    } else {
      int leftFieldCount = join.getLeft().getRowType().getFieldCount();
      for (int i = 0; i < mainBranch.getRowType().getFieldCount(); i++) {
        mapping.set(i + leftFieldCount, i);
      }
    }

    return Optional.of(Tuple.of(mainBranch, mapping));
  }

  /**
   * Tries to return the side we are projecting from.
   *
   * @param join
   * @param fieldsUsed
   * @return The side we are projecting from (LEFT, RIGHT, or EMPTY if both).
   */
  private static ProjectJoinType getProjectJoinType(Join join, ImmutableBitSet fieldsUsed) {
    if (fieldsUsed.isEmpty()) {
      return ProjectJoinType.NEITHER;
    }

    int minFieldIndex = Integer.MAX_VALUE;
    int maxFiledIndex = -1;
    for (int index : fieldsUsed) {
      minFieldIndex = Math.min(minFieldIndex, index);
      maxFiledIndex = Math.max(maxFiledIndex, index);
    }

    int numLeftColumns = join.getLeft().getRowType().getFieldCount();
    boolean projectsFromBothSides =
        (minFieldIndex < numLeftColumns) && (maxFiledIndex >= numLeftColumns);
    if (projectsFromBothSides) {
      return ProjectJoinType.BOTH;
    }

    boolean isLeftOnlyJoin = (minFieldIndex < numLeftColumns) && (maxFiledIndex < numLeftColumns);
    return isLeftOnlyJoin ? ProjectJoinType.LEFT : ProjectJoinType.RIGHT;
  }

  private static boolean joinTypeCompatibleWithProjectType(
      Join join, ProjectJoinType projectJoinType) {
    switch (join.getJoinType()) {
      case FULL:
        // A full join will have all the rows from both the LEFT and RIGHT table
        return true;

      case INNER:
        // This is the same as a FULL JOIN
        return join.getCondition().isAlwaysTrue();

      case LEFT:
        return projectJoinType == ProjectJoinType.LEFT;

      case RIGHT:
        return projectJoinType == ProjectJoinType.RIGHT;

      default:
        // We don't know how to support these other join types yet.
        return false;
    }
  }

  private static boolean isJoinConditionOneToOne(Join join, ProjectJoinType projectJoinType) {
    RelNode sideBranch = projectJoinType == ProjectJoinType.LEFT ? join.getRight() : join.getLeft();
    RexNode condition = join.getCondition();
    boolean isOneToOne;
    if (condition.isAlwaysTrue()) {
      // We need to know that the join is "cardinality preserving"
      // This means the side branch should have cardinality 1
      isOneToOne = hasCardinalityOne(sideBranch);
    } else if (condition instanceof RexCall) {
      RexCall rexCall = (RexCall) condition;
      isOneToOne = isJoinConditionOneToOneRexCall(join, rexCall, sideBranch);
    } else {
      isOneToOne = false;
    }

    return isOneToOne;
  }

  private static boolean hasCardinalityOne(RelNode relNode) {
    // We need metadata query to do any row count operations,
    // so if it's not available, then just give up.
    if (relNode.getCluster() == null || relNode.getCluster().getMetadataQuery() == null) {
      return false;
    }

    RelMetadataQuery relMetadataQuery = relNode.getCluster().getMetadataQuery();
    return ONE.equals(relMetadataQuery.getMaxRowCount(relNode))
        && ONE.equals(relMetadataQuery.getMinRowCount(relNode));
  }

  private static boolean isJoinConditionOneToOneRexCall(
      Join join, RexCall condition, RelNode sideBranch) {
    switch (condition.getKind()) {
      case EQUALS:
        return isJoinConditionOneToOneRexCallEquals(join, condition, sideBranch);
      case AND:
        return isJoinConditionOneToOneRexCallAnd(join, condition, sideBranch);
      default:
        return false;
    }
  }

  private static boolean isJoinConditionOneToOneRexCallEquals(
      Join join, RexCall condition, RelNode sideBranch) {
    assert condition.getKind() == SqlKind.EQUALS;
    // We need to have an equi join on the sideBranch with only unique values
    // Example:
    //     JoinRel(condition=[=($0, $2)], joinType=[left])"
    //       ProjectRel(col1=[$0], col2=[$1])"
    //         FilesystemScanDrel(table=[cp.\"dx56085/t1.json\"], columns=[`col1`, `col2`],
    // splits=[1])"
    //       AggregateRel(group=[{0}])"
    // We know that =($0, $2) can match at most one value, since $2 is a groupkey
    Optional<Integer> optionalSideBranchEqualityIndex =
        tryGetSideBranchEqualityIndex(join, condition, sideBranch);
    if (!optionalSideBranchEqualityIndex.isPresent()) {
      return false;
    }

    int sideBranchEqualityIndex = optionalSideBranchEqualityIndex.get();
    ImmutableBitSet columns =
        ImmutableBitSet.builder().addAll(ImmutableList.of(sideBranchEqualityIndex)).build();

    return Boolean.TRUE.equals(
        sideBranch.getCluster().getMetadataQuery().areColumnsUnique(sideBranch, columns));
  }

  private static boolean isJoinConditionOneToOneRexCallAnd(
      Join join, RexCall condition, RelNode sideBranch) {
    assert condition.getKind() == SqlKind.AND;
    // If the condition is the AND of a bunch of equality checks that when unioned span unique
    // column sets
    // Then we can remove the JOIN
    // For example:
    //   ProjectRel(col1=[$0])"
    //     JoinRel(condition=[AND(=($0, $2), =($1, $3))], joinType=[left])"
    //       FilesystemScanDrel(table=[cp.\"dx56085/t1.json\"], columns=[`col1`, `col2`],
    // splits=[1])"
    //       AggregateRel(group=[{0, 1}])"
    //         FilesystemScanDrel(table=[cp.\"dx56085/t2.json\"], columns=[`col1`, `col2`],
    // splits=[1])"
    // Is doing an equality check on $2 and $3 which when combined gets us the full group=[{0, 1}]
    // which is unique
    List<Integer> columns =
        condition.getOperands().stream()
            .map(operand -> tryGetSideBranchEqualityIndex(join, operand, sideBranch))
            .filter(optional -> optional.isPresent())
            .map(optional -> optional.get())
            .collect(Collectors.toList());

    ImmutableBitSet columnBitSet = ImmutableBitSet.builder().addAll(columns).build();

    return Boolean.TRUE.equals(
        sideBranch.getCluster().getMetadataQuery().areColumnsUnique(sideBranch, columnBitSet));
  }

  private static Optional<Integer> tryGetSideBranchEqualityIndex(
      Join join, RexNode condition, RelNode sideBranch) {
    if (condition.getKind() != SqlKind.EQUALS) {
      return Optional.empty();
    }

    RexCall equalsConditon = (RexCall) condition;
    if (equalsConditon.getOperands().size() != 2) {
      throw new UnsupportedOperationException(
          "Expected equals condition to have exactly 2 operands.");
    }

    for (RexNode operand : equalsConditon.getOperands()) {
      if (!(operand instanceof RexInputRef)) {
        return Optional.empty();
      }
    }

    int leftIndex = ((RexInputRef) equalsConditon.getOperands().get(0)).getIndex();
    int rightIndex = ((RexInputRef) equalsConditon.getOperands().get(1)).getIndex();
    if (leftIndex > rightIndex) {
      int temp = leftIndex;
      leftIndex = rightIndex;
      rightIndex = temp;
    }

    int numLeftColumns = join.getLeft().getRowType().getFieldCount();
    if ((leftIndex >= numLeftColumns) || (rightIndex < numLeftColumns)) {
      return Optional.empty();
    }

    // We want the index of the column participating in the join condition on the side branch
    // If the side branch is the right join,
    // then we need to adjust the index to be relative side branch,
    // since it's currently relative to the whole join.
    int adjustedIndex =
        sideBranch.equals(join.getRight()) ? rightIndex - numLeftColumns : leftIndex;
    return Optional.of(adjustedIndex);
  }

  /** Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for {@link MultiJoin}. */
  @SuppressWarnings("unused")
  public TrimResult trimFields(
      MultiJoin multiJoin, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    Util.discard(extraFields); // unlike #trimFields in RelFieldTrimmer

    final List<RelNode> originalInputs = multiJoin.getInputs();
    final RexNode originalJoinFilter = multiJoin.getJoinFilter();
    final List<RexNode> originalOuterJoinConditions = multiJoin.getOuterJoinConditions();
    final RexNode originalPostJoinFilter = multiJoin.getPostJoinFilter();

    int fieldCount = 0;
    for (RelNode input : originalInputs) {
      fieldCount += input.getRowType().getFieldCount();
    }

    // add in fields used in the all the conditions; including the ones requested in "fieldsUsed"
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(null, fieldsUsed);
    originalJoinFilter.accept(inputFinder);
    originalOuterJoinConditions.forEach(
        rexNode -> {
          if (rexNode != null) {
            rexNode.accept(inputFinder);
          }
        });
    if (originalPostJoinFilter != null) {
      originalPostJoinFilter.accept(inputFinder);
    }
    final ImmutableBitSet fieldsUsedPlus = inputFinder.build();

    int offset = 0;
    int changeCount = 0;
    int newFieldCount = 0;

    final List<RelNode> newInputs = Lists.newArrayListWithExpectedSize(originalInputs.size());
    final List<Mapping> inputMappings = Lists.newArrayList();

    for (RelNode input : originalInputs) {
      final RelDataType inputRowType = input.getRowType();
      final int inputFieldCount = inputRowType.getFieldCount();

      // compute required mapping
      final ImmutableBitSet.Builder inputFieldsUsed = ImmutableBitSet.builder();
      for (int bit : fieldsUsedPlus) {
        if (bit >= offset && bit < offset + inputFieldCount) {
          inputFieldsUsed.set(bit - offset);
        }
      }

      final TrimResult trimResult =
          trimChild(multiJoin, input, inputFieldsUsed.build(), Collections.emptySet());
      newInputs.add(trimResult.left);
      //noinspection ObjectEquality
      if (trimResult.left != input) {
        ++changeCount;
      }

      final Mapping inputMapping = trimResult.right;
      inputMappings.add(inputMapping);

      // move offset to point to start of next input
      offset += inputFieldCount;
      newFieldCount += inputMapping.getTargetCount();
    }

    final Mapping mapping =
        Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, newFieldCount);

    offset = 0;
    int newOffset = 0;
    for (final Mapping inputMapping : inputMappings) {
      ImmutableBitSet.Builder projBuilder = ImmutableBitSet.builder();
      for (final IntPair pair : inputMapping) {
        mapping.set(pair.source + offset, pair.target + newOffset);
      }

      offset += inputMapping.getSourceCount();
      newOffset += inputMapping.getTargetCount();
    }

    if (changeCount == 0 && mapping.isIdentity()) {
      result(multiJoin, Mappings.createIdentity(fieldCount));
    }

    // build new MultiJoin

    final RexVisitor<RexNode> inputFieldPermuter =
        new RexPermuteInputsShuttle(mapping, newInputs.toArray(new RelNode[0]));

    final RexNode newJoinFilter = originalJoinFilter.accept(inputFieldPermuter);

    // row type is simply re-mapped
    final List<RelDataTypeField> originalFieldList = multiJoin.getRowType().getFieldList();
    final RelDataType newRowType =
        new RelRecordType(
            StreamSupport.stream(mapping.spliterator(), false)
                .map(pair -> pair.source)
                .map(originalFieldList::get)
                .map(
                    originalField ->
                        new RelDataTypeFieldImpl(
                            originalField.getName(),
                            mapping.getTarget(originalField.getIndex()),
                            originalField.getType()))
                .collect(Collectors.toList()));

    final List<RexNode> newOuterJoinConditions =
        originalOuterJoinConditions.stream()
            .map(expr -> expr == null ? null : expr.accept(inputFieldPermuter))
            .collect(Collectors.toList());

    // see MultiJoin#getProjFields; ideally all input fields must be used, and this is a list of
    // "nulls"
    final List<ImmutableBitSet> newProjFields = Lists.newArrayList();

    for (final Ord<Mapping> inputMapping : Ord.zip(inputMappings)) {
      if (multiJoin.getProjFields().get(inputMapping.i) == null) {
        newProjFields.add(null);
        continue;
      }

      ImmutableBitSet.Builder projBuilder = ImmutableBitSet.builder();
      for (final IntPair pair : inputMapping.e) {
        if (multiJoin.getProjFields().get(inputMapping.i).get(pair.source)) {
          projBuilder.set(pair.target);
        }
      }
      newProjFields.add(projBuilder.build());
    }

    final ImmutableMap<Integer, ImmutableIntList> newJoinFieldRefCountsMap =
        computeJoinFieldRefCounts(newInputs, newFieldCount, newJoinFilter);

    final RexNode newPostJoinFilter =
        originalPostJoinFilter == null ? null : originalPostJoinFilter.accept(inputFieldPermuter);

    final MultiJoin newMultiJoin =
        new MultiJoin(
            multiJoin.getCluster(),
            newInputs,
            newJoinFilter,
            newRowType,
            multiJoin.isFullOuterJoin(),
            newOuterJoinConditions,
            multiJoin.getJoinTypes(),
            newProjFields,
            newJoinFieldRefCountsMap,
            newPostJoinFilter);

    return result(newMultiJoin, mapping);
  }

  public TrimResult trimFields(
      Window window, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    // Fields:
    //
    // Window rowtype
    // | input fields | agg functions |
    //
    // Rowtype used internally by the Window operator for groups:
    // | input fields | constants (hold by Window node) |
    //
    // Input rowtype:
    // | input fields |
    //
    // Trimming operations:
    // 1. If an agg is not used, don't include it in the group
    // 2. If a group is not used, remove the group
    // 3. If no group, skip the operator
    // 4. If a constant is not used, don't include it in the new window operator
    //
    // General description of the algorithm:
    // 1. Identify input fields and constants in use
    //    - input fields directly used by caller
    //    - input fields used by agg call, if agg call used by caller
    //    - input fields used by window group if at least one agg from the group is used by caller
    //    - constants used by call call, if agg call used by caller
    // 2. Trim input
    //    - only use list of used input fields (do not include constants) when calling for trimChild
    //      as it will confuse callee.
    // 3. Create new operator and final mapping
    //    - create a mapping combining input mapping and constants in use to rewrite expressions
    //    - if no agg is actually used by caller, return early with the new input and a copy of the
    //      input mapping matching the number of fields (skip the current operator)
    //    - Go over each group/agg call used by caller, and rewrite them by visiting them with the
    //      mapping combining input and constants
    //    - create a new window operator and return operator and mapping to caller

    final RelDataType rowType = window.getRowType();
    final int fieldCount = rowType.getFieldCount();

    final RelNode input = window.getInput();
    final int inputFieldCount = input.getRowType().getFieldCount();

    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>(extraFields);
    ImmutableBitSet.Builder inputBitSet = ImmutableBitSet.builder();

    //
    // 1. Identify input fields and constants in use
    //
    for (Integer bit : fieldsUsed) {
      if (bit >= inputFieldCount) {
        // exit if it goes over the input fields
        break;
      }
      inputBitSet.set(bit);
    }

    final RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(inputExtraFields, inputBitSet.build());
    inputBitSet = ImmutableBitSet.builder();

    // number of agg calls and agg calls actually used
    int aggCalls = 0;
    int aggCallsUsed = 0;

    // Capture which input fields and constants are used by the agg calls
    // thanks to the visitor
    for (final Window.Group group : window.groups) {
      boolean groupUsed = false;
      for (final RexWinAggCall aggCall : group.aggCalls) {
        int offset = inputFieldCount + aggCalls;
        aggCalls++;

        if (!fieldsUsed.get(offset)) {
          continue;
        }

        aggCallsUsed++;
        groupUsed = true;
        aggCall.accept(inputFinder);
      }

      // If no agg from the group are being used, do not include group fields
      if (!groupUsed) {
        continue;
      }

      group.lowerBound.accept(inputFinder);
      group.upperBound.accept(inputFinder);

      // Add partition fields
      inputBitSet.addAll(group.keys);

      // Add collation fields
      for (RelFieldCollation fieldCollation : group.collation().getFieldCollations()) {
        inputBitSet.set(fieldCollation.getFieldIndex());
      }
    }
    // Create the final bitset containing both input and constants used
    inputBitSet.addAll(inputFinder.build());
    final ImmutableBitSet inputAndConstantsFieldsUsed = inputBitSet.build();

    //
    // 2. Trim input
    //

    // Create input with trimmed columns. Need a bitset containing only input fields
    final ImmutableBitSet inputFieldsUsed =
        inputAndConstantsFieldsUsed.intersect(ImmutableBitSet.range(inputFieldCount));
    final int constantsUsed =
        inputAndConstantsFieldsUsed.cardinality() - inputFieldsUsed.cardinality();
    final TrimResult trimResult = trimChild(window, input, inputFieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input && fieldsUsed.cardinality() == fieldCount) {
      return result(window, Mappings.createIdentity(fieldCount));
    }

    //
    // 3. Create new operator and final mapping
    //
    // if new input cardinality is 0, create a dummy project
    // Note that the returned mapping is not a valid INVERSE_SURJECTION mapping
    // as it does not include a source for each target!
    if (inputFieldsUsed.cardinality() == 0) {
      final TrimResult dummyResult = dummyProject(inputFieldCount, newInput);
      newInput = dummyResult.left;
      inputMapping = dummyResult.right;
    }

    // Create a new input mapping which includes constants
    final int newInputFieldCount = newInput.getRowType().getFieldCount();
    final Mapping inputAndConstantsMapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            inputFieldCount + window.constants.size(),
            newInputFieldCount + constantsUsed);
    // include input mappping
    inputMapping.forEach(pair -> inputAndConstantsMapping.set(pair.source, pair.target));

    // Add constant mapping (while trimming list of constants)
    final List<RexLiteral> newConstants = new ArrayList<>();
    for (int i = 0; i < window.constants.size(); i++) {
      int index = inputFieldCount + i;
      if (inputAndConstantsFieldsUsed.get(index)) {
        inputAndConstantsMapping.set(index, newInputFieldCount + newConstants.size());
        newConstants.add(window.constants.get(i));
      }
    }

    // Create a new mapping. Include all the input fields
    final Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION, fieldCount, newInputFieldCount + aggCallsUsed);
    // Remap the field projects (1:1 remap)
    inputMapping.forEach(pair -> mapping.set(pair.source, pair.target));

    // Degenerated case: no agg calls being used, skip this operator!
    if (aggCallsUsed == 0) {
      return result(newInput, mapping);
    }

    // Create/Rewrite a new window operator by dropping unnecessary groups/agg calls and permuting
    // the inputs
    final RexPermuteInputsShuttle shuttle =
        new RexPermuteInputsShuttle(inputAndConstantsMapping, newInput);
    final List<Window.Group> newGroups = new ArrayList<>();

    int oldOffset = inputFieldCount;
    int newOffset = newInputFieldCount;
    for (final Window.Group group : window.groups) {
      final List<RexWinAggCall> newCalls = new ArrayList<>();
      for (final RexWinAggCall agg : group.aggCalls) {
        if (!fieldsUsed.get(oldOffset)) {
          // Skip unused aggs
          oldOffset++;
          continue;
        }

        RexWinAggCall newCall = permuteWinAggCall(agg, shuttle, newCalls.size());
        newCalls.add(newCall);
        mapping.set(oldOffset, newOffset);
        oldOffset++;
        newOffset++;
      }

      // If no agg from the group, let's skip the group
      if (newCalls.isEmpty()) {
        continue;
      }

      final RexWindowBound newLowerBound = group.lowerBound.accept(shuttle);
      final RexWindowBound newUpperBound = group.upperBound.accept(shuttle);

      // Remap partition fields
      final ImmutableBitSet newKeys = Mappings.apply(inputAndConstantsMapping, group.keys);

      // Remap collation fields
      final List<RelFieldCollation> newFieldCollations = new ArrayList<>();
      for (RelFieldCollation fieldCollation : group.collation().getFieldCollations()) {
        newFieldCollations.add(
            fieldCollation.copy(
                inputAndConstantsMapping.getTarget(fieldCollation.getFieldIndex())));
      }

      final Window.Group newGroup =
          new Window.Group(
              newKeys,
              group.isRows,
              newLowerBound,
              newUpperBound,
              RelCollations.of(newFieldCollations),
              newCalls);
      newGroups.add(newGroup);
    }
    final Mapping permutationMapping;

    // If no input column being used, still need to include the dummy column in the row type
    // by temporarily adding it to the mapping
    if (inputFieldsUsed.cardinality() != 0) {
      permutationMapping = mapping;
    } else {
      permutationMapping =
          Mappings.create(
              MappingType.INVERSE_SURJECTION, mapping.getSourceCount(), mapping.getTargetCount());
      mapping.forEach(pair -> permutationMapping.set(pair.source, pair.target));
      // set a fake mapping for the dummy project
      permutationMapping.set(0, 0);
    }
    final RelDataType newRowType =
        RelOptUtil.permute(window.getCluster().getTypeFactory(), rowType, permutationMapping);

    // TODO: should there be a relbuilder for window?
    if (window instanceof LogicalWindow) {
      return result(
          LogicalWindow.create(window.getTraitSet(), newInput, newConstants, newRowType, newGroups),
          mapping);
    }

    if (window instanceof WindowRel) {
      return result(
          WindowRel.create(
              window.getCluster(),
              window.getTraitSet(),
              newInput,
              newConstants,
              newRowType,
              newGroups),
          mapping);
    }

    return result(window, mapping);
  }

  private static RexWinAggCall permuteWinAggCall(
      RexWinAggCall call, RexPermuteInputsShuttle shuttle, int newOrdinal) {
    // Cast should be safe as the shuttle creates new RexCall instance (but not RexWinAggCall)
    RexCall newCall = (RexCall) call.accept(shuttle);
    if (newCall == call && newOrdinal == call.ordinal) {
      return call;
    }

    return new RexWinAggCall(
        (SqlAggFunction) call.getOperator(),
        call.getType(),
        newCall.getOperands(),
        // remap partition ordinal
        newOrdinal,
        call.distinct);
  }

  /**
   * Compute the reference counts of fields in the inputs from the new join condition.
   *
   * @param inputs inputs into the new MultiJoin
   * @param totalFieldCount total number of fields in the MultiJoin
   * @param joinCondition the new join condition
   * @return Map containing the new join condition
   */
  private static ImmutableMap<Integer, ImmutableIntList> computeJoinFieldRefCounts(
      final List<RelNode> inputs, final int totalFieldCount, final RexNode joinCondition) {
    // count the input references in the join condition
    final int[] joinCondRefCounts = new int[totalFieldCount];
    joinCondition.accept(new InputReferenceCounter(joinCondRefCounts));

    final Map<Integer, int[]> refCountsMap = Maps.newHashMap();
    final int numInputs = inputs.size();
    int currInput = 0;
    for (final RelNode input : inputs) {
      refCountsMap.put(currInput++, new int[input.getRowType().getFieldCount()]);
    }

    // add on to the counts for each input into the MultiJoin the
    // reference counts computed for the current join condition
    currInput = -1;
    int startField = 0;
    int inputFieldCount = 0;
    for (int i = 0; i < totalFieldCount; i++) {
      if (joinCondRefCounts[i] == 0) {
        continue;
      }
      while (i >= (startField + inputFieldCount)) {
        startField += inputFieldCount;
        currInput++;
        assert currInput < numInputs;
        inputFieldCount = inputs.get(currInput).getRowType().getFieldCount();
      }
      final int[] refCounts = refCountsMap.get(currInput);
      refCounts[i - startField] += joinCondRefCounts[i];
    }

    final ImmutableMap.Builder<Integer, ImmutableIntList> builder = ImmutableMap.builder();
    for (final Map.Entry<Integer, int[]> entry : refCountsMap.entrySet()) {
      builder.put(entry.getKey(), ImmutableIntList.of(entry.getValue()));
    }
    return builder.build();
  }

  /**
   * Visitor that keeps a reference count of the inputs used by an expression.
   *
   * <p>Duplicates {@link org.apache.calcite.rel.rules.JoinToMultiJoinRule.InputReferenceCounter}.
   */
  private static class InputReferenceCounter extends RexVisitorImpl<Void> {
    private final int[] refCounts;

    InputReferenceCounter(int[] refCounts) {
      super(true);
      this.refCounts = refCounts;
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
      refCounts[inputRef.getIndex()]++;
      return null;
    }
  }

  /** Enum to represent which side of the join we are projecting from. */
  private enum ProjectJoinType {
    /**
     * Exclusively projecting from the LEFT hand side of the JOIN.
     *
     * <p>SELECT a.* FROM a JOIN b
     */
    LEFT,

    /**
     * Exclusively projecting from the RIGHT hand side of the JOIN.
     *
     * <p>SELECT b.* FROM a JOIN b
     */
    RIGHT,

    /**
     * Projecting from both sides of the JOIN
     *
     * <p>SELECT * FROM a JOIN b
     */
    BOTH,

    /**
     * Projecting from neither side of the JOIN (no ref indexes in the projection)
     *
     * <p>SELECT COUNT(*), 'asdf', UPPER('foo') FROM a JOIN b
     */
    NEITHER
  }
}
