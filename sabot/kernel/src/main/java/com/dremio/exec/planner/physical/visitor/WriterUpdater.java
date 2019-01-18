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
package com.dremio.exec.planner.physical.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionType;
import com.dremio.exec.planner.physical.DistributionTraitDef;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectAllowDupPrel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.SortPrel;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.planner.sql.SqlOperatorImpl;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Insert additional operators before writing to impose various types of operations including:
 *  - Create a hashed value to use for sorting when doing DISTRIBUTE BY
 *  - Sorting the data (on DISTRIBUTE BY as well as the local sort field(s))
 *  - Create a change detection field to create separate files for partitions.
 */
public class WriterUpdater extends BasePrelVisitor<Prel, Void, RuntimeException> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WriterUpdater.class);

  private static final WriterUpdater INSTANCE = new WriterUpdater();

  private WriterUpdater(){}

  public static Prel update(Prel prel) {
    return prel.accept(INSTANCE, null);
  }

  private Prel renameAsNecessary(RelDataType expectedRowType, Prel initialInput) {
    if(RelOptUtil.areRowTypesEqual(initialInput.getRowType(), expectedRowType, false) && !RelOptUtil.areRowTypesEqual(initialInput.getRowType(), expectedRowType, true)) {
      final List<RexNode> refs = new ArrayList<>();
      final List<RelDataTypeField> fields = expectedRowType.getFieldList();
      final RexBuilder rb = initialInput.getCluster().getRexBuilder();
      for(int i = 0; i < expectedRowType.getFieldCount(); i++) {
        refs.add(rb.makeInputRef(fields.get(i).getType(), i));
      }
      return new ProjectPrel(initialInput.getCluster(), initialInput.getTraitSet(), initialInput, refs, expectedRowType);
    } else {
      return initialInput;
    }
  }

  @Override
  public Prel visitWriter(WriterPrel initialPrel, Void value) throws RuntimeException {
    final WriterOptions options = initialPrel.getCreateTableEntry().getOptions();
    final Prel initialInput = ((Prel) initialPrel.getInput()).accept(this, null);

    final Prel input = renameAsNecessary(initialPrel.getExpectedInboundRowType(), initialInput);
    final WriterPrel prel = (WriterPrel) initialPrel.copy(initialPrel.getTraitSet(), ImmutableList.<RelNode>of(input));

    if(options.hasDistributions()){

      // we need to add a new hash value field
      // TODO: make this happen in tandem with the distribution hashing as opposed to separate).
      DistributionTrait distribution = input.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
      if(distribution.getType() != DistributionType.HASH_DISTRIBUTED){
        throw UserException.planError().message("Tried to plan a distribution writer but distribution was incorrect.").build(logger);
      }

      if(distribution.getFields().size() != options.getDistributionColumns().size()){
        // TODO: add check
      }

      Prel project = HashPrelUtil.addHashProject(distribution.getFields(), input, options.getRingCount());

      // then add a sort that is the hash field followed by the sort
      List<Integer> sortKeys = new ArrayList<>();
      // last column is the hash modulo expression.
      sortKeys.add(project.getRowType().getFieldCount() - 1);

      // if partitions, add those to sort.
      final Set<Integer> sortedKeys = Sets.newHashSet();
      if (options.hasPartitions()){
        List<Integer> partitionKeys = getFieldIndices(options.getPartitionColumns(), project.getRowType());
        sortKeys.addAll(partitionKeys);
        sortedKeys.addAll(partitionKeys);
      }

      // if sorted, add those as well.
      if (options.hasSort()) {
        List<Integer> sortRequestKeys = getFieldIndices(options.getSortColumns(), project.getRowType());
        for(Integer key : sortRequestKeys){
          if(sortedKeys.contains(key)){
            logger.warn("Rejecting sort key {} since it is already included in partition clause.", key);
            continue;
          }
          sortKeys.add(key);
        }
      }

      final RelCollation collation = getCollation(prel.getTraitSet(), sortKeys);

      final Prel sort = new SortPrel(project.getCluster(), project.getTraitSet().plus(collation), project, collation);

      List<Integer> fieldIndices = new ArrayList<>();
      // add bucket field.
      fieldIndices.add(sort.getRowType().getFieldCount() - 1);
      if(options.hasPartitions()){
        fieldIndices.addAll(getFieldIndices(options.getPartitionColumns(), input.getRowType()));
      }

      final Prel changeDetection = addChangeDetectionProject(sort, fieldIndices);

      final WriterPrel writer = new WriterPrel(prel.getCluster(), prel.getTraitSet(), changeDetection, prel.getCreateTableEntry(), prel.getExpectedInboundRowType());
      return writer;

    } else if(options.hasPartitions()) {
      List<Integer> sortKeys = new ArrayList<>();

      // sort by partitions.
      final Set<Integer> sortedKeys = Sets.newHashSet();
      List<Integer> partitionKeys = getFieldIndices(options.getPartitionColumns(), input.getRowType());
      sortKeys.addAll(partitionKeys);
      sortedKeys.addAll(partitionKeys);

      // then sort by sort keys, if available.
      if (options.hasSort()) {
        List<Integer> sortRequestKeys = getFieldIndices(options.getSortColumns(), input.getRowType());
        for(Integer key : sortRequestKeys){
          if(sortedKeys.contains(key)){
            logger.warn("Rejecting sort key {} since it is already included in partition clause.", key);
            continue;
          }
          sortKeys.add(key);
        }
      }

      final RelCollation collation = getCollation(prel.getTraitSet(), sortKeys);
      final Prel sort = new SortPrel(input.getCluster(), input.getTraitSet().plus(collation), input, collation);

      // we need to sort by the partitions.
      final Prel changeDetectionPrel = addChangeDetectionProject(sort, getFieldIndices(options.getPartitionColumns(), input.getRowType()));
      final WriterPrel writer = new WriterPrel(prel.getCluster(), prel.getTraitSet(), changeDetectionPrel, prel.getCreateTableEntry(), prel.getExpectedInboundRowType());
      return writer;

    } else if(options.hasSort()){
      // no partitions or distributions.
      // insert a sort on sort fields.
      final RelCollation collation = getCollation(prel.getTraitSet(), getFieldIndices(options.getSortColumns(), input.getRowType()));
      final Prel sort = new SortPrel(input.getCluster(), input.getTraitSet().plus(collation), input, collation);
      final WriterPrel writer = new WriterPrel(prel.getCluster(), prel.getTraitSet(), sort, prel.getCreateTableEntry(), prel.getExpectedInboundRowType());
      return writer;

    } else {
      return prel;
    }
  }

  private static RelCollation getCollation(RelTraitSet set, List<Integer> keys) {
    return set.canonize(RelCollations.of(FluentIterable.from(keys)
        .transform(new Function<Integer, RelFieldCollation>() {
          @Override
          public RelFieldCollation apply(Integer input) {
            return new RelFieldCollation(input);
          }
        }).toList()));
  }

  public static List<Integer> getFieldIndices(final List<String> columns, final RelDataType inputRowType) {
    return FluentIterable.from(columns)
        .transform(new Function<String, Integer>() {
          @Override
          public Integer apply(String input) {
            return Preconditions.checkNotNull(inputRowType.getField(input, false, false),
                String.format("Partition column '%s' could not be resolved in the table's column lists", input))
                .getIndex();
          }
        }).toList();
  }


  /**
   * A PrelVisitor which will insert a project under Writer.
   *
   * For CTAS : create table t1 partition by (con_A) select * from T1;
   *   A Project with Item expr will be inserted, in addition to *.  We need insert another Project to remove
   *   this additional expression.
   *
   * In addition, to make execution's implementation easier,  a special field is added to Project :
   *     PARTITION_COLUMN_IDENTIFIER = newPartitionValue(Partition_colA)
   *                                    || newPartitionValue(Partition_colB)
   *                                    || ...
   *                                    || newPartitionValue(Partition_colN).
   */
  /**
   * Given a list of index keys, generate an additional project that detects change in the index keys.
   * @return
   * @throws RuntimeException
   */
  public Prel addChangeDetectionProject(final Prel input, final List<Integer> changeKeys) throws RuntimeException {

    // No partition columns.
    if(changeKeys.isEmpty()) {
      return input;
    }

    final RelDataType childRowType = input.getRowType();
    final RelOptCluster cluster = input.getCluster();
    final List<RexNode> exprs = new ArrayList<>();
    final List<String> fieldNames = new ArrayList<>();

    for (final RelDataTypeField field : childRowType.getFieldList()) {
      exprs.add(RexInputRef.of(field.getIndex(), childRowType));
      fieldNames.add(field.getName());
    }


    // find list of partition columns.
    final List<RexNode> partitionColumnExprs = Lists.newArrayListWithExpectedSize(changeKeys.size());
    final List<RelDataTypeField> fields = childRowType.getFieldList();
    for (Integer changeKey : changeKeys) {
      RelDataTypeField field = fields.get(changeKey);
      if (field == null) {
        throw UserException.validationError()
            .message("Partition column %s is not in the SELECT list of CTAS!", changeKey)
            .build(logger);
      }

      partitionColumnExprs.add(RexInputRef.of(field.getIndex(), childRowType));
    }

    // Add partition column comparator to Project's field name list.
    fieldNames.add(WriterPrel.PARTITION_COMPARATOR_FIELD);

    final RexNode partionColComp = createPartitionColComparator(cluster.getRexBuilder(), partitionColumnExprs);
    exprs.add(partionColComp);
    final RelDataType rowTypeWithPCComp = RexUtil.createStructType(cluster.getTypeFactory(), exprs, fieldNames);

    final ProjectPrel projectUnderWriter = new ProjectAllowDupPrel(cluster, cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL), input, exprs, rowTypeWithPCComp);
    return projectUnderWriter;
  }

  private static RexNode createPartitionColComparator(final RexBuilder rexBuilder, List<RexNode> inputs) {

    final SqlOperatorImpl op = new SqlOperatorImpl(WriterPrel.PARTITION_COMPARATOR_FUNC, 1, true);

    final List<RexNode> compFuncs = Lists.newArrayListWithExpectedSize(inputs.size());

    for (final RexNode input : inputs) {
      compFuncs.add(rexBuilder.makeCall(op, ImmutableList.of(input)));
    }

    return composeDisjunction(rexBuilder, compFuncs);
  }

  private static RexNode composeDisjunction(final RexBuilder rexBuilder, List<RexNode> compFuncs) {
    final SqlOperatorImpl booleanOrFunc = new SqlOperatorImpl("orNoShortCircuit", 2, true);
    RexNode node = compFuncs.remove(0);
    while (!compFuncs.isEmpty()) {
      node = rexBuilder.makeCall(booleanOrFunc, node, compFuncs.remove(0));
    }
    return node;
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {

    List<RelNode> newInputs = new ArrayList<>();
    for(Prel input : prel){
      newInputs.add(input.accept(this, null));
    }

    return (Prel) prel.copy(prel.getTraitSet(), newInputs);
  }


}
