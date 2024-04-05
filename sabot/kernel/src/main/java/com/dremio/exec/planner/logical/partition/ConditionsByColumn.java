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
package com.dremio.exec.planner.logical.partition;

import static com.dremio.common.expression.CompleteType.fromMajorType;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.store.iceberg.FieldIdBroker;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;

class ConditionsByColumn extends SargPrunableEvaluator {
  public ConditionsByColumn(final PartitionStatsBasedPruner pruner) {
    super(pruner);
  }

  private final Multimap<String, Condition> queryConditions = HashMultimap.create();

  void addCondition(final String colName, final Condition condition) {
    queryConditions.put(colName, condition);
  }

  private void addCondition(
      final Function<RexNode, List<Integer>> usedIndexes,
      final List<SchemaPath> projectedColumns,
      final RexCall condition,
      final FindSimpleFilters.StateHolder input,
      final FindSimpleFilters.StateHolder literal) {
    final int colIndex = usedIndexes.apply(input.getNode()).stream().findFirst().get();
    final String colName = projectedColumns.get(colIndex).getAsUnescapedPath();
    final Comparable<?> value = getValueFromFilter(colName, literal.getNode());

    // handle partition transformations
    // value might be transformed and sqlKind adjusted using the partition transformation
    final Condition adjustedCondition =
        getPartitionTransformAdjustedCondition(colName, value, condition.getKind());
    if (adjustedCondition != null) {
      this.addCondition(colName, adjustedCondition);
    }
  }

  /**
   * Applies an Iceberg transform to a constant so that it can be used in comparison with values
   * from partition stats Does nothing if the transformation is identity Otherwise it will transform
   * the constant, in a way that it can be compared to the partition stats (which are already
   * transformed) Sometimes the sqlKind is changed to include equality so that the boundary
   * partition is matched We have 2 options how to apply the transform, transform the constant or
   * try to reverse the transform and reverse transform the partition stat values However, we might
   * have hundreds of thousands or even a million partition stat values, so it is better to
   * transform the constant just once for performance reasons In addition, some transforms cannot be
   * reversed
   *
   * @param colName Column name
   * @param oldValue Input value from the Condition
   * @param oldKind Old kind before the adjustment
   * @return the transformed value to use for comparison with the partition stats
   */
  private Condition getPartitionTransformAdjustedCondition(
      final String colName, final Comparable<?> oldValue, final SqlKind oldKind) {
    final Integer indexInPartitionSpec =
        getPruner().getPartitionColNameToSpecColIdMap().get(colName);
    final PartitionField partitionField =
        getPruner().getPartitionSpec().fields().get(indexInPartitionSpec);
    final Transform transform = partitionField.transform();
    if (transform.isIdentity()) {
      // We don't do anything special for identity, just return the oldValue
      return new Condition(oldKind, oldValue);
    }
    boolean isSupportedTransform = (oldKind == EQUALS || transform.preservesOrder());
    if (!isSupportedTransform) {
      // We cannot support this case as we have comparison operator other than EQUALS, and non-order
      // preserving function
      // Example LESS_THAN with Bucket function
      // we return null here, so the condition is ignored for pruning purposes
      // it is possible that other conditions are still applied and pruning is still possible
      return null;
    }
    SqlKind transformedKind = oldKind;
    final TypeProtos.MajorType majorType = getPruner().partitionColNameToTypeMap.get(colName);
    final SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
    final Type inputType =
        schemaConverter.toIcebergType(
            fromMajorType(majorType), null, new FieldIdBroker.UnboundedFieldIdBroker());

    // special handle milli to micro seconds
    final Comparable<?> icebergAdjustedValue =
        (Comparable<?>) IcebergUtils.toIcebergValue(oldValue, majorType);

    // for equality or order preserving transforms, we will just transform the constant
    final Comparable<?> transformedValue =
        (Comparable<?>) transform.bind(inputType).apply(icebergAdjustedValue);

    // additional work is needed for comparison operators LESS_THAN and GREATER_THAN to make sure we
    // handle the boundary partition case
    switch (oldKind) {
      case LESS_THAN:
        // example
        // Transform = year
        // constant = 2023 01 01 7:15 am
        // transformed value is in effect 2023, however, we don't want to apply "partition_value <
        // 2023"
        // the year 2023 partition should be selected, because at least some of our rows are in 2023
        // So we change the condition to "partition_value <= 2023", to make sure 2023 is selected
        transformedKind = LESS_THAN_OR_EQUAL;
        break;
      case GREATER_THAN:
        // example
        // Transform = year
        // constant = 2023 01 01 7:15 am
        // value is now 2023, however, we don't want to apply "partition_value > 2023"
        // the year 2023 should be selected, because at least some of our rows are in 2023
        // so we change the condition to  "partition_value >= 2023", to make sure 2023 is selected
        transformedKind = GREATER_THAN_OR_EQUAL;
        break;
      default:
    }
    return new Condition(transformedKind, transformedValue);
  }

  @Override
  public boolean hasConditions() {
    return !queryConditions.isEmpty();
  }

  private Comparable getValueFromFilter(final String colName, final RexNode node) {
    final TypeProtos.MinorType minorType =
        getPruner().partitionColNameToTypeMap.get(colName).getMinorType();
    final RexLiteral literal = (RexLiteral) node;
    switch (minorType) {
      case BIT:
        return literal.getValueAs(Boolean.class);
      case INT:
      case DATE:
      case TIME:
        return literal.getValueAs(Integer.class);
      case BIGINT:
      case TIMESTAMP:
        return literal.getValueAs(Long.class);
      case FLOAT4:
        return literal.getValueAs(Float.class);
      case FLOAT8:
        return literal.getValueAs(Double.class);
      case VARCHAR:
        return literal.getValueAs(String.class);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + minorType);
    }
  }

  boolean satisfiesComparison(final String colName, final Comparable valueFromPartitionData) {
    for (final Condition condition : queryConditions.get(colName)) {
      if (valueFromPartitionData == null || !condition.matches(valueFromPartitionData)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isRecordMatch(final StructLike partitionData) {
    for (final String colName : getColumnNames()) {
      final Integer indexInPartitionSpec =
          getPruner().getPartitionColNameToSpecColIdMap().get(colName);
      final Comparable<?> valueFromPartitionData =
          getValueFromPartitionData(getPruner(), indexInPartitionSpec, colName, partitionData);
      if (!satisfiesComparison(colName, valueFromPartitionData)) {
        return false;
      }
    }
    return true;
  }

  public Set<String> getColumnNames() {
    return queryConditions.keySet();
  }

  private Comparable getValueFromPartitionData(
      final PartitionStatsBasedPruner partitionStatsBasedPruner,
      final int indexInPartitionSpec,
      final String colName,
      final StructLike partitionData) {
    final TypeProtos.MajorType majorType =
        partitionStatsBasedPruner.getPartitionColNameToPartitionFunctionOutputType().get(colName);
    switch (majorType.getMinorType()) {
      case BIT:
        return partitionData.get(indexInPartitionSpec, Boolean.class);
      case INT:
      case DATE:
        return partitionData.get(indexInPartitionSpec, Integer.class);
      case BIGINT:
        return partitionData.get(indexInPartitionSpec, Long.class);
      case TIME:
      case TIMESTAMP:
        // Divide by 1000 to convert microseconds to millis
        return partitionData.get(indexInPartitionSpec, Long.class) / 1000;
      case FLOAT4:
        return partitionData.get(indexInPartitionSpec, Float.class);
      case FLOAT8:
        return partitionData.get(indexInPartitionSpec, Double.class);
      case VARBINARY:
        return partitionData.get(indexInPartitionSpec, ByteBuffer.class);
      case FIXEDSIZEBINARY:
      case VARCHAR:
        return partitionData.get(indexInPartitionSpec, String.class);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + majorType);
    }
  }

  public static ConditionsByColumn buildSargPrunableConditions(
      final PartitionStatsBasedPruner partitionStatsBasedPruner,
      final Function<RexNode, List<Integer>> usedIndexes,
      final List<SchemaPath> projectedColumns,
      final FindSimpleFilters rexVisitor,
      final ImmutableList<RexCall> rexConditions) {
    final ConditionsByColumn conditionsByColumn = new ConditionsByColumn(partitionStatsBasedPruner);
    List<UnprocessedCondition> unprocessedConditions =
        buildUnprocessedConditions(rexConditions, rexVisitor);
    unprocessedConditions.stream()
        .forEachOrdered(
            x ->
                conditionsByColumn.addCondition(
                    usedIndexes,
                    projectedColumns,
                    x.getCondition(),
                    x.getColumn(),
                    x.getConstant()));
    return conditionsByColumn;
  }

  class Condition {

    private final Predicate<Comparable> matcher;

    Condition(final SqlKind sqlKind, final Comparable<?> valueFromCondition) {
      switch (sqlKind) {
        case EQUALS:
          matcher =
              valueFromPartitionData -> valueFromPartitionData.compareTo(valueFromCondition) == 0;
          break;
        case LESS_THAN:
          matcher =
              valueFromPartitionData -> valueFromPartitionData.compareTo(valueFromCondition) < 0;
          break;
        case LESS_THAN_OR_EQUAL:
          matcher =
              valueFromPartitionData -> valueFromPartitionData.compareTo(valueFromCondition) <= 0;
          break;
        case GREATER_THAN:
          matcher =
              valueFromPartitionData -> valueFromPartitionData.compareTo(valueFromCondition) > 0;
          break;
        case GREATER_THAN_OR_EQUAL:
          matcher =
              valueFromPartitionData -> valueFromPartitionData.compareTo(valueFromCondition) >= 0;
          break;
        default:
          throw new IllegalStateException("Unsupported SQL operator type: " + sqlKind);
      }
    }

    boolean matches(final Comparable valueFromPartitionData) {
      return matcher.test(valueFromPartitionData);
    }
  }
}
