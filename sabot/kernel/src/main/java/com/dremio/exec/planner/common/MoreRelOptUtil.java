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
package com.dremio.exec.planner.common;

import static com.dremio.exec.planner.common.ScanRelBase.getRowTypeFromProjectedColumns;
import static java.lang.Math.max;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_TYPES;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelHintsPropagator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;

import com.carrotsearch.hppc.IntIntHashMap;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.resolver.TypeCastRules;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.parquet.ParquetTypeHelper;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * Utility class that is a subset of the RelOptUtil class and is a placeholder for Dremio specific
 * static methods that are needed during either logical or physical planning.
 */
public final class MoreRelOptUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MoreRelOptUtil.class);

  private MoreRelOptUtil() {}

  /**
   * Finds all columns used by {@link RexInputRef} in a {@link RexNode}.
   * @param rexNode {@link RexNode} to find inputs for
   * @return set of columns used by the rexNode
   */
  public static ImmutableBitSet findColumnsUsed(RexNode rexNode) {
    ImmutableBitSet.Builder inputs = ImmutableBitSet.builder();
    rexNode.accept(new RexVisitorImpl<Void>(true){
      @Override public Void visitInputRef(RexInputRef inputRef) {
        inputs.set(inputRef.getIndex());
        return super.visitInputRef(inputRef);
      }
    });
    return inputs.build();
  }

  public static ImmutableBitSet buildColumnSet (
    RelNode filterableScan,
    Collection<String> columnNames) {
    if (columnNames == null) {
      return ImmutableBitSet.of();
    }

    final List<String> fieldNames = filterableScan.getRowType().getFieldNames();

    final Map<String, Integer> fieldMap = IntStream.range(0, fieldNames.size())
      .boxed()
      .collect(Collectors.toMap(fieldNames::get, i->i));


    final ImmutableBitSet.Builder partitionColumnBitSetBuilder = ImmutableBitSet.builder();

    for (String field : columnNames) {
      int partitionIndex = fieldMap.getOrDefault(field, -1);
      if (-1 != partitionIndex) {
        partitionColumnBitSetBuilder.set(partitionIndex);
      }
    }
    return partitionColumnBitSetBuilder.build();
  }

  /**
   * Returns a set of columns from the relNode that are primitive type.
   */
  public static ImmutableBitSet buildParquetPrimitiveTypeSetForColumns(RelNode relNode){
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    RelDataType rowType = relNode.getRowType();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      if (isParquetPrimitiveType(rowType.getFieldList().get(i).getType())){
        builder.set(i);
      }
    }
    return builder.build();
  }

  /**
   * Returns true if the type is primitive.
   */
  public static boolean isParquetPrimitiveType(RelDataType type) {
    return null != ParquetTypeHelper.getPrimitiveTypeNameForMinorType(
      TypeInferenceUtils.getMinorTypeFromCalciteType(type));
  }

  /**
   *
   * @param invalidColumns - set of invalid columns.
   * @param rexNode - predicate to evaluate
   * @return true if the rex call does not contain any invalid columns and the data types of operands are equal
   * without the need of casting. Otherwise, it returns false.
   */
  public static boolean checkIsValidComparison(ImmutableBitSet invalidColumns, RexNode rexNode) {
    if (!(rexNode instanceof RexCall)) {
      return false;
    }

    switch (rexNode.getKind()) {
      case LESS_THAN:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
      case NOT_EQUALS:
      case EQUALS: {
        RexCall rexCall = (RexCall)rexNode;
        RexNode operand1 = rexCall.getOperands().get(0);
        RexNode operand2 = rexCall.getOperands().get(1);
        final RelDataType leftType = operand1.getType();
        final RelDataType rightType = operand2.getType();
        if (!MoreRelOptUtil.areDataTypesEqualWithoutRequiringCast(leftType, rightType)) {
          return false;
        }
        return (operand1 instanceof RexInputRef && operand2 instanceof RexLiteral
          && !invalidColumns.get(((RexInputRef)operand1).getIndex()))
          || (operand2 instanceof RexInputRef && operand1 instanceof RexLiteral
          && !invalidColumns.get(((RexInputRef)operand2).getIndex()));
      }
      default:
        return false;
    }
  }

  /**
   * Checks if a {@link RexNode} does not contains a {@link RexCorrelVariable},
   * {@link RexFieldAccess}, or a non deterministic function.
   */
  public static boolean isNotComplexFilter(RexNode rexNode) {
    return rexNode.accept(new ComplexityChecker());
  }

  private static class ComplexityChecker extends RexVisitorImpl<Boolean> {
    protected ComplexityChecker() {
      super(true);
    }

    @Override public Boolean visitInputRef(RexInputRef rexInputRef) {
      return true;
    }

    @Override public Boolean visitLiteral(RexLiteral rexLiteral) {
      return true;
    }

    @Override public Boolean visitCall(RexCall rexCall) {
      return !rexCall.getOperator().isDynamicFunction()
        && rexCall.getOperator().isDeterministic()
        && (rexCall.getOperands().isEmpty()
        || rexCall.getOperands().stream().allMatch(op -> op.accept(this)));
    }

    @Override public Boolean visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
      return false;
    }

    @Override public Boolean visitFieldAccess(RexFieldAccess rexFieldAccess) {
      return false;
    }

    @Override public Boolean visitLocalRef(RexLocalRef rexLocalRef) {
      throw new IllegalArgumentException("Unexpected RexLocalRef");
    }

    @Override public Boolean visitDynamicParam(RexDynamicParam rexDynamicParam) {
      throw new IllegalArgumentException("Unexpected RexDynamicParam");
    }

    @Override public Boolean visitRangeRef(RexRangeRef rexRangeRef) {
      throw new IllegalArgumentException("Unexpected RexRangeRef");
    }

    @Override public Boolean visitSubQuery(RexSubQuery rexSubQuery) {
      throw new IllegalArgumentException("Unexpected RexSubQuery");
    }

    @Override public Boolean visitTableInputRef(RexTableInputRef rexTableInputRef) {
      throw new IllegalArgumentException("Unexpected RexTableInputRef");
    }

    @Override public Boolean visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
      throw new IllegalArgumentException("Unexpected RexPatternFieldRef");
    }
  }

  /**
   * Computes the height of the rel tree under the input rel node.
   * @param rel RelNode to compute the minimum height of the tree underneath it
   * @return minimum height of the tree under the input rel node
   */
  public static int getDepth(RelNode rel) {
    if (rel == null) {
      return 0;
    }
    if (rel instanceof RelSubset) {
      RelSubset subset = (RelSubset) rel;
      return getDepth(subset.getBest());
    }

    if (rel.getInputs() == null || rel.getInputs().size() == 0) {
      return 1;
    }

    int minDepth = Integer.MAX_VALUE;
    for (RelNode node : rel.getInputs()) {
      int nodeDepth = getDepth(node);
      if (nodeDepth > 0) {
        minDepth = Math.min(nodeDepth, minDepth);
      }
    }

    if (minDepth == Integer.MAX_VALUE) {
      return 0;
    }

    return minDepth + 1;
  }

  public static int countJoins(RelNode relNode) {
    return countRelNodes(relNode, Join.class::isInstance);
  }

  public static int countRelNodes(RelNode rel, Predicate<RelNode> relNodePredicate) {
    int sum = relNodePredicate.test(rel) ? 1 : 0;
    for (RelNode sub : rel.getInputs()) {
      sum += countRelNodes(sub, relNodePredicate);
    }
    return sum;
  }

  public static int countRelNodes(RelNode rel) {
    int count = 1; // Current node
    for (RelNode child : rel.getInputs()) { // Add children
      count += countRelNodes(child);
    }
    return count;
  }


  /**
   * Given a view SQL, is the Calcite validated row type of this SQL equal to the Arrow batch type
   * last seen for this SQL?
   */
  public static boolean areRowTypesEqualForViewSchemaLearning(RelDataType calciteRowType, RelDataType arrowRowType) {
    if (arrowRowType.getFieldCount() != calciteRowType.getFieldCount()) {
      return false;
    }
    final List<RelDataTypeField> f1 = calciteRowType.getFieldList();
    final List<RelDataTypeField> f2 = arrowRowType.getFieldList();
    for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
      final RelDataType type1 = pair.left.getType();
      final RelDataType type2 = pair.right.getType();
      if (type1.getSqlTypeName() == SqlTypeName.NULL) {
        continue; // Arrow batch schema does not retain null type
      }
      // Arrow batch schema does not retain nullability or precision so we can only compare type names
      if (!type1.getSqlTypeName().equals(type2.getSqlTypeName())) {
        return false;
      }
      // Field names must always match
      if (!pair.left.getName().equalsIgnoreCase(pair.right.getName())) {
        return false;
      }
    }
    return true;
  }

  public static boolean areRowTypesCompatibleForInsert(
    RelDataType rowType1,
    RelDataType rowType2,
    boolean compareNames,
    boolean allowSubstring) {
    return checkRowTypesCompatibility(rowType1, rowType2, compareNames, allowSubstring, true);
  }

  public static boolean areRowTypesCompatible(
    RelDataType rowType1,
    RelDataType rowType2,
    boolean compareNames,
    boolean allowSubstring) {
    return checkRowTypesCompatibility(rowType1, rowType2, compareNames, allowSubstring, false);
  }

  public static boolean checkFieldTypesCompatibility(
    RelDataType fieldType1,
    RelDataType fieldType2,
    boolean allowSubstring,
    boolean insertOp) {
    // If one of the types is ANY comparison should succeed
    if (fieldType1.getSqlTypeName() == SqlTypeName.ANY
      || fieldType2.getSqlTypeName() == SqlTypeName.ANY) {
      return true;
    }

    if ((fieldType1.toString().equals(fieldType2.toString()))) {
      return true;
    }

    if (allowSubstring
      && (fieldType1.getSqlTypeName() == SqlTypeName.CHAR && fieldType2.getSqlTypeName() == SqlTypeName.CHAR)
      && (fieldType1.getPrecision() <= fieldType2.getPrecision())) {
      return true;
    }

    // Check if Dremio implicit casting can resolve the incompatibility
    List<TypeProtos.MinorType> types = Lists.newArrayListWithCapacity(2);
    TypeProtos.MinorType minorType1 = Types.getMinorTypeFromName(fieldType1.getSqlTypeName().getName());
    TypeProtos.MinorType minorType2 = Types.getMinorTypeFromName(fieldType2.getSqlTypeName().getName());
    types.add(minorType1);
    types.add(minorType2);
    if (insertOp) {
      // Insert is more strict than normal select in terms of implicit casts
      // Return false if TypeCastRules do not allow implicit cast
      if (TypeCastRules.isCastable(minorType1, minorType2, true) &&
        TypeCastRules.getLeastRestrictiveTypeForInsert(types) != null) {
        if (TypeCastRules.isCastSafeFromDataTruncation(fieldType1, fieldType2)) {
          return true;
        }
      }
    } else {
      if (TypeCastRules.getLeastRestrictiveType(types) != null) {
        return true;
      }
    }

    return false;
  }

  // Similar to RelOptUtil.areRowTypesEqual() with the additional check for allowSubstring
  private static boolean checkRowTypesCompatibility(
      RelDataType rowType1,
      RelDataType rowType2,
      boolean compareNames,
      boolean allowSubstring,
      boolean insertOp) {
    if (rowType1 == rowType2) {
      return true;
    }
    if (compareNames) {
      // if types are not identity-equal, then either the names or
      // the types must be different
      return false;
    }
    if (rowType2.getFieldCount() != rowType1.getFieldCount()) {
      return false;
    }
    final List<RelDataTypeField> f1 = rowType1.getFieldList();
    final List<RelDataTypeField> f2 = rowType2.getFieldList();
    for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
      final RelDataType type1 = pair.left.getType();
      final RelDataType type2 = pair.right.getType();
      if (!checkFieldTypesCompatibility(type1, type2, allowSubstring, insertOp)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Verifies that two row type names match.
   * Does not compare nullability.
   * Differs from RelOptUtil implementation by not defining types as equal if one is of type ANY.
   *
   * @param rowType1           row type for comparison
   * @param rowType2           row type for comparison
   * @param compareNames       boolean for name match
   * @param compareNullability boolean for nullability match
   *
   * @return boolean indicating that row types are equivalent
   */
  public static boolean areRowTypesEqual(
    RelDataType rowType1,
    RelDataType rowType2,
    boolean compareNames,
    boolean compareNullability) {
    if (rowType1 == rowType2) {
      return true;
    }

    if (rowType2.getFieldCount() != rowType1.getFieldCount()) {
      return false;
    }

    final List<RelDataTypeField> f1 = rowType1.getFieldList();
    final List<RelDataTypeField> f2 = rowType2.getFieldList();
    for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
      final RelDataType type1 = pair.left.getType();
      final RelDataType type2 = pair.right.getType();

      if (!areDataTypesEqual(type1, type2, !compareNullability)) {
        return false;
      }

      if (compareNames) {
        if (!pair.left.getName().equalsIgnoreCase(pair.right.getName())) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Verifies that two data types match without requiring any cast
   *
   * @param dataType1 data type for comparison
   * @param dataType2 data type for comparison
   * @return boolean indicating that data types are equivalent
   */
  public static boolean areDataTypesEqualWithoutRequiringCast(
    RelDataType dataType1,
    RelDataType dataType2) {
    return
      // The types are equal
      areDataTypesEqual(dataType1, dataType2, true) ||
      // They fall in the same type bucket without requiring a cast
      (getTypeBucket(dataType1.getSqlTypeName()) == getTypeBucket(dataType2.getSqlTypeName()));
  }

  public static int getTypeBucket(SqlTypeName typeName) {
    // Groups of types which don't require a cast if compared.
    switch (typeName) {
      case BOOLEAN:
        return 1;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return 2;
      case DECIMAL:
        return 3;
      case FLOAT:
        return 4;
      case REAL:
        return 5;
      case DOUBLE:
        return 6;
      case DATE:
        return 7;
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
        return 8;
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return 9;
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        return 10;
      case CHAR:
      case VARCHAR:
        return 11;
      case BINARY:
      case VARBINARY:
        return 12;
      case NULL:
        return 13;
      case ANY:
        return 14;
      default:
        return -1;
    }
  }

  /**
   * Verifies that two data types match.
   * @param dataType1          data type for comparison
   * @param dataType2          data type for comparison
   * @param sqlTypeNameOnly    boolean for only SqlTypeName match
   *
   * @return boolean indicating that data types are equivalent
   */
  public static boolean areDataTypesEqual(
    RelDataType dataType1,
    RelDataType dataType2,
    boolean sqlTypeNameOnly) {
    if (sqlTypeNameOnly) {
      return dataType1.getSqlTypeName().equals(dataType2.getSqlTypeName());
    }
    return dataType1.equals(dataType2);
  }

  /**
   * Creates a projection which casts a rel's output to a desired row type.
   * Differs from RelOptUtil implementation by casting even when type is ANY.
   *
   * @param rel         producer of rows to be converted
   * @param castRowType row type after cast
   *
   * @return conversion rel
   */
  public static RelNode createCastRel(
    final RelNode rel,
    RelDataType castRowType) {
    return createCastRel(
      rel, castRowType, RelFactories.DEFAULT_PROJECT_FACTORY);
  }

  /**
   * Creates a projection which casts a rel's output to a desired row type.
   * Differs from RelOptUtil implementation by casting even when type is ANY.
   *
   * @param rel         producer of rows to be converted
   * @param castRowType row type after cast
   * @param projectFactory Project Factory
   *
   * @return conversion rel
   */
  public static RelNode createCastRel(
    final RelNode rel,
    RelDataType castRowType,
    RelFactories.ProjectFactory projectFactory) {
    assert projectFactory != null;
    RelDataType rowType = rel.getRowType();
    if (areRowTypesEqual(rowType, castRowType, false, true)) {
      // nothing to do
      return rel;
    }
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    final List<RexNode> castExps =
      RexUtil.generateCastExpressions(rexBuilder, castRowType, rowType);

    final List<RexNode> castExpsWithNullReplacement = new ArrayList<>();
    for (RexNode expr : castExps) {
      if (expr.getKind() == SqlKind.CAST && expr.getType().getSqlTypeName() == SqlTypeName.NULL) {
        // Cast to NULL type is not supported. Insert NULL type literal instead
        castExpsWithNullReplacement.add(rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL)));
      } else {
        castExpsWithNullReplacement.add(expr);
      }
    }

    return projectFactory.createProject(rel, ImmutableList.of(), castExpsWithNullReplacement, rowType.getFieldNames());
  }

  /**
   * Returns a relational expression which has the same fields as the
   * underlying expression, but the fields have different names.
   *
   *
   * @param rel        Relational expression
   * @param fieldNames Field names
   * @return Renamed relational expression
   */
  public static RelNode createRename(
      RelNode rel,
      final List<String> fieldNames) {
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    assert fieldNames.size() == fields.size();
    final List<RexNode> refs =
        new AbstractList<RexNode>() {
          @Override
          public int size() {
            return fields.size();
          }

          @Override
          public RexNode get(int index) {
            return RexInputRef.of(index, fields);
          }
        };

    return RelOptUtil.createProject(rel, refs, fieldNames, false);
  }

  public static boolean isSimpleColumnSelection(Project project) {
    Set<Integer> inputRefReferenced = new HashSet<>();
    for (Pair<RexNode, String> proj : project.getNamedProjects()) {
      if (proj.getKey().getKind() != SqlKind.INPUT_REF) {
        return false;
      }
      RexInputRef inputRef = (RexInputRef) proj.getKey();
      // If the input reference is again referenced, then it is not a simple column selection (since it is not a permutation).
      if (inputRefReferenced.contains(inputRef.getIndex())) {
        return false;
      }
      final String nameOfProjectField = proj.getValue();
      final String nameOfInput = project.getInput().getRowType().getFieldNames().get(inputRef.getIndex());
      // Renaming a column is not a simple column selection
      if (nameOfProjectField == null || !nameOfProjectField.equals(nameOfInput)) {
        return false;
      }
      inputRefReferenced.add(inputRef.getIndex());
    }
    return true;
  }

  public static boolean isTrivialProject(Project project) {
    return containIdentity(project.getProjects(),
      project.getRowType(),
      project.getInput().getRowType(),
      String::compareTo);
  }

  public static boolean isTrivialProjectIgnoreNameCasing(Project project) {
    return containIdentity(project.getProjects(),
        project.getRowType(),
        project.getInput().getRowType(),
        String::compareToIgnoreCase);
  }

  /** Returns a rowType having all unique field name.
   *
   * @param rowType : input rowType
   * @param typeFactory : type factory used to create a new row type.
   * @return
   */
  public static RelDataType uniqifyFieldName(final RelDataType rowType, final RelDataTypeFactory typeFactory) {
    return typeFactory.createStructType(RelOptUtil.getFieldTypeList(rowType),
        SqlValidatorUtil.uniquify(rowType.getFieldNames()));
  }

  /**
   * Returns whether the leading edge of a given array of expressions is
   * wholly {@link RexInputRef} objects with types and names corresponding
   * to the underlying row type. */
  public static boolean containIdentity(List<? extends RexNode> exps,
      RelDataType rowType,
      RelDataType childRowType,
      Comparator<String> nameComparator) {
    List<RelDataTypeField> fields = rowType.getFieldList();
    List<RelDataTypeField> childFields = childRowType.getFieldList();
    int fieldCount = childFields.size();
    if (exps.size() != fieldCount) {
      return false;
    }
    for (int i = 0; i < exps.size(); i++) {
      RexNode exp = exps.get(i);
      if (!(exp instanceof RexInputRef)) {
        return false;
      }
      RexInputRef var = (RexInputRef) exp;
      if (var.getIndex() != i) {
        return false;
      }
      if (0 != nameComparator.compare(fields.get(i).getName(), childFields.get(i).getName())) {
        return false;
      }
      if (!fields.get(i).getType().equals(childFields.get(i).getType())) {
        return false;
      }
    }
    return true;
  }

  public static Pair<Integer, RelDataTypeField> findFieldWithIndex(List<RelDataTypeField> fields, String fieldName) {
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).getName().equals(fieldName)) {
        return Pair.of(i, fields.get(i));
      }
    }
    return null;
  }

  public static class VertexRemover extends StatelessRelShuttleImpl {

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof HepRelVertex) {
        return super.visit(((HepRelVertex) other).getCurrentRel());
      } else {
        return super.visit(other);
      }
    }
  }

  public static class SubsetRemover extends StatelessRelShuttleImpl {

    private final boolean needBest;

    public SubsetRemover() {
      this(true);
    }

    public SubsetRemover(boolean needBest) {
      this.needBest = needBest;
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof RelSubset) {
        if (((RelSubset) other).getBest() != null) {
          return ((RelSubset) other).getBest().accept(this);
        }
        if (!needBest && ((RelSubset) other).getRelList().size() == 1) {
          return ((RelSubset) other).getRelList().get(0).accept(this);
        }
        throw UserException.unsupportedError().message("SubsetRemover: found null best, parent " + other).build(logger);
      } else {
        return super.visit(other);
      }
    }
  }

  /**
   * Helper class to rewrite the Calcite plan to remove Sort nodes
   * that would be in sub-queries and do not have a fetch or offset.
   *
   * A sort has no effect inside a sub-query on its own.
   */
  public static class OrderByInSubQueryRemover extends StatelessRelShuttleImpl {

    /**
     * The sort at the root of the plan (or underneath a project
     * at the root of the plan). An order by without fetch or limit
     * is valid here, so never remove this node, if it exists.
     */
    private final Sort topLevelSort;

    /**
     * Constructor.
     *
     * @param root The root of the plan.
     */
    public OrderByInSubQueryRemover(RelNode root)
    {
      // Identify if there is either a sort at the plan root, or
      // a Project with a sort at the plan root. These Sorts should not
      // be skipped since ORDER BY is always legal there.
      if (root instanceof Sort) {
        this.topLevelSort = (Sort) root;
      } else if (root instanceof Project &&
        root.getInput(0) instanceof Sort) {
        this.topLevelSort = (Sort) root.getInput(0);
      } else {
        this.topLevelSort = null;
      }
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {

      // Ignore the root node.
      if (null == parent) {
        return super.visitChild(parent, i, child);
      }

      // Ignore non-sort child nodes.
      if (!(child instanceof Sort)) {
        return super.visitChild(parent, i, child);
      }

      // Ignore the sort for the top level SELECT. It's valid to use ORDER BY
      // without FETCH / OFFSET here.
      if (child == topLevelSort) {
        return super.visitChild(parent, i, child);
      }

      // If the child Sort has FETCH and LIMIT clauses, do not touch them.
      Sort childAsSort = (Sort) child;
      if (childAsSort.offset == null &&
          childAsSort.fetch == null) {
        parent.replaceInput(i, childAsSort.getInput());
        return super.visitChild(parent, i, childAsSort.getInput());
      }

      return super.visitChild(parent, i, child);
    }
  }

  public static class FlattenRexVisitor extends RexVisitorImpl<Boolean> {

    public FlattenRexVisitor() {
      super(true);
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
      return false;
    }

    @Override
    public Boolean visitLocalRef(RexLocalRef localRef) {
      return false;
    }

    @Override
    public Boolean visitLiteral(RexLiteral literal) {
      return false;
    }

    @Override
    public Boolean visitOver(RexOver over) {
      return false;
    }

    @Override
    public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      return false;
    }

    @Override
    public Boolean visitCall(RexCall call) {
      if (call.getOperator().getName().equalsIgnoreCase("flatten")) {
        return true;
      }

      for (RexNode op : call.getOperands()) {
        Boolean opResult = op.accept(this);
        if (opResult != null && opResult.booleanValue()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
      return false;
    }

    @Override
    public Boolean visitRangeRef(RexRangeRef rangeRef) {
      return false;
    }

    @Override
    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess.getReferenceExpr().accept(this);
    }

    @Override
    public Boolean visitSubQuery(RexSubQuery subQuery) {
      return false;
    }
  }

  public static final class ContainsRexVisitor extends RexVisitorImpl<Boolean> {

    private final boolean checkOrigin;
    private final RelMetadataQuery mq;
    private final RelNode node;
    private final int index;

    public static boolean hasContains(RexNode rexNode) {
      return rexNode.accept(new ContainsRexVisitor(false, null, null, -1));
    }

    public static boolean hasContainsCheckOrigin(RelNode node, RexNode rex, int index) {
      return rex.accept(new ContainsRexVisitor(true, node.getCluster().getMetadataQuery(), node, index));
    }

    private ContainsRexVisitor(boolean checkOrigin, RelMetadataQuery mq, RelNode node, int index) {
      super(true);
      this.checkOrigin = checkOrigin;
      this.mq = mq;
      this.node = node;
      this.index = index;
      Preconditions.checkArgument(!checkOrigin || (mq != null && node != null));
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
      return false;
    }

    @Override
    public Boolean visitLocalRef(RexLocalRef localRef) {
      return false;
    }

    @Override
    public Boolean visitLiteral(RexLiteral literal) {
      return false;
    }

    @Override
    public Boolean visitOver(RexOver over) {
      return false;
    }

    @Override
    public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      return false;
    }

    @Override
    public Boolean visitCall(RexCall call) {
      if (call.getOperator().getName().equalsIgnoreCase("contains")) {
        if (!checkOrigin) {
          return true;
        }
        // Check origin
        final Set<RelColumnOrigin> origins;
        if (index >= 0) {
          origins = mq.getColumnOrigins(node, index);
        } else {
          List<RelDataTypeField> fields = ImmutableList.<RelDataTypeField>of(new RelDataTypeFieldImpl("ContainsTemp", 0, call.getType()));
          Project temporary =
              new LogicalProject(node.getCluster(), node.getTraitSet().plus(Convention.NONE), node.getInput(0), ImmutableList.of(call), new RelRecordType(fields));
          origins = mq.getColumnOrigins(temporary, 0);
        }

        boolean supportContains = true;
        if (origins == null) {
          supportContains = false;
        } else {
          for (RelColumnOrigin column : origins) {
            if (column.getOriginTable() == null) {
              supportContains = false;
            } else {
              NamespaceTable namespaceTable2 = column.getOriginTable().unwrap(NamespaceTable.class);
              if (namespaceTable2 != null) {
                if(!namespaceTable2.getStoragePluginId().getCapabilities().getCapability(SourceCapabilities.SUPPORTS_CONTAINS)){
                  supportContains = false;
                }
              } else {
                supportContains = false;
              }
            }
          }
        }
        if (!supportContains) {
          throw UserException.unsupportedError().message("Contains operator is not supported for the table, %s", call).build(logger);
        }
        return true;
      }

      for (RexNode op : call.getOperands()) {
        Boolean opResult = op.accept(this);
        if (opResult != null && opResult.booleanValue()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
      return false;
    }

    @Override
    public Boolean visitRangeRef(RexRangeRef rangeRef) {
      return false;
    }

    @Override
    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess.getReferenceExpr().accept(this);
    }

    @Override
    public Boolean visitSubQuery(RexSubQuery subQuery) {
      return false;
    }
  }

  public static class NodeRemover extends  RelShuttleImpl{

    private final Predicate<RelNode> predicate;

    public NodeRemover(Predicate<RelNode> predicate) {
      super();
      this.predicate = predicate;
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      if(!predicate.test(parent)){
        return child.accept(this);
      }

      return super.visitChild(parent, i, child);
    }
  }

  /**
   * Find the path to the node that matches the predicate
   * @param root
   * @param predicate
   * @return
   */
  public static List<Integer> findPathToNode(final RelNode root, final Predicate<RelNode> predicate) {
    final Deque<Integer> localStack = new ArrayDeque<>();
    final List<Integer> result = new ArrayList<>();
    final Pointer<Boolean> found = new Pointer<>(false);
    root.accept(new RoutingShuttle() {
      @Override
      public RelNode visitChildren(RelNode rel) {
        for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
          localStack.addLast(input.i);
          rel = visitChild(rel, input.i, input.e);
          localStack.removeLast();
          if (found.value) {
            return rel;
          }
        }
        return rel;
      }
      @Override
      public RelNode visit(RelNode other) {
        if (found.value) {
          return other;
        }
        if (predicate.test(other)) {
          result.addAll(localStack);
          found.value = true;
          return other;
        }
        return super.visit(other);
      }
    });

    Preconditions.checkState(found.value);

    return result;
  }

  /**
   * Find the node at a given path.  The output from {@link this.findPathToNode} when used as input will return the
   * found node
   * @param root
   * @param path
   * @return
   */
  public static RelNode findNodeAtPath(final RelNode root, final Iterable<Integer> path) {
    final Iterator<Integer> iter = path.iterator();

    RelNode node = root.accept(new RoutingShuttle() {
      @Override
      public RelNode visit(RelNode other) {
        return super.visit(other);
      }

      @Override
      protected RelNode visitChildren(RelNode rel) {
        if (!iter.hasNext()) {
          return rel;
        }

        int child = iter.next();

        return rel.getInput(child).accept(this);
      }
    });

    Preconditions.checkNotNull(node);
    return node;
  }

  /**
   * Removes constant group keys in aggregate and adds a project on top.
   * Returns the original rel if there is a failure handling the rel.
   */
  public static RelNode removeConstantGroupKeys(RelNode rel, RelBuilderFactory factory) {
    return rel.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(RelNode other) {
        if (other instanceof Aggregate) {
          Aggregate agg = (Aggregate) super.visitChildren(other);
          RelNode newRel = handleAggregate(agg, factory.create(agg.getCluster(), null));
          if (newRel == null) {
            return agg;
          }
          return newRel;
        }
        return super.visit(other);
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        Aggregate agg = (Aggregate) super.visitChildren(aggregate);
        RelNode newRel = handleAggregate(agg, factory.create(agg.getCluster(), null));
        if (newRel == null) {
          return agg;
        }
        return newRel;
      }
    });
  }

  private static RelNode handleAggregate(Aggregate agg, RelBuilder relBuilder) {
    final Set<Integer> newGroupKeySet = new HashSet<>();
    relBuilder.push(agg.getInput());

    // 1. Create an array with the number of group by keys in Aggregate
    // 2. Partially fill out the array with constant fields at the original position in the rowType
    // 3. Add a project below aggregate by reordering the fields in a way that non-constant fields come first
    // 3. Build an aggregate for the remaining fields
    // 4. Fill out the rest of the array by getting fields from constructed aggregate
    // 5. Create a Project with removed constants along with original fields from input
    int numGroupKeys = agg.getGroupSet().cardinality();
    if (numGroupKeys == 0) {
      return null;
    }
    final RexNode[] groupProjects = new RexNode[numGroupKeys];
    final Set<Integer> constantInd = new LinkedHashSet<>();
    final Set<Integer> groupInd = new LinkedHashSet<>();
    final Map<Integer, Integer> mapping = new HashMap<>();
    for (int i = 0 ; i < agg.getGroupSet().cardinality() ; i++) {
      RexLiteral literal = projectedLiteral(agg.getInput(), i);
      if(literal != null) {
        groupProjects[i] = literal;
        constantInd.add(i);
      } else {
        groupProjects[i] = null;
        newGroupKeySet.add(groupInd.size());
        groupInd.add(i);
      }
    }

    final List<RexNode> projectBelowAggregate = new ArrayList<>();
    if (constantInd.size() <= 1 || constantInd.size() == numGroupKeys) {
      return null;
    }

    for(int ind : groupInd) {
      mapping.put(ind, projectBelowAggregate.size());
      projectBelowAggregate.add(relBuilder.field(ind));
    }

    for(int ind : constantInd) {
      mapping.put(ind, projectBelowAggregate.size());
      projectBelowAggregate.add(relBuilder.field(ind));
    }

    for(int i = 0 ; i < relBuilder.fields().size() ; i++) {
      if(!constantInd.contains(i) && !groupInd.contains(i)) {
        mapping.put(i, projectBelowAggregate.size());
        projectBelowAggregate.add(relBuilder.field(i));
      }
    }

    final List<AggregateCall> aggregateCalls = new ArrayList<>();
    for (final AggregateCall aggregateCall : agg.getAggCallList()) {
      List<Integer> newArgList = new ArrayList<>();
      for (final int argIndex : aggregateCall.getArgList()) {
        newArgList.add(mapping.get(argIndex));
      }
      aggregateCalls.add(aggregateCall.copy(newArgList, aggregateCall.filterArg));
    }

    ImmutableBitSet newGroupSet = ImmutableBitSet.of(newGroupKeySet);
    relBuilder.project(projectBelowAggregate).aggregate(relBuilder.groupKey(newGroupSet.toArray()), aggregateCalls);

    int count = 0;
    for(int i = 0; i < groupProjects.length; i++) {
      if (groupProjects[i] == null) {
        groupProjects[i] = relBuilder.field(count);
        count++;
      }
    }

    List<RexNode> projects = new ArrayList<>(Arrays.asList(groupProjects));
    for(int i = 0 ; i < agg.getAggCallList().size() ; i++) {
      projects.add(relBuilder.field(i+newGroupKeySet.size()));
    }
    RelNode newRel = relBuilder.project(projects, agg.getRowType().getFieldNames()).build();
    if (!RelOptUtil.areRowTypesEqual(newRel.getRowType(), agg.getRowType(), true)) {
      return null;
    }
    return newRel;
  }
  private static RexLiteral projectedLiteral(RelNode rel, int i) {
    if (rel instanceof Project) {
      Project project = (Project)rel;
      RexNode node = (RexNode)project.getProjects().get(i);
      if (node instanceof RexLiteral) {
        return (RexLiteral)node;
      }
    }

    return null;
  }

  public static List<RexNode> identityProjects(RelDataType type) {
    List<RexNode> projects = new ArrayList<>();
    List<RelDataTypeField> fieldList = type.getFieldList();
    for (int i = 0; i < type.getFieldCount(); i++) {
      RelDataTypeField field = fieldList.get(i);
      projects.add(new RexInputRef(i, field.getType()));
    }
    return projects;
  }

  public static List<RexNode> identityProjects(RelDataType type, ImmutableBitSet selectedColumns) {
    List<RexNode> projects = new ArrayList<>();
    List<RelDataTypeField> fieldList = type.getFieldList();
    for (int i = 0; i < type.getFieldCount(); i++) {
      if(selectedColumns.get(i)) {
        RelDataTypeField field = fieldList.get(i);
        projects.add(new RexInputRef(i, field.getType()));
      }
    }
    return projects;
  }

  public static boolean isNegative(RexLiteral literal) {
    Double d = literal.getValueAs(Double.class);
    return d < 0;
  }

  public static SqlOperator op(SqlKind kind) {
    switch (kind) {
    case IS_FALSE:
      return SqlStdOperatorTable.IS_FALSE;
    case IS_TRUE:
      return SqlStdOperatorTable.IS_TRUE;
    case IS_UNKNOWN:
      return SqlStdOperatorTable.IS_UNKNOWN;
    case IS_NULL:
      return SqlStdOperatorTable.IS_NULL;
    case IS_NOT_FALSE:
      return SqlStdOperatorTable.IS_NOT_FALSE;
    case IS_NOT_TRUE:
      return SqlStdOperatorTable.IS_NOT_TRUE;
    case IS_NOT_NULL:
      return SqlStdOperatorTable.IS_NOT_NULL;
    case EQUALS:
      return SqlStdOperatorTable.EQUALS;
    case NOT_EQUALS:
      return SqlStdOperatorTable.NOT_EQUALS;
    case LESS_THAN:
      return SqlStdOperatorTable.LESS_THAN;
    case GREATER_THAN:
      return SqlStdOperatorTable.GREATER_THAN;
    case LESS_THAN_OR_EQUAL:
      return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    case GREATER_THAN_OR_EQUAL:
      return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    default:
      throw new AssertionError(kind);
    }
  }

  /**
   * RexVisitor which returns an ImmutableBitSet of the columns referenced by a list of RexNodes
   */
  public static class InputRefFinder extends RexVisitorImpl<Void> {
    private final ImmutableBitSet.Builder setBuilder = ImmutableBitSet.builder();

    public InputRefFinder() {
      super(true);
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
      setBuilder.set(inputRef.getIndex());
      return null;
    }

    public ImmutableBitSet getInputRefs(List<RexNode> rexNodes) {
      for (RexNode rexNode : rexNodes) {
        rexNode.accept(this);
      }
      return setBuilder.build();
    }
  }

  /**
   * RexShuttle which canonicalizes numeric literals in the RexNode to its expanded form.
   */
  public static class RexLiteralCanonicalizer extends RexShuttle {
    private final RexBuilder rexBuilder;

    public RexLiteralCanonicalizer(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      RexNode newLiteral = literal(literal.getValue(), literal.getType());
      if (newLiteral != null) {
        return newLiteral;
      }
      // Should we try with value2/value3?
      return literal;
    }

    /**
     * Creates a literal (constant expression).
     */
    private RexNode literal(Object value, RelDataType type) {
      if (value instanceof BigDecimal) {
        return rexBuilder.makeExactLiteral(stripTrailingZeros((BigDecimal) value), type);
      } else if (value instanceof Float || value instanceof Double) {
        return rexBuilder.makeApproxLiteral(
          stripTrailingZeros(BigDecimal.valueOf(((Number) value).doubleValue())), type);
      } else if (value instanceof Number) {
        return rexBuilder.makeExactLiteral(
          stripTrailingZeros(BigDecimal.valueOf(((Number) value).longValue())), type);
      } else {
        return null;
      }
    }

    private BigDecimal stripTrailingZeros(BigDecimal value) {
      if (value.scale() <= 0) {
        // Its already an integer value. No need to strip any zeroes.
        return value;
      }
      try {
        BigDecimal newValue = value.stripTrailingZeros();
        int scale = newValue.scale();
        if (scale >= 0 && scale <= rexBuilder.getTypeFactory().getTypeSystem().getMaxNumericScale()) {
          return newValue;
        } else if (scale < 0) {
          // In case we have something like 600.00, where the scale is greater than 0,
          // stripping trailing zeros will end up in scientific notation 6E+2. Set
          // the scale to 0 to get the expanded notation.
          return newValue.setScale(0, BigDecimal.ROUND_UNNECESSARY);
        }
      } catch (Exception ex) {
        // In case any exception happens, log and continue without stripping.
        logger.info(String.format("Caught exception while stripping trailing zeroes from %s", value.toPlainString()), ex);
      }
      return value;
    }
  }

  /**
   * Rewrites structured condition
   * For example,
   * ROW($1,$2) <> ROW(1,2)
   * =>
   * $1 <> 1 and $2 <> 2
   */
  public static final class StructuredConditionRewriter extends StatelessRelShuttleImpl {
    private StructuredConditionRewriter() {
    }

    @Override
    public RelNode visit(LogicalProject project) {
      final ConditionFlattenter flattener = new ConditionFlattenter(project.getCluster().getRexBuilder());
      return super.visit(project)
        .accept(flattener);
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
      final ConditionFlattenter flattener = new ConditionFlattenter(filter.getCluster().getRexBuilder());
      return super.visit(filter)
        .accept(flattener);
    }

    public static RelNode rewrite(RelNode rel) {
      return rel.accept(new StructuredConditionRewriter());
    }

    /**
     *
     */
    private static final class ConditionFlattenter extends RexShuttle {
      private RexBuilder builder;

      private ConditionFlattenter(RexBuilder builder) {
        this.builder = builder;
      }

      @Override
      public RexNode visitCall(RexCall rexCall) {
        if (rexCall.isA(SqlKind.COMPARISON)) {
          if(rexCall.getOperands().get(0).getType().isStruct()) {
            RexNode converted = flattenComparison(builder, rexCall.getOperator(), rexCall.getOperands());
            if (converted != null) {
              return converted;
            }
          }
        }
        return super.visitCall(rexCall);
      }
    }

    private static RexNode flattenComparison(RexBuilder rexBuilder, SqlOperator op, List<RexNode> exprs) {
      final List<Pair<RexNode, String>> flattenedExps = Lists.newArrayList();
      for(RexNode expr : exprs) {
        if (expr instanceof RexCall && expr.isA(SqlKind.ROW)) {
          RexCall call = (RexCall) expr;
          List<RexNode> operands = call.getOperands();
          for (int i = 0 ; i < operands.size() ; i++) {
            flattenedExps.add(new Pair<>(operands.get(i), call.getType().getFieldList().get(i).getName()));
          }
        } else if (expr instanceof RexCall && expr.isA(SqlKind.CAST)) {
          RexCall call = (RexCall) expr;
          if (!call.getOperands().get(0).isA(SqlKind.ROW)) {
            return null;
          }
          List<RexNode> fields =  ((RexCall) call.getOperands().get(0)).getOperands();
          List<RelDataTypeField> types = call.type.getFieldList();
          for (int i = 0 ; i < fields.size() ; i++) {
            RexNode cast = rexBuilder.makeCast(types.get(i).getType(), fields.get(i));
            flattenedExps.add(new Pair<>(cast, types.get(i).getName()));
          }
        } else {
          return null;
        }
      }

      int n = flattenedExps.size() / 2;
      boolean negate = false;
      if (op.getKind() == SqlKind.NOT_EQUALS) {
        negate = true;
        op = SqlStdOperatorTable.EQUALS;
      }

      if (n > 1 && op.getKind() != SqlKind.EQUALS) {
        throw Util.needToImplement("inequality comparison for row types");
      } else {
        RexNode conjunction = null;

        for(int i = 0; i < n; ++i) {
          RexNode comparison = rexBuilder.makeCall(op, (RexNode) flattenedExps.get(i).left, (RexNode) flattenedExps.get(i + n).left);
          if (conjunction == null) {
            conjunction = comparison;
          } else {
            conjunction = rexBuilder.makeCall(SqlStdOperatorTable.AND, conjunction, comparison);
          }
        }

        if (negate) {
          return rexBuilder.makeCall(SqlStdOperatorTable.NOT, conjunction);
        } else {
          return conjunction;
        }
      }
    }
  }

  public static class RexNodeCountVisitor extends RexShuttle {
    private final Pointer<Integer> totalCount = new Pointer<>(0);
    private final Pointer<Integer> inputRefCount = new Pointer<>(0);

    public static int count(RexNode node) {
      RexNodeCountVisitor v = new RexNodeCountVisitor();
      node.accept(v);
      return v.totalCount.value;
    }

    public static int inputRefCount(RexNode node) {
      RexNodeCountVisitor v = new RexNodeCountVisitor();
      node.accept(v);
      return v.inputRefCount.value;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      totalCount.value++;
      return super.visitCall(call);
    }

    @Override
    public RexNode visitInputRef(RexInputRef input) {
      totalCount.value++;
      inputRefCount.value++;
      return super.visitInputRef(input);
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      totalCount.value++;
      return super.visitLiteral(literal);
    }
  }

  /**
   * Same as {@link RelOptUtil#shiftFilter(int, int, int, RexBuilder, List, int, List, RexNode)}.
   * Shift filters using inputs ref of joins to filters using input refs of children of joins
   */
  public static RexNode shiftFilter(
    int start,
    int end,
    int offset,
    RexBuilder rexBuilder,
    List<RelDataTypeField> joinFields,
    int nTotalFields,
    List<RelDataTypeField> rightFields,
    RexNode filter) {
    int[] adjustments = new int[nTotalFields];
    for (int i = start; i < end; i++) {
      adjustments[i] = offset;
    }
    return filter.accept(
      new RelOptUtil.RexInputConverter(
        rexBuilder,
        joinFields,
        rightFields,
        adjustments));
  }

  public static PrelUtil.InputRewriter getInputRewriterFromProjectedFields(List<SchemaPath> projection, RelDataType rowType, BatchSchema batchSchema, RelOptCluster cluster) {
    final List<String> newFields = getRowTypeFromProjectedColumns(projection, batchSchema, cluster).getFieldNames();
    final Map<String, Integer> fieldMap = new HashMap<>();
    final IntIntHashMap oldToNewFields = new IntIntHashMap();
    final List<String> oldFields = rowType.getFieldNames();
    IntStream.range(0, newFields.size()).forEach(i -> fieldMap.put(newFields.get(i), i));
    for (int i = 0; i < oldFields.size(); i++) {
      final String field = oldFields.get(i);
      if (fieldMap.containsKey(field)) {
        oldToNewFields.put(i, fieldMap.get(field));
      }
    }
    return new PrelUtil.InputRewriter(oldToNewFields);
  }

  /**
   * Combination of {@link RelOptUtil#conjunctions(RexNode)} and {@link RelOptUtil#disjunctions(RexNode)}.
   * Check {@code RexNode.getKind()} and for AND/OR return unnested elements.
   */
  public static List<RexNode> conDisjunctions(RexNode rexNode) {
    switch (rexNode.getKind()) {
      case AND:
        return RelOptUtil.conjunctions(rexNode);
      case OR:
        return RelOptUtil.disjunctions(rexNode);
      default:
        throw new UnsupportedOperationException();
    }
  }

  /**
   * Combination of {@link RexUtil#composeConjunction(RexBuilder, Iterable, boolean)} and
   * {@link RexUtil#composeDisjunction(RexBuilder, Iterable, boolean)}.
   * Check {@code RexNode.getKind()} and for AND/OR compose expressions using {@code nodes}.
   */
  public static RexNode composeConDisjunction(RexBuilder rexBuilder,
    Iterable<? extends RexNode> nodes, boolean nullOnEmpty, SqlKind sqlKind) {
    switch (sqlKind) {
      case AND:
        return RexUtil.composeConjunction(rexBuilder, nodes, nullOnEmpty);
      case OR:
        return RexUtil.composeDisjunction(rexBuilder, nodes, nullOnEmpty);
      default:
        throw new UnsupportedOperationException();
    }
  }

  /**
   * Returns a list of the child expressions of given relational expression.
   * @param rel Relational expression
   * @return List of the child expressions of given relational expression
   */
  public static List<RexNode> getChildExps(RelNode rel) {
    List<RexNode> rexNodes = ImmutableList.of();
    if (rel instanceof Filter) {
      rexNodes = ImmutableList.of(((Filter)rel).getCondition());
    } else if (rel instanceof Join) {
      rexNodes = ImmutableList.of(((Join)rel).getCondition());
    } else if (rel instanceof MultiJoin) {
      rexNodes = ImmutableList.of(((MultiJoin)rel).getJoinFilter());
    } else if (rel instanceof Project) {
      rexNodes = ((Project)rel).getProjects();
    } else if (rel instanceof Sort) {
      rexNodes = ((Sort)rel).getSortExps();
    } else if (rel instanceof TableFunctionScan) {
      rexNodes = ImmutableList.of(((TableFunctionScan)rel).getCall());
    } else if (rel instanceof Snapshot) {
      rexNodes = ImmutableList.of(((Snapshot)rel).getPeriod());
    }
    return rexNodes;
  }

  /* If the call was to datetime - interval. */
  public static boolean isDatetimeMinusInterval(RexCall call) {
    return ((call.getOperator() == SqlStdOperatorTable.MINUS_DATE)
            && (call.getOperands().size() == 2)
            && (INTERVAL_TYPES.contains(call.getOperands().get(1).getType().getSqlTypeName())));
  }

  /* If the call was to datetime +/- interval. */
  public static boolean isDatetimeIntervalArithmetic(RexCall call) {
    return ((call.getOperator() == SqlStdOperatorTable.DATETIME_PLUS)
            || isDatetimeMinusInterval(call));
  }

  /**
   * TODO we might be able to replace this deepEquals/deepHashcode in CALCITE-4129
   * @param relNode
   * @return
   */
  public static long longHashCode(RelNode relNode) {
    Hasher hasher = Hashing.sha256().newHasher();
    relNode.explain(new RelWriter() {
      @Override
      public void explain(RelNode rel, List<Pair<String, Object>> valueList) {
        for(Pair<String, Object> pair: valueList) {
          item(pair.left, pair.right);
        }
        done(relNode);
      }

      @Override
      public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.DIGEST_ATTRIBUTES;
      }

      @Override
      public RelWriter input(String term, RelNode input) {
        hasher.putString(term, StandardCharsets.UTF_8);
        input.explain(this);
        return this;
      }

      @Override
      public RelWriter item(String term, Object value) {
        if(value instanceof RelNode) {
          input(term, (RelNode) value);
        } else {
          hasher.putString(term, StandardCharsets.UTF_8)
            .putString(value.toString(), StandardCharsets.UTF_8);
        }
        return this;
      }

      @Override
      public RelWriter itemIf(String term, Object value, boolean condition) {
        if(condition) {
          return item(term, value);
        } else {
          return this;
        }
      }

      @Override
      public RelWriter done(RelNode node) {
        hasher.putString(node.getClass().toString(), StandardCharsets.UTF_8);
        return this;
      }

      @Override
      public boolean nest() {
        return true;
      }
    });
    return hasher.hash().asLong();
  }

  public static class TransformCollectingCall extends RelOptRuleCall {
    private final List<RelNode> outcome = new ArrayList<>();

    public TransformCollectingCall(RelOptPlanner planner, RelOptRuleOperand operand, RelNode[] rels,
                                   Map<RelNode, List<RelNode>> nodeInputs) {
      super(planner, operand, rels, nodeInputs);
    }

    @Override
    public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv, RelHintsPropagator handler) {
      outcome.add(rel);
    }

    public boolean isTransformed() {
      return outcome.size() != 0;
    }

    public RelNode getTransformedRel() {
      if (isTransformed()) {
        return outcome.get(0);
      } else {
        throw new RuntimeException("No transformed node found.");
      }
    }
  }

  /** Builds an equi-join condition from a list of (leftKey, rightKey, filterNull). */
  public static RexNode createEquiJoinCondition(
    final RelNode left,
    final RelNode right,
    final List<Triple<Integer, Integer, Boolean>> equiConditions,
    final RexBuilder rexBuilder) {
    final List<RelDataType> leftTypes =
      RelOptUtil.getFieldTypeList(left.getRowType());
    final List<RelDataType> rightTypes =
      RelOptUtil.getFieldTypeList(right.getRowType());
    final List<RexNode> conditions = new ArrayList<>();
    for (Triple<Integer, Integer, Boolean> key : equiConditions) {
      final int leftKey = key.getLeft();
      final int rightKey = key.getMiddle();
      final Boolean filterNull = key.getRight();
      final SqlOperator op = filterNull ? SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
      conditions.add(rexBuilder.makeCall(op,
        rexBuilder.makeInputRef(leftTypes.get(leftKey), leftKey),
        rexBuilder.makeInputRef(rightTypes.get(rightKey),
          leftTypes.size() + rightKey)));
    }
    return RexUtil.composeConjunction(rexBuilder, conditions, false);
  }

  public static boolean containsUnsupportedDistinctCall(Aggregate aggregate) {
    return aggregate.getAggCallList().stream().anyMatch(aggregateCall ->
      aggregateCall.isDistinct() && aggregateCall.getAggregation().getKind() != SqlKind.LISTAGG);
  }

  public static int getNextExprIndexFromFields(List<String> fieldNames) {
    int startIndex = -1;
    for (String name : fieldNames) {
      if (name.startsWith("EXPR$")) {
        String[] split = name.split("EXPR\\$");
        if (split.length == 2 && StringUtils.isNumeric(split[1])) {
          startIndex = max(Integer.parseInt(split[1]), startIndex);
        }
      }
    }
    return startIndex + 1;
  }

  public static class SimpleReflectionFinderVisitor extends StatelessRelShuttleImpl {
    private final List<String> primaryKey;
    private final List<Integer> indices;

    public SimpleReflectionFinderVisitor() {
      this.primaryKey = new ArrayList<>();
      this.indices = new ArrayList<>();
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      // Will get here if its an agg reflection. We only support PK for raw reflections.
      return aggregate;
    }

    @Override
    public RelNode visit(LogicalProject project) {
      return visit(project.getInput());
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
      return visit(filter.getInput());
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof ScanRelBase) {
        List<String> toAdd = ((ScanRelBase) other).getTableMetadata().getPrimaryKey();
        if (!CollectionUtils.isEmpty(toAdd)) {
          primaryKey.addAll(toAdd);
          indices.addAll(other.getRowType().getFieldList().stream()
            .filter(f -> toAdd.contains(f.getName().toLowerCase(Locale.ROOT)))
            .map(RelDataTypeField::getIndex)
            .collect(Collectors.toList()));
        }
        return other;
      } else if (other instanceof LogicalProject || other instanceof LogicalFilter) {
        return visit(((SingleRel) other).getInput());
      } else {
        return other;
      }
    }

    public List<String> getPrimaryKey() {
      return primaryKey;
    }

    public List<Integer> getIndices() {
      return indices;
    }
  }

  /**
   * Looks for a BROADCAST hint specified on a TableScan node at or below the given tree representing a side of a join.
   * Will stop looking once it sees another Join.
   */
  public static class BroadcastHintCollector extends StatelessRelShuttleImpl {

    private boolean shouldBroadcast;

    public BroadcastHintCollector() {
      this.shouldBroadcast = false;
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof RelSubset) {
        RelNode rel = ((RelSubset) other).getOriginal();
        return rel == null ? other : rel.accept(this);
      } else if (other instanceof HepRelVertex) {
        RelNode rel = ((HepRelVertex) other).getCurrentRel();
        return rel == null ? other : rel.accept(this);
      } else if (other instanceof Join) {
        return other;
      } else {
        return super.visit(other);
      }
    }

    @Override
    public RelNode visit(TableScan scan) {
      List<RelHint> hints = scan.getHints();
      this.shouldBroadcast = hints != null
        && hints.stream().anyMatch(hint -> hint.hintName.equalsIgnoreCase("BROADCAST"));
      return scan;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
      return join;
    }

    public boolean shouldBroadcast() {
      return this.shouldBroadcast;
    }
  }
}
