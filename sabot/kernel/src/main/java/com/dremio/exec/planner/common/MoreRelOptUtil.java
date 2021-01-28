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

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
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

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.resolver.TypeCastRules;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Utility class that is a subset of the RelOptUtil class and is a placeholder for Dremio specific
 * static methods that are needed during either logical or physical planning.
 */
public final class MoreRelOptUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MoreRelOptUtil.class);

  private MoreRelOptUtil() {}

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

  public static int countRelNodes(RelNode rel) {
    int count = 1; // Current node
    for (RelNode child : rel.getInputs()) { // Add children
      count += countRelNodes(child);
    }
    return count;
  }

  public static boolean areRowTypesCompatibleForInsert(
    RelDataType rowType1,
    RelDataType rowType2,
    boolean compareNames,
    boolean allowSubstring) {
    return checkRowTypesCompatiblity(rowType1, rowType2, compareNames, allowSubstring, true);
  }

  public static boolean areRowTypesCompatible(
    RelDataType rowType1,
    RelDataType rowType2,
    boolean compareNames,
    boolean allowSubstring) {
    return checkRowTypesCompatiblity(rowType1, rowType2, compareNames, allowSubstring, false);
  }

  // Similar to RelOptUtil.areRowTypesEqual() with the additional check for allowSubstring
  private static boolean checkRowTypesCompatiblity(
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
      // If one of the types is ANY comparison should succeed
      if (type1.getSqlTypeName() == SqlTypeName.ANY
        || type2.getSqlTypeName() == SqlTypeName.ANY) {
        continue;
      }
      if (!(type1.toString().equals(type2.toString()))) {
        if (allowSubstring
            && (type1.getSqlTypeName() == SqlTypeName.CHAR && type2.getSqlTypeName() == SqlTypeName.CHAR)
            && (type1.getPrecision() <= type2.getPrecision())) {
          continue;
        }

        // Check if Dremio implicit casting can resolve the incompatibility
        List<TypeProtos.MinorType> types = Lists.newArrayListWithCapacity(2);
        TypeProtos.MinorType minorType1 = Types.getMinorTypeFromName(type1.getSqlTypeName().getName());
        TypeProtos.MinorType minorType2 = Types.getMinorTypeFromName(type2.getSqlTypeName().getName());
        types.add(minorType1);
        types.add(minorType2);
        if (insertOp) {
          // Insert is more strict than normal select in terms of implicit casts
          // Return false if TypeCastRules do not allow implicit cast
          if (TypeCastRules.isCastable(minorType1, minorType2, true) &&
            TypeCastRules.getLeastRestrictiveTypeForInsert(types) != null) {
            if (TypeCastRules.isCastSafeFromDataTruncation(type1, type2)) {
              continue;
            }
          }
        } else {
          if (TypeCastRules.getLeastRestrictiveType(types) != null) {
            continue;
          }
        }

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
    final List<RexNode> castExps =
      RexUtil.generateCastExpressions(rexBuilder, castRowType, rowType);

    return projectFactory.createProject(rel, castExps, rowType.getFieldNames());
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
    HashSet<Integer> inputRefReferenced = new HashSet<>();
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
      }
      else if (root instanceof Project &&
        root.getInput(0) instanceof Sort) {
        this.topLevelSort = (Sort) root.getInput(0);
      }
      else {
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

  public static class ContainsRexVisitor extends RexVisitorImpl<Boolean> {

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
    final Deque<Integer> stack = new ArrayDeque<>();
    final List<Integer> result = new ArrayList<>();
    final Pointer<Boolean> found = new Pointer<>(false);
    root.accept(new RoutingShuttle() {
      @Override
      public RelNode visitChildren(RelNode rel) {
        for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
          stack.addLast(input.i);
          rel = visitChild(rel, input.i, input.e);
          stack.removeLast();
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
          result.addAll(stack);
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
    final LinkedHashSet<Integer> constantInd = new LinkedHashSet<>();
    final LinkedHashSet<Integer> groupInd = new LinkedHashSet<>();
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

  public static List<RexNode> identityProjects(RelDataType type, ImmutableBitSet selectedColumns) {
    List<RexNode> projects = new ArrayList<>();
    if (selectedColumns == null) {
      selectedColumns = ImmutableBitSet.range(type.getFieldCount());
    }
    for (Pair<Integer,RelDataTypeField> pair : Pair.zip(selectedColumns, type.getFieldList())) {
      projects.add(new RexInputRef(pair.left, pair.right.getType()));
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
  public static class StructuredConditionRewriter extends StatelessRelShuttleImpl {
    private StructuredConditionRewriter() {
    }

    @Override
    public RelNode visit(LogicalProject project) {
      RelNode newInput = project.getInput().accept(this);
      final ConditionFlattenter flattener = new ConditionFlattenter(project.getCluster().getRexBuilder());
      List<RexNode> newExprs = project.getChildExps().stream().map(expr -> expr.accept(flattener)).collect(Collectors.toList());
      return LogicalProject.create(newInput, newExprs, project.getRowType().getFieldNames());
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
      RelNode newInput = filter.getInput().accept(this);
      final ConditionFlattenter flattener = new ConditionFlattenter(filter.getCluster().getRexBuilder());
      return LogicalFilter.create(newInput, filter.getCondition().accept(flattener));
    }

    public static RelNode rewrite(RelNode rel) {
      return rel.accept(new StructuredConditionRewriter());
    }

    /**
     *
     */
    private static class ConditionFlattenter extends RexShuttle {
      private RexBuilder builder;

      private ConditionFlattenter(RexBuilder builder) {
        this.builder = builder;
      }

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

    public static int count(RexNode node) {
      RexNodeCountVisitor v = new RexNodeCountVisitor();
      node.accept(v);
      return v.count.value;
    }

    Pointer <Integer>count = new Pointer<Integer>(0);
    public int getRexNodeCount() {
      return count.value;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      count.value++;
      return super.visitCall(call);
    }

    @Override
    public RexNode visitInputRef(RexInputRef input) {
      count.value++;
      return super.visitInputRef(input);
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      count.value++;
      return super.visitLiteral(literal);
    }
  }
}
