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
package com.dremio.exec.planner.sql.handlers;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.PlannerType;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.google.common.collect.Sets;

public final class RexSubQueryUtils {

  private RexSubQueryUtils() {}

  /*
   * Finds RexSubQuery/FieldAccess/CorrelatedVariable in RelNode's expressions.
   */
  public static class RexSubQueryFinder extends RexShuttle {
    private boolean foundRexSubQuery = false;
    private boolean foundFieldAccess = false;
    private boolean foundCorrelVariable = false;

    public boolean getFoundSubQuery() {
      return foundRexSubQuery || foundFieldAccess || foundCorrelVariable;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      foundRexSubQuery = true;
      return super.visitSubQuery(subQuery);
    }

    @Override
    public RexNode visitCorrelVariable(RexCorrelVariable variable) {
      foundCorrelVariable = true;
      return super.visitCorrelVariable(variable);
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      foundFieldAccess = true;
      return super.visitFieldAccess(fieldAccess);
    }
  }

  /**
   * Calls the DefaultSqlHandler's transform() on each RexSubQuery's subtree.
   */
  public static class RexSubQueryTransformer extends RexShuttle {

    private final SqlHandlerConfig config;
    private RelTraitSet traitSet;
    private boolean failed = false;

    public boolean isFailed() {
      return failed;
    }

    public RexSubQueryTransformer(SqlHandlerConfig config) {
      this.config = config;
    }

    public void setTraitSet(RelTraitSet traitSet) {
      this.traitSet = traitSet;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      RelNode transformed;
      try {
        final RelNode convertedSubquery = subQuery.rel.accept(new InjectSampleAndJdbcLogical(false, true));
        transformed = PrelTransformer.transform(config, PlannerType.VOLCANO, PlannerPhase.JDBC_PUSHDOWN, convertedSubquery, traitSet, false);
      } catch (Throwable t) {
        failed = true;
        return subQuery;
      }
      return subQuery.clone(transformed);
    }
  }

  /**
   * Transforms a RelNode with RexSubQuery into JDBC convention.  Does so by using {@link RexSubQueryTransformer}.
   */
  public static class RelsWithRexSubQueryTransformer extends StatelessRelShuttleImpl {

    private final RexSubQueryTransformer transformer;

    public RelsWithRexSubQueryTransformer(SqlHandlerConfig config) {
      this.transformer = new RexSubQueryTransformer(config);
    }

    public boolean failed() {
      return transformer.isFailed();
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      RelNode newParent = parent;
      if (parent.getConvention() instanceof JdbcConventionIndicator) {
        transformer.setTraitSet(parent.getTraitSet().plus(DistributionTrait.ANY).plus(RelCollations.EMPTY));
        newParent = parent.accept(transformer);
      }
      return super.visitChild(newParent, i, newParent.getInput(i));
    }
  }

  /**
   * Calls the sqlToRelConverter's flattenTypes on each RexSubQuery's subtree.
   */
  public static class RexSubQueryFlattener extends RexShuttle {

    private final SqlToRelConverter converter;

    public RexSubQueryFlattener(SqlToRelConverter converter) {
      this.converter = converter;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      final RelNode transformed = converter.flattenTypes(subQuery.rel, true);
      final RelNode transformed2 = transformed.accept(new RelsWithRexSubQueryFlattener(converter));
      return subQuery.clone(transformed2);
    }
  }

  /**
   * Transforms a RelNode with RexSubQuery into JDBC convention.  Does so by using {@link RexSubQueryTransformer}.
   */
  public static class RelsWithRexSubQueryFlattener extends StatelessRelShuttleImpl {

    private final RexSubQueryFlattener flattener;

    public RelsWithRexSubQueryFlattener(SqlToRelConverter converter) {
      flattener = new RexSubQueryFlattener(converter);
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      RelNode newParent = parent.accept(flattener);
      return super.visitChild(newParent, i, newParent.getInput(i));
    }
  }


  /**
   * Finds relnodes that are not of JDBC convention that have RexSubQuery rexnodes
   */
  public static class FindNonJdbcConventionRexSubQuery {

    public boolean visit(final RelNode node) {
      for (RelNode input : node.getInputs()) {
        if (visit(input)) {
          return true;
        }
      }

      if (node.getConvention() instanceof JdbcConventionIndicator) {
        return false;
      }

      final RexSubQueryFinder subQueryFinder = new RexSubQueryFinder();
      node.accept(subQueryFinder);
      if (subQueryFinder.getFoundSubQuery()) {
        return true;
      }
      return false;
    }
  }

  /**
   * Checks that the subquery is of JDBC convention.
   */
  public static class RexSubQueryConventionChecker extends RexShuttle {
    private final Set<Convention> conventions = Sets.newHashSet();
    private boolean cannotPushdownRexSubQuery = false;

    public boolean isCannotPushdownRexSubQuery() {
      return cannotPushdownRexSubQuery;
    }

    public Set<Convention> getConventions() {
      return conventions;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      RexSubQueryUtils.RexSubQueryPushdownChecker checker = new RexSubQueryUtils.RexSubQueryPushdownChecker();
      checker.visit(subQuery.rel);
      conventions.addAll(checker.getAllConventionsFound());
      if (checker.cannotPushdownRexSubQuery()) {
        cannotPushdownRexSubQuery = true;
      }
      return super.visitSubQuery(subQuery);
    }
  }

  /**
   * Checks for RexSubQuery rexnodes in the tree, and if there are any, ensures that the underlying
   * table scans for these RexSubQuery is of a single JDBC Convention (cannot pushdown a subquery
   * that is across multiple/different databases).
   */
  public static class RexSubQueryPushdownChecker {

    private final Set<Convention> allConventionsFound = Sets.newHashSet();
    private boolean cannotPushdownRexSubQuery = false;
    private boolean foundRexSubQuery = false;

    public boolean cannotPushdownRexSubQuery() {
      return cannotPushdownRexSubQuery;
    }

    public boolean foundRexSubQuery() {
      return foundRexSubQuery;
    }

    public Set<Convention> getAllConventionsFound() {
      return allConventionsFound;
    }

    public Set<Convention> visit(final RelNode node) {
      if (node instanceof TableScan) {
        allConventionsFound.add(node.getConvention());
        return Sets.newHashSet(node.getConvention());
      }

      if (cannotPushdownRexSubQuery) {
        return Sets.newHashSet();
      }

      final Set<Convention> childrenConventions = new HashSet<>();
      for (RelNode input : node.getInputs()) {
        childrenConventions.addAll(visit(input));
      }

      final RexSubQueryFinder subQueryFinder = new RexSubQueryFinder();
      node.accept(subQueryFinder);
      if (!subQueryFinder.getFoundSubQuery()) {
        return childrenConventions;
      }

      foundRexSubQuery = true;

      // Check that the subquery has the same jdbc convention as well!
      final RexSubQueryConventionChecker subQueryConventionChecker = new RexSubQueryConventionChecker();
      node.accept(subQueryConventionChecker);
      childrenConventions.addAll(subQueryConventionChecker.getConventions());
      if (subQueryConventionChecker.isCannotPushdownRexSubQuery()) {
        return childrenConventions;
      }

      // Found some subquery or field access or correlated variables.
      Convention found = null;
      boolean foundMoreThanOneConvention = false;
      boolean foundNoneJdbcConvention = false;
      for (final Convention childConvention : childrenConventions) {
        if (found == null) {
          found = childConvention;
          if (!(found instanceof JdbcConventionIndicator)) {
            foundNoneJdbcConvention = true;
            break;
          }
        } else if (found != childConvention) {
          foundMoreThanOneConvention = true;
          break;
        }
      }

      if (foundMoreThanOneConvention || foundNoneJdbcConvention) {
        cannotPushdownRexSubQuery = true;
      }

      return childrenConventions;
    }
  }
}
