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
package com.dremio.exec.planner.physical.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.options.OptionManager;
import com.google.common.base.Preconditions;

/*
* This class validates there are no join conditions containing columns from the same side of the tables in join.
* Otherwise, those become predicates we can push down so join operator has fewer rows of data as input.
* This should be validated after all push-down is finished.
* */
public final class JoinConditionValidatorVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {
  private static final JoinConditionValidatorVisitor INSTANCE = new JoinConditionValidatorVisitor();

  public static Prel validate(Prel prel, OptionManager options) {
    return options.getOption(PlannerSettings.JOIN_CONDITIONS_VALIDATION) ?
      prel.accept(INSTANCE, null) :
      prel;
  }

  public void validateJoinCondition(int leftTableColumnCount, int rightTableColumnCount, RexNode condition){

    JoinConditionValidator joinConditionValidator = new JoinConditionValidator(leftTableColumnCount, rightTableColumnCount);
    condition.accept(joinConditionValidator);

    boolean isValid = joinConditionValidator.isValid();

    String message = String.format("Join condition %s is invalid.", condition);

    Preconditions.checkArgument(isValid,
      message);
  }
  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = new ArrayList<>();

    for (Prel child : prel) {
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitJoin(JoinPrel prel, Void voidValue) throws RuntimeException {
    RexNode condition = prel.getCondition();
    int leftTableColumnCount= prel.getLeft().getRowType().getFieldCount();
    int rightTableColumnCount= prel.getRight().getRowType().getFieldCount();

    validateJoinCondition(leftTableColumnCount,rightTableColumnCount,condition);
    return prel;
  }

  private static class JoinConditionValidator extends RexVisitorImpl<Void> {

    private final int leftTableColumnCount;
    private final int rightTableColumnCount;

    private boolean isValid;

    public boolean isValid() {
      return isValid;
    }

    public JoinConditionValidator(int leftTableColumnCount, int rightTableColumnCount) {
      super(true);
      this.leftTableColumnCount = leftTableColumnCount;
      this.rightTableColumnCount = rightTableColumnCount;
      this.isValid = true;
    }

    @Override
    public Void visitCall(RexCall call){
      if(call.op.kind== SqlKind.EQUALS
        || call.op.kind== SqlKind.NOT_EQUALS
        || call.op.kind== SqlKind.LESS_THAN
        || call.op.kind== SqlKind.LESS_THAN_OR_EQUAL
        || call.op.kind== SqlKind.GREATER_THAN
        || call.op.kind== SqlKind.GREATER_THAN_OR_EQUAL
      ) {
        if (call.operands.get(0) instanceof RexInputRef && call.operands.get(1) instanceof RexInputRef) {

          int in1 = ((RexInputRef) call.operands.get(0)).getIndex();
          int in2 = ((RexInputRef) call.operands.get(1)).getIndex();
          if ((in1 < leftTableColumnCount && in2 < leftTableColumnCount) // If both expressions are on left side
            || (in1 >= leftTableColumnCount && in2 >= leftTableColumnCount) // If both expressions are on right side
            || (in1 >= leftTableColumnCount + rightTableColumnCount || in2 >= leftTableColumnCount + rightTableColumnCount)) // If either of them is beyond upper bound
          {
            isValid = false;
          }
        }
      }
      return super.visitCall(call);
    }
  }

}
