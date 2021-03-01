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
package com.dremio.exec.store;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

/**
 * MinMaxRewriter
 * Update columns in range to reference actual min and max columns from input
 */
public class MinMaxRewriter extends RexShuttle {
  private final RexBuilder builder;
  private final List<String> fieldNames;
  private final RelNode input;

  public MinMaxRewriter(RexBuilder builder, RelDataType rowType, RelNode input) {
    this.builder = builder;
    this.input = input;
    this.fieldNames = rowType.getFieldNames();
  }

  @Override
  public RexNode visitCall(RexCall call) {
    boolean isLessThan = false;
    switch (call.getOperator().getKind()) {
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        isLessThan = true;
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        final RexNode arg1 = call.getOperands().get(0);
        final RexNode arg2 = call.getOperands().get(1);
        final RexInputRef inputRef;
        final RexNode other;
        boolean inputFirst = true;
        if ((arg1 instanceof RexInputRef) && !(arg2 instanceof RexInputRef)) {
          inputRef = (RexInputRef) arg1;
          other = arg2;
        } else {
          inputRef = (RexInputRef) arg2;
          other = arg1;
          inputFirst = false;
        }
        final String newFieldName = fieldNames.get(inputRef.getIndex()) + getSuffix(inputFirst, isLessThan);
        int newFieldInd = input.getRowType().getFieldNames().indexOf(newFieldName);
        if (newFieldInd == -1) {
          throw new IllegalArgumentException(String.format("Could not find column, %s, from input, %s.", newFieldName, input.getRowType()));
        }
        final RexInputRef newInput = builder.makeInputRef(input, newFieldInd);
        return inputFirst ? builder.makeCall(call.getOperator(), newInput, other) : builder.makeCall(call.getOperator(), other, newInput);
      case AND:
      case OR:
        final List<RexNode> andOrOperands = visitList(call.operands, new boolean[]{false});
        return builder.makeCall(call.getOperator(), andOrOperands);
      default:
        return call;
    }
  }

  private String getSuffix(boolean inputFirst, boolean isLessThan) {
    return inputFirst ^ isLessThan ? "_max" : "_min";
  }
}

