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
package com.dremio.plugins.elastic.planning.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
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
import org.apache.calcite.rex.RexVisitorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.google.common.collect.Lists;

/**
 * Finds the source column names in elasticsearch (complex expressions with multiple column references in a project
 * column should also resolve to a set of elastic source column names).
 */
public class ElasticSourceNameFinder extends RexVisitorImpl<List<List<String>>> {

  private static final Logger logger = LoggerFactory.getLogger(ElasticSourceNameFinder.class);

  private final RelDataType rowType;

  public ElasticSourceNameFinder(final RelDataType rowType) {
    super(true);
    this.rowType = rowType;
  }

  public static List<SchemaPath> getSchemaPaths(List<List<String>> sourceNames) {
    List<SchemaPath> schemaPaths = Lists.newArrayList();
    for (List<String> oneName : sourceNames) {
      String[] array = new String[oneName.size()];
      schemaPaths.add(SchemaPath.getCompoundPath(oneName.toArray(array)));
    }
    return schemaPaths;
  }

  @Override
  public List<List<String>> visitLiteral(RexLiteral literal) {
    return new ArrayList<List<String>>();
  }

  @Override
  public List<List<String>> visitInputRef(RexInputRef inputRef) {
    final int index = inputRef.getIndex();
    final RelDataTypeField field = rowType.getFieldList().get(index);
    return Lists.<List<String>>newArrayList(Lists.newArrayList(field.getName()));
  }

  @Override
  public List<List<String>> visitFieldAccess(RexFieldAccess fieldAccess) {
    RexNode leftRex = fieldAccess.getReferenceExpr();
    RelDataTypeField field = fieldAccess.getField();
    List<List<String>> left = leftRex.accept(this);

    if (left.isEmpty()) {
      // left was rex literal
      left.add(Lists.newArrayList(leftRex.toString()));
    }

    left.get(0).addAll(Lists.newArrayList(field.getName()));
    return left;
  }

  @Override
  public List<List<String>> visitCall(RexCall call) {
    if (call.getOperator().getName().equalsIgnoreCase("item") || call.getOperator().getName().equalsIgnoreCase("dot")) {
      if (call.getOperands().size() != 2) {
        throw UserException.planError().message("Item operator should only have two operands, but got " + call.getOperands().size()).build(logger);
      }
      RexNode leftRex = call.getOperands().get(0);
      RexNode rightRex = call.getOperands().get(1);
      List<List<String>> left = leftRex.accept(this);
      List<List<String>> right = rightRex.accept(this);

      if (left.isEmpty()) {
        // left was rex literal
        left.add(Lists.newArrayList(leftRex.toString()));
      }

      if (right.isEmpty()) {
        right.add(Lists.newArrayList(rightRex.toString()));
      }

      assert left.size() == 1 && right.size() == 1;

      left.get(0).addAll(right.get(0));
      return left;
    } else {
      Set<List<String>> childrenNames = new HashSet<List<String>>();
      for (RexNode rexnode : call.getOperands()) {
        List<List<String>> childNames = rexnode.accept(this);
        childrenNames.addAll(childNames);
      }
      return Lists.newArrayList(childrenNames);
    }
  }

  @Override
  public List<List<String>> visitDynamicParam(RexDynamicParam dynamicParam) {
    return visitUnknown(dynamicParam);
  }

  @Override
  public List<List<String>> visitRangeRef(RexRangeRef rangeRef) {
    return visitUnknown(rangeRef);
  }

  @Override
  public List<List<String>> visitLocalRef(RexLocalRef localRef) {
    return visitUnknown(localRef);
  }

  @Override
  public List<List<String>> visitOver(RexOver over) {
    return visitUnknown(over);
  }

  @Override
  public List<List<String>> visitCorrelVariable(RexCorrelVariable correlVariable) {
    return visitUnknown(correlVariable);
  }

  protected List<List<String>> visitUnknown(RexNode o){
    // raise an error
    throw UserException.planError()
            .message("Unsupported for elastic pushdown: RexNode Class: %s, RexNode Digest: %s", o.getClass().getName(), o.toString())
            .build(logger);
  }
}
