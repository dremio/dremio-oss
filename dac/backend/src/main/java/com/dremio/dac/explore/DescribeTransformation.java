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
package com.dremio.dac.explore;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;

import com.dremio.dac.explore.model.FieldTransformationBase;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.proto.model.dataset.ConvertCase;
import com.dremio.dac.proto.model.dataset.Dimension;
import com.dremio.dac.proto.model.dataset.JoinCondition;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.TransformAddCalculatedField;
import com.dremio.dac.proto.model.dataset.TransformConvertCase;
import com.dremio.dac.proto.model.dataset.TransformConvertToSingleType;
import com.dremio.dac.proto.model.dataset.TransformCreateFromParent;
import com.dremio.dac.proto.model.dataset.TransformDrop;
import com.dremio.dac.proto.model.dataset.TransformExtract;
import com.dremio.dac.proto.model.dataset.TransformField;
import com.dremio.dac.proto.model.dataset.TransformFilter;
import com.dremio.dac.proto.model.dataset.TransformGroupBy;
import com.dremio.dac.proto.model.dataset.TransformJoin;
import com.dremio.dac.proto.model.dataset.TransformLookup;
import com.dremio.dac.proto.model.dataset.TransformRename;
import com.dremio.dac.proto.model.dataset.TransformSort;
import com.dremio.dac.proto.model.dataset.TransformSorts;
import com.dremio.dac.proto.model.dataset.TransformSplitByDataType;
import com.dremio.dac.proto.model.dataset.TransformTrim;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.proto.model.dataset.TrimType;
import com.google.common.base.Joiner;

class DescribeTransformation implements TransformBase.TransformVisitor<String> {

  @Override
  public String visit(TransformLookup lookup) throws Exception {
    // TODO
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public String visit(TransformJoin join) throws Exception {
    String jointype;
    switch (join.getJoinType()) {
    case Inner:
      jointype = "Inner";
      break;
    case LeftOuter:
      jointype = "Left outer";
      break;
    case RightOuter:
      jointype = "Rigth outer";
      break;
    case FullOuter:
      jointype = "Full outer";
      break;
    default:
      throw new UnsupportedOperationException("Unknown join type " + join.getJoinType().name());
    }
    List<String> conditions = new ArrayList<>();
    for (JoinCondition jc : join.getJoinConditionsList()) {
      conditions.add(jc.getLeftColumn() + " = " + jc.getRightColumn());
    }
    // TODO show quoted path
    return jointype + " join with " + Joiner.on(".").join(join.getRightTableFullPathList()) + " on "
        + Joiner.on(" and ").join(conditions);
  }

  @Override
  public String visit(TransformSort sort) throws Exception {
    return "Sort by " + describe(new Order(sort.getSortedColumnName(), sort.getOrder()));
  }

  @Override
  public String visit(TransformSorts sortMultiple) throws Exception {
    List<Order> columnsList = sortMultiple.getColumnsList();
    List<String> display = new ArrayList<>();
    for (Order order : columnsList) {
      display.add(describe(order));
    }
    return "Sort by " + Joiner.on(',').join(display);
  }

  private String describe(Order order) {
    return order.getName() + " " + order.getDirection();
  }

  @Override
  public String visit(TransformDrop drop) throws Exception {
    return "Drop " + drop.getDroppedColumnName();
  }

  @Override
  public String visit(TransformRename rename) throws Exception {
    return "Rename " + rename.getOldColumnName() + " to " + rename.getNewColumnName();
  }

  @Override
  public String visit(TransformConvertCase convertCase) throws Exception {
    return describeConvertCase(convertCase.getConvertCase(), convertCase.getColumnName());
  }

  private String describeConvertCase(ConvertCase c, String columnName) {
    String display;
    switch (c) {
    case LOWER_CASE:
      display = "lower case";
      break;
    case UPPER_CASE:
      display = "upper case";
      break;
    case TITLE_CASE:
      display = "title case";
      break;
    default:
      throw new UnsupportedOperationException("Unknown case " + c);
    }
    return "Convert case of " + columnName + " to " + display;
  }

  @Override
  public String visit(TransformTrim trim) throws Exception {
    return describeTrim(trim.getColumnName(), trim.getTrimType());
  }

  private String describeTrim(String columnName, TrimType trimType) {
    String display;
    switch (trimType) {
    case BOTH:
      display = "on both sides";
      break;
    case LEFT:
      display = "on the left";
      break;
    case RIGHT:
      display = "on the right";
      break;
    default:
      throw new UnsupportedOperationException("Unknown trim " + trimType);
    }
    return "Trim " + columnName + " " + display;
  }

  @Override
  public String visit(TransformExtract extract) throws Exception {
    return "Extract " + ExtractRecommender.describe(extract.getRule()) + " as " + extract.getNewColumnName();
  }

  @Override
  public String visit(TransformAddCalculatedField addCalculatedField) throws Exception {
    return "Add calculated field " + addCalculatedField.getNewColumnName();
  }

  @Override
  public String visit(TransformUpdateSQL updateSQL) throws Exception {
    return "SQL Edited to: " + updateSQL.getSql();
  }

  @Override
  public String visit(final TransformField field) throws Exception {
    return FieldTransformationBase.unwrap(field.getFieldTransformation())
        .accept(new DescribeFieldTransformation(field));
  }

  @Override
  public String visit(TransformConvertToSingleType convertToSingleType) throws Exception {
    return format("convert mixed type %s to single type %s", convertToSingleType.getSourceColumnName(),
        convertToSingleType.getDesiredType());
  }

  @Override
  public String visit(TransformSplitByDataType splitByDataType) throws Exception {
    return format("split mixed type %s to separate columns %s*", splitByDataType.getSourceColumnName(),
        splitByDataType.getNewColumnNamePrefix());
  }

  @Override
  public String visit(TransformGroupBy groupBy) throws Exception {
    List<String> cols = new ArrayList<>();
    List<Dimension> columnsDimensionsList = groupBy.getColumnsDimensionsList();
    if (columnsDimensionsList != null) {
      for (Dimension dimension : columnsDimensionsList) {
        cols.add(dimension.getColumn());
      }
    }
    return "Group by " + Joiner.on(", ").join(cols);
  }

  @Override
  public String visit(TransformFilter filter) throws Exception {
    return format("filter rows*");
  }

  @Override
  public String visit(TransformCreateFromParent createFromParent) throws Exception {
    return "Dataset creation";
  }

}
