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
package com.dremio.sabot.op.fromjson;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.SinglePrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPOP.ConversionColumn;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

@Options
public class ConvertFromJsonPrel extends SinglePrel {

  public static final LongValidator RESERVE =
      new PositiveLongValidator(
          "planner.op.convert_from_json.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT =
      new PositiveLongValidator(
          "planner.op.convert_from_json.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  private final List<ConversionColumn> conversions;

  public ConvertFromJsonPrel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelDataType rowType,
      RelNode child,
      List<ConversionColumn> conversions) {
    super(cluster, traits, child);
    this.rowType = rowType;
    this.conversions = conversions;
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ConvertFromJsonPrel(
        getCluster(), getTraitSet(), rowType, inputs.get(0), conversions);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    PhysicalOperator child = ((Prel) getInput()).getPhysicalOperator(creator);
    return new ConvertFromJsonPOP(
        creator.props(
            this, null, getSchema(child.getProps().getSchema(), conversions), RESERVE, LIMIT),
        child,
        conversions);
  }

  public List<ConversionColumn> getConversions() {
    return conversions;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    List<RexNode> childExprs = MoreRelOptUtil.getChildExps(input);

    List<String> convertFields =
        Lists.transform(
            conversions,
            new Function<ConversionColumn, String>() {
              @Override
              public String apply(ConversionColumn input) {
                return input.getInputField();
              }
            });

    for (Ord<RelDataTypeField> field : Ord.zip(rowType.getFieldList())) {
      String fieldName = field.e.getName();
      if (fieldName == null) {
        fieldName = "field#" + field.i;
      }
      if (convertFields.contains(fieldName)) {
        pw.item(fieldName, "CONVERT(" + fieldName + ")");
      } else {
        pw.item(fieldName, childExprs.get(field.i));
      }
    }

    pw.item("conversions", conversions);

    return pw;
  }

  private static BatchSchema getSchema(BatchSchema schema, List<ConversionColumn> conversions) {
    final Map<String, ConversionColumn> cMap = new HashMap<>();
    for (ConversionColumn c : conversions) {
      cMap.put(c.getInputField().toLowerCase(), c);
    }

    final SchemaBuilder builder = BatchSchema.newBuilder();
    for (Field f : schema) {
      ConversionColumn conversion = cMap.get(f.getName().toLowerCase());
      if (conversion != null) {
        builder.addField(conversion.asField(f.getName()));
      } else {
        builder.addField(f);
      }
    }

    builder.setSelectionVectorMode(SelectionVectorMode.NONE);
    return builder.build();
  }
}
