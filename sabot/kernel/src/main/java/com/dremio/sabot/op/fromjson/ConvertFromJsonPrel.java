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
package com.dremio.sabot.op.fromjson;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.SinglePrel;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPOP.ConversionColumn;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class ConvertFromJsonPrel extends SinglePrel {

  private final List<ConversionColumn> conversions;

  public ConvertFromJsonPrel(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType, RelNode child, List<ConversionColumn> conversions) {
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
    return new ConvertFromJsonPrel(getCluster(), getTraitSet(), rowType, inputs.get(0), conversions);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    ConvertFromJsonPOP p = new ConvertFromJsonPOP(((Prel) getInput()).getPhysicalOperator(creator), conversions);
    return creator.addMetadata(this, p);
  }

  public List<ConversionColumn> getConversions() {
    return conversions;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    List<RexNode> childExprs = input.getChildExps();

    List<String> convertFields = Lists.transform(conversions, new Function<ConversionColumn, String>() {
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
}
