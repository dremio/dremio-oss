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
package com.dremio.exec.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.Describer;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.AbstractSchemaConverter;
import com.dremio.exec.physical.config.FlattenPOP;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public class FlattenPrel extends SinglePrel implements Prel {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FlattenPrel.class);

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.flatten.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.flatten.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  RexNode toFlatten;

  public FlattenPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode toFlatten) {
    super(cluster, traits, child);
    this.toFlatten = toFlatten;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FlattenPrel(getCluster(), traitSet, sole(inputs), toFlatten);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    // We expect for flattens output to be expanding. Use a constant to expand the data.
    return mq.getRowCount(input) * PrelUtil.getPlannerSettings(getCluster().getPlanner()).getFlattenExpansionAmount();
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    SchemaPath column = (SchemaPath) getFlattenExpression(new ParseContext(PrelUtil.getSettings(getCluster())));
    SchemaBuilder builder = BatchSchema.newBuilder();

    BatchSchema batchSchema = childPOP.getProps().getSchema();
    // flatten operator currently puts the flattened field first
    Field flattenField = batchSchema.findField(column.getAsUnescapedPath());
    builder.addField(flattenField.getType().accept(new SchemaConverter(flattenField, column)));
    for(Field f : batchSchema){
      if (f.getName().equals(column.getAsUnescapedPath())) {
        continue;
      }
      builder.addField(f.getType().accept(new SchemaConverter(f, column)));
    }

    builder.setSelectionVectorMode(SelectionVectorMode.NONE);
    BatchSchema schema = builder.build();

    return new FlattenPOP(
        creator.props(this, null, schema, RESERVE, LIMIT),
        childPOP,
        column);
  }

  @Override
  protected RelDataType deriveRowType() {
    if (PrelUtil.getPlannerSettings(getCluster()).isFullNestedSchemaSupport()) {
      if (toFlatten instanceof RexInputRef) {
        RelDataType rowType = input.getRowType();
        List<RelDataTypeField> inputFields = rowType.getFieldList();
        List<RelDataTypeField> outputFields = new ArrayList<>();
        for (int i = 0; i < inputFields.size(); i++) {
          RelDataTypeField field = inputFields.get(i);
          if (((RexInputRef) toFlatten).getIndex() == i) {
            RelDataType newType = field.getType().getComponentType();
            if (newType == null){
              outputFields.add(field);
            } else {
              outputFields.add(new RelDataTypeFieldImpl(field.getName(), i, newType));
            }
          } else {
            outputFields.add(field);
          }
        }
        final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        for (RelDataTypeField field : outputFields) {
          builder.add(field);
        }
        return builder.build();
      }
    }
    return super.deriveRowType();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("flattenField", this.toFlatten);
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  protected LogicalExpression getFlattenExpression(ParseContext context){
    return RexToExpr.toExpr(context, getInput().getRowType(), getCluster().getRexBuilder(), toFlatten);
  }


  private static class SchemaConverter extends AbstractSchemaConverter {

    private final SchemaPath column;

    public SchemaConverter(Field field, SchemaPath column) {
      super(field);
      this.column = column;
    }

    @Override
    public Field visit(ArrowType.List type) {
      if(field.getName().equals(column.getAsUnescapedPath())){
        Field child = field.getChildren().get(0);
        return new Field(field.getName(), child.isNullable(), child.getType(), child.getChildren());
      }
      return field;
    }

    @Override
    public Field visitGeneric(ArrowType type) {
      if(field.getName().equals(column.getAsUnescapedPath())){
        throw UserException.validationError().message("You're trying to flatten a field that is not a list. The offending field is %s.", Describer.describe(field)).build(logger);
      }
      return super.visitGeneric(type);
    }

  }
}
