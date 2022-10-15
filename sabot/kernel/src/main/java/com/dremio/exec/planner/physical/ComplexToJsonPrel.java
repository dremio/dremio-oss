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

import static com.dremio.exec.record.BatchSchema.assertNoUnion;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.AbstractSchemaConverter;
import com.dremio.exec.physical.config.ComplexToJson;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public class ComplexToJsonPrel extends SingleRel implements Prel, CustomPrel {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.complexToJson.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.complexToJson.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  public ComplexToJsonPrel(Prel phyRelNode) {
    super(phyRelNode.getCluster(), phyRelNode.getTraitSet(), phyRelNode);
  }

  @Override
  public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ComplexToJsonPrel((Prel) sole(inputs));
  }

  @Override
  public Prel getOriginPrel() {
    return ((Prel) getInput());
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    PhysicalOperator child = ((Prel) getInput()).getPhysicalOperator(creator);

    BatchSchema childSchema = child.getProps().getSchema();
    assertNoUnion(childSchema.getFields());

    final SchemaBuilder builder = BatchSchema.newBuilder();
    for(Field f : child.getProps().getSchema()){
      builder.addField(f.getType().accept(new SchemaConverter(f)));
    }
    BatchSchema schema = builder.build();

    return new ComplexToJson(creator.props(this, null, schema, RESERVE, LIMIT), child);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
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
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }


  private static class SchemaConverter extends AbstractSchemaConverter {

    public SchemaConverter(Field field) {
      super(field);
    }

    Field asVarchar(){
      return new Field(field.getName(), new FieldType(field.isNullable(), Utf8.INSTANCE, field.getDictionary()), Collections.<Field>emptyList());
    }

    @Override
    public Field visit(Null type) {
      return asVarchar();
    }

    @Override
    public Field visit(Struct type) {
      return asVarchar();
    }

    @Override
    public Field visit(ArrowType.List type) {
      return asVarchar();
    }

    @Override
    public Field visit(ArrowType.Map type) {
      return asVarchar();
    }

    @Override
    public Field visit(Union type) {
      return asVarchar();
    }

  }

}
