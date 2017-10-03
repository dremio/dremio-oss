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
package com.dremio.exec.physical.config;

import java.util.Collections;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("complex-to-json")
public class ComplexToJson extends AbstractSingle {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComplexToJson.class);

  @JsonCreator
  public ComplexToJson(@JsonProperty("child") PhysicalOperator child) {
    super(child);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitOp(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new ComplexToJson(child);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.COMPLEX_TO_JSON_VALUE;
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext context) {
    final SchemaBuilder builder = BatchSchema.newBuilder();
    for(Field f : child.getSchema(context)){
      builder.addField(f.getType().accept(new SchemaConverter(f)));
    }
    return builder.build();
  }

  private class SchemaConverter extends AbstractSchemaConverter {

    public SchemaConverter(Field field) {
      super(field);
    }

    Field asVarchar(){
      return new Field(field.getName(), true, Utf8.INSTANCE, Collections.<Field>emptyList());
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
    public Field visit(Union type) {
      return asVarchar();
    }

  }
}
