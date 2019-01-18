/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.Iterator;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.Describer;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;

@JsonTypeName("flatten")
public class FlattenPOP extends AbstractSingle {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FlattenPOP.class);

  private SchemaPath column;

  @JsonCreator
  public FlattenPOP(
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("column") SchemaPath column) {
    super(child);
    this.column = column;
  }


  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.singletonIterator(child);
  }

  public SchemaPath getColumn() {
    return column;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitFlatten(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new FlattenPOP(child, column);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.FLATTEN_VALUE;
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext context) {
    SchemaBuilder builder = BatchSchema.newBuilder();
    BatchSchema batchSchema = child.getSchema(context);
    // flatten operator currently puts the flattened field first
    Field flattenField = batchSchema.findField(column.getAsUnescapedPath());
    builder.addField(flattenField.getType().accept(new SchemaConverter(flattenField)));
    for(Field f : batchSchema){
      if (f.getName().equals(column.getAsUnescapedPath())) {
        continue;
      }
      builder.addField(f.getType().accept(new SchemaConverter(f)));
    }
    builder.setSelectionVectorMode(SelectionVectorMode.NONE);
    return builder.build();
  }

  private class SchemaConverter extends AbstractSchemaConverter {

    public SchemaConverter(Field field) {
      super(field);
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
