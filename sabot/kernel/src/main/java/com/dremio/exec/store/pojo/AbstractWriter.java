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
package com.dremio.exec.store.pojo;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;

import java.lang.reflect.Field;

import org.apache.arrow.vector.ValueVector;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.sabot.op.scan.OutputMutator;

abstract class AbstractWriter<V extends ValueVector> implements PojoWriter{

  protected final Field field;
  protected V vector;
  protected final MajorType type;

  public AbstractWriter(Field field, MajorType type){
    this.field = field;
    this.type = type;
  }

  @Override
  public void init(OutputMutator output) throws SchemaChangeException {
//    MaterializedField mf = MajorTypeHelper.getFieldForNameAndMajorType(field.getName(), type);
    org.apache.arrow.vector.types.pojo.Field mf = new org.apache.arrow.vector.types.pojo.Field(field.getName(), true, getArrowMinorType(type.getMinorType()).getType(), null);
    @SuppressWarnings("unchecked")
    Class<V> valueVectorClass = (Class<V>) TypeHelper.getValueVectorClass(getArrowMinorType(type.getMinorType()));
    this.vector = output.addField(mf, valueVectorClass);
  }

  @Override
  public void allocate() {
    vector.allocateNew();
  }

  public void setValueCount(int valueCount){
    vector.setValueCount(valueCount);
  }

  @Override
  public void cleanup() {
  }


}
