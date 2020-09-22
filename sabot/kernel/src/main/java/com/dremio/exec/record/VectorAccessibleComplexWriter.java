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
package com.dremio.exec.record;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class VectorAccessibleComplexWriter extends NonNullableStructVector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorAccessibleComplexWriter.class);

  private final VectorContainer vc;

  public VectorAccessibleComplexWriter(VectorContainer vc) {
    super("", null, new FieldType(false, ArrowType.Struct.INSTANCE, null, null), null);
    this.vc = vc;
  }
  @Override
  public <T extends FieldVector> T addOrGet(String childName, FieldType fieldType, Class<T> clazz) {
    Field field = new Field(childName, fieldType, null);
    final FieldVector v = vc.addOrGet(field);
    putChild(childName, v);
    return this.typeify(v, clazz);

  }

  public static ComplexWriter getWriter(String name, VectorContainer container) {
    VectorAccessibleComplexWriter vc = new VectorAccessibleComplexWriter(container);
    return new ComplexWriterImpl(name, vc);
  }
}
