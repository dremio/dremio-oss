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
package com.dremio.common.util;

import org.apache.arrow.vector.types.pojo.ArrowType;

import com.google.flatbuffers.FlatBufferBuilder;

public class ObjectType extends ArrowType.Null {

  public static ArrowType INTERNAL_OBJECT_TYPE = new ObjectType();

  private ObjectType(){}

  @Override
  public ArrowTypeID getTypeID() {
    return ArrowTypeID.NONE;
  }

  @Override
  public int getType(FlatBufferBuilder flatBufferBuilder) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T accept(ArrowTypeVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public boolean isComplex() {
    return false;
  }

}
