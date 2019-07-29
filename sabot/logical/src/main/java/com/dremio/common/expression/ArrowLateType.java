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
package com.dremio.common.expression;

import org.apache.arrow.vector.types.pojo.ArrowType;

import com.google.flatbuffers.FlatBufferBuilder;

public final class ArrowLateType extends ArrowType {

  public static final ArrowType INSTANCE = new ArrowLateType();

  private ArrowLateType(){}
  @Override
  public ArrowTypeID getTypeID() {
    return null;
  }

  @Override
  public int getType(FlatBufferBuilder builder) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T accept(ArrowTypeVisitor<T> visitor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isComplex() {
    throw new UnsupportedOperationException();
  }
}
