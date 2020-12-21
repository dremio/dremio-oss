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
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeList;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;

public abstract class AbstractArrowTypeVisitor<T> implements ArrowTypeVisitor<T> {

  public AbstractArrowTypeVisitor() {
    super();
  }

  @Override
  public T visit(Null type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(Struct type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(List type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(Union type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(Int type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(FloatingPoint type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(Utf8 type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(Binary type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(Bool type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(Decimal type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(Date type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(Time type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(Timestamp type) {
    return visitGeneric(type);
  }


  @Override
  public T visit(Interval type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(FixedSizeList type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(FixedSizeBinary type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(LargeBinary type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(LargeList type) {
    return visitGeneric(type);
  }

  @Override
  public T visit(LargeUtf8 type) {
    return visitGeneric(type);
  }

  protected abstract T visitGeneric(ArrowType type);

  @Override
  public T visit(ArrowType.Duration type) {
    throw new UnsupportedOperationException("Dremio does not support duration yet.");
  }

  @Override
  public T visit(ArrowType.Map type) {
    throw new UnsupportedOperationException("Dremio does not support map yet.");
  }

}
