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
package com.dremio.exec.vector.accessor;

import java.math.BigDecimal;

import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.reader.FieldReader;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;

public class UnionSqlAccessor extends AbstractSqlAccessor {

  FieldReader reader;

  public UnionSqlAccessor(UnionVector vector) {
    reader = vector.getReader();
  }

  @Override
  public boolean isNull(int rowOffset) {
    reader.setPosition(rowOffset);
    return reader.isSet();
  }

  @Override
  public BigDecimal getBigDecimal(int rowOffset) throws InvalidAccessException{
    reader.setPosition(rowOffset);
    return reader.readBigDecimal();
  }

  @Override
  public boolean getBoolean(int rowOffset) throws InvalidAccessException{
    reader.setPosition(rowOffset);
    return reader.readBoolean();
  }

  @Override
  public byte getByte(int rowOffset) throws InvalidAccessException{
    reader.setPosition(rowOffset);
    return reader.readByte();
  }

  @Override
  public byte[] getBytes(int rowOffset) throws InvalidAccessException{
    reader.setPosition(rowOffset);
    return reader.readByteArray();
  }

  @Override
  public double getDouble(int rowOffset) throws InvalidAccessException{
    reader.setPosition(rowOffset);
    return reader.readDouble();
  }

  @Override
  public float getFloat(int rowOffset) throws InvalidAccessException{
    reader.setPosition(rowOffset);
    return reader.readFloat();
  }

  @Override
  public int getInt(int rowOffset) throws InvalidAccessException{
    reader.setPosition(rowOffset);
    return reader.readInteger();
  }

  @Override
  public long getLong(int rowOffset) throws InvalidAccessException{
    reader.setPosition(rowOffset);
    return reader.readLong();
  }

  @Override
  public short getShort(int rowOffset) throws InvalidAccessException{
    reader.setPosition(rowOffset);
    return reader.readShort();
  }

  @Override
  public char getChar(int rowOffset) throws InvalidAccessException{
    reader.setPosition(rowOffset);
    return reader.readCharacter();
  }

  @Override
  public String getString(int rowOffset) throws InvalidAccessException{
    reader.setPosition(rowOffset);
    return getObject(rowOffset).toString();
  }

  @Override
  public Object getObject(int rowOffset) throws InvalidAccessException {
    reader.setPosition(rowOffset);
    return reader.readObject();
  }

  @Override
  public MajorType getType() {
    return Types.optional(MinorType.UNION);
  }

  @Override
  public Class<?> getObjectClass() {
    return Object.class;
  }
}
