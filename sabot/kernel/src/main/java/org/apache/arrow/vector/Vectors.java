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
package org.apache.arrow.vector;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import io.netty.buffer.ArrowBuf;

public class Vectors {

  private static final Field LAST_SET_VARCHAR;
  private static final Field LAST_SET_VARBINARY;
  private static final Method fillEmptiesVarChar;
  private static final Method fillEmptiesVarBinary;
  static {
    try {
      LAST_SET_VARCHAR = NullableVarCharVector.Mutator.class.getDeclaredField("lastSet");
      LAST_SET_VARCHAR.setAccessible(true);
      LAST_SET_VARBINARY = NullableVarBinaryVector.Mutator.class.getDeclaredField("lastSet");
      LAST_SET_VARBINARY.setAccessible(true);

      fillEmptiesVarChar = NullableVarCharVector.Mutator.class.getDeclaredMethod("fillEmpties", int.class);
      fillEmptiesVarChar.setAccessible(true);
      fillEmptiesVarBinary = NullableVarBinaryVector.Mutator.class.getDeclaredMethod("fillEmpties", int.class);
      fillEmptiesVarBinary.setAccessible(true);
    } catch (NoSuchFieldException | SecurityException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  public static ArrowBuf allocateData(NullableVarCharVector vector) {
    vector.getValuesVector().data.release();
    vector.getValuesVector().data = vector.allocator.buffer(32*1024);
    return vector.getValuesVector().data;
  }

  public static ArrowBuf allocateData(NullableVarBinaryVector vector) {
    vector.getValuesVector().data.release();
    vector.getValuesVector().data = vector.allocator.buffer(32*1024);
    return vector.getValuesVector().data;
  }

  public static void swapDataBuf(NullableVarCharVector vector, ArrowBuf newData) {
    vector.getValuesVector().data.release();
    vector.getValuesVector().data = newData;
  }

  public static void setLastSet(NullableVarCharVector.Mutator mutator, int i) {
    try {
      LAST_SET_VARCHAR.set(mutator, i);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static void setLastSet(NullableVarBinaryVector.Mutator mutator, int i) {
    try {
      LAST_SET_VARBINARY.set(mutator, i);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }


  public static void fillEmpties(NullableVarCharVector.Mutator mutator, int i) {
    try {
      fillEmptiesVarChar.invoke(mutator, i);
    } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static void fillEmpties(NullableVarBinaryVector.Mutator mutator, int i) {
    try {
      fillEmptiesVarBinary.invoke(mutator, i);
    } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
