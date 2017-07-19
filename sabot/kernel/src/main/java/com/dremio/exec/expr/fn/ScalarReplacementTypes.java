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
package com.dremio.exec.expr.fn;

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.DateMilliHolder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.holders.IntervalYearHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalYearHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.TimeMilliHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.google.common.collect.ImmutableSet;

/**
 * Reference list of classes we will perform scalar replacement on.
 */
public class ScalarReplacementTypes {
  // This class only has static utilities.
  private ScalarReplacementTypes() {
  }

  static {
    Class<?>[] classList = {
        BitHolder.class,
        IntHolder.class,
        BigIntHolder.class,
        Float4Holder.class,
        Float8Holder.class,
        IntervalDayHolder.class,
        IntervalYearHolder.class,
        DateMilliHolder.class,
        TimeMilliHolder.class,
        TimeStampMilliHolder.class,
        VarCharHolder.class,
        VarBinaryHolder.class,
        NullableBitHolder.class,
        NullableIntHolder.class,
        NullableBigIntHolder.class,
        NullableFloat4Holder.class,
        NullableFloat8Holder.class,
        NullableVarCharHolder.class,
        NullableVarBinaryHolder.class,
        NullableIntervalDayHolder.class,
        NullableIntervalYearHolder.class,
        NullableDateMilliHolder.class,
        NullableTimeMilliHolder.class,
        NullableTimeStampMilliHolder.class,
    };

    CLASSES = ImmutableSet.copyOf(classList);
  }

  public static final ImmutableSet<Class<?>> CLASSES;

  /**
   * Determine if a class is a holder class.
   *
   * @param className the name of the class
   * @return true if the class belongs to the CLASSES set.
   */
  public static boolean isHolder(final String className) {
    try {
      final Class<?> clazz = Class.forName(className);
      return CLASSES.contains(clazz);
    } catch (ClassNotFoundException e) {
      // do nothing
    }

    return false;
  }
}