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
package org.apache.arrow.vector;

import java.math.BigDecimal;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.TimeMilliHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.util.DecimalUtility;

import com.google.common.base.Charsets;


public class ValueHolderHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueHolderHelper.class);

  public static IntHolder getIntHolder(int value) {
    IntHolder holder = new IntHolder();
    holder.value = value;

    return holder;
  }

  public static BigIntHolder getBigIntHolder(long value) {
    BigIntHolder holder = new BigIntHolder();
    holder.value = value;

    return holder;
  }

  public static Float4Holder getFloat4Holder(float value) {
    Float4Holder holder = new Float4Holder();
    holder.value = value;

    return holder;
  }

  public static Float8Holder getFloat8Holder(double value) {
    Float8Holder holder = new Float8Holder();
    holder.value = value;

    return holder;
  }

  public static DateMilliHolder getDateMilliHolder(long value) {
    DateMilliHolder holder = new DateMilliHolder();
    holder.value = value;
    return holder;
  }

  public static TimeMilliHolder getTimeMilliHolder(int value) {
    TimeMilliHolder holder = new TimeMilliHolder();
    holder.value = value;
    return holder;
  }

  public static TimeStampMilliHolder getTimeStampMilliHolder(long value) {
    TimeStampMilliHolder holder = new TimeStampMilliHolder();
    holder.value = value;
    return holder;
  }

  public static BitHolder getBitHolder(int value) {
    BitHolder holder = new BitHolder();
    holder.value = value;

    return holder;
  }

  public static NullableBigIntHolder getNullableBigIntHolder(long value) {
    NullableBigIntHolder holder = new NullableBigIntHolder();
    holder.value = value;
    holder.isSet = 1;

    return holder;
  }

  public static NullableFloat4Holder getNullableFloat4Holder(float value) {
    NullableFloat4Holder holder = new NullableFloat4Holder();
    holder.value = value;
    holder.isSet = 1;

    return holder;
  }

  public static NullableFloat8Holder getNullableFloat8Holder(double value) {
    NullableFloat8Holder holder = new NullableFloat8Holder();
    holder.value = value;
    holder.isSet = 1;

    return holder;
  }

  public static NullableDateMilliHolder getNullableDateMilliHolder(long value) {
    NullableDateMilliHolder holder = new NullableDateMilliHolder();
    holder.value = value;
    holder.isSet = 1;
    return holder;
  }

  public static NullableTimeMilliHolder getNullableTimeMilliHolder(int value) {
    NullableTimeMilliHolder holder = new NullableTimeMilliHolder();
    holder.value = value;
    holder.isSet = 1;
    return holder;
  }

  public static NullableTimeStampMilliHolder getNullableTimeStampMilliHolder(long value) {
    NullableTimeStampMilliHolder holder = new NullableTimeStampMilliHolder();
    holder.value = value;
    holder.isSet = 1;
    return holder;
  }

  public static NullableBitHolder getNullableBitHolder(int value) {
    NullableBitHolder holder = new NullableBitHolder();
    holder.value = value;
    holder.isSet = 1;

    return holder;
  }

  public static NullableBitHolder getNullableBitHolder(boolean isNull, int value) {
    NullableBitHolder holder = new NullableBitHolder();
    holder.isSet = isNull? 0 : 1;
    if (! isNull) {
      holder.value = value;
    }

    return holder;
  }


  public static NullableVarCharHolder getNullableVarCharHolder(ArrowBuf buf, String s){
    NullableVarCharHolder vch = new NullableVarCharHolder();

    byte[] b = s.getBytes(Charsets.UTF_8);
    vch.start = 0;
    vch.end = b.length;
    vch.buffer = buf.reallocIfNeeded(b.length);
    vch.buffer.setBytes(0, b);
    vch.isSet = 1;
    return vch;
  }

  public static VarCharHolder getVarCharHolder(ArrowBuf buf, String s){
    VarCharHolder vch = new VarCharHolder();

    byte[] b = s.getBytes(Charsets.UTF_8);
    vch.start = 0;
    vch.end = b.length;
    vch.buffer = buf.reallocIfNeeded(b.length);
    vch.buffer.setBytes(0, b);
    return vch;
  }

  public static VarCharHolder getVarCharHolder(BufferAllocator a, String s){
    VarCharHolder vch = new VarCharHolder();

    byte[] b = s.getBytes(Charsets.UTF_8);
    vch.start = 0;
    vch.end = b.length;
    vch.buffer = a.buffer(b.length); //
    vch.buffer.setBytes(0, b);
    return vch;
  }


  public static IntervalYearHolder getIntervalYearHolder(int intervalYear) {
    IntervalYearHolder holder = new IntervalYearHolder();

    holder.value = intervalYear;
    return holder;
  }

  public static IntervalDayHolder getIntervalDayHolder(int days, int millis) {
    IntervalDayHolder dch = new IntervalDayHolder();

    dch.days = days;
    dch.milliseconds = millis;
    return dch;
  }

  public static NullableIntervalDayHolder getNullableIntervalDayHolder(int days, int millis) {
    NullableIntervalDayHolder dch = new NullableIntervalDayHolder();
    dch.isSet = 1;
    dch.days = days;
    dch.milliseconds = millis;
    return dch;
  }
  public static NullableDecimalHolder getNullableDecimalHolder(ArrowBuf buf, String value) {
    BigDecimal bd = new BigDecimal(value);
    return getNullableDecimalHolder(buf, bd, bd.precision(), bd.scale());
  }

  public static NullableDecimalHolder getNullableDecimalHolder(ArrowBuf buf, BigDecimal decimal, int precision, int scale) {
    NullableDecimalHolder holder = new NullableDecimalHolder();
    buf.reallocIfNeeded(16);
    DecimalUtility.writeBigDecimalToArrowBuf(decimal, buf, 0, DecimalVector.TYPE_WIDTH);
    holder.buffer = buf;
    holder.start = 0;
    holder.scale = scale;
    holder.precision = precision;
    holder.isSet = 1;
    return holder;
  }

}

