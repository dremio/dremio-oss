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
package com.dremio.exec.expr.fn.impl.conv;

import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

public class ConvertFromImpalaTimestamp {


  @FunctionTemplate(name = "convert_fromTIMESTAMP_IMPALA_LOCALTIMEZONE", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class ImpalaTimestampConvertFromWithLocalTimezone implements SimpleFunction {

    @Param VarBinaryHolder in;
    @Output TimeStampMilliHolder out;


    @Override
    public void setup() { }

    @Override
    public void eval() {
      com.dremio.exec.util.ByteBufUtil.checkBufferLength(in.buffer, in.start, in.end, 12);

      in.buffer.readerIndex(in.start);
      long nanosOfDay = in.buffer.readLong();
      int julianDay = in.buffer.readInt();
      long dateTime = (julianDay - com.dremio.exec.store.parquet.ParquetReaderUtility.JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) *
          org.joda.time.DateTimeConstants.MILLIS_PER_DAY + (nanosOfDay / com.dremio.exec.store.parquet.ParquetReaderUtility.NanoTimeUtils.NANOS_PER_MILLISECOND);
      out.value = com.dremio.common.util.DateTimes.toMillis(new org.joda.time.LocalDateTime(dateTime, org.joda.time.chrono.JulianChronology.getInstance()));
    }
  }

  @FunctionTemplate(name = "convert_fromTIMESTAMP_IMPALA", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class ImpalaTimestampConvertFrom implements SimpleFunction {

    @Param VarBinaryHolder in;
    @Output TimeStampMilliHolder out;


    @Override
    public void setup() { }

    @Override
    public void eval() {
      com.dremio.exec.util.ByteBufUtil.checkBufferLength(in.buffer, in.start, in.end, 12);

      in.buffer.readerIndex(in.start);
      long nanosOfDay = in.buffer.readLong();
      int julianDay = in.buffer.readInt();
      out.value = (julianDay - com.dremio.exec.store.parquet.ParquetReaderUtility.JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) *
          org.joda.time.DateTimeConstants.MILLIS_PER_DAY + (nanosOfDay / com.dremio.exec.store.parquet.ParquetReaderUtility.NanoTimeUtils.NANOS_PER_MILLISECOND);
    }
  }
}
