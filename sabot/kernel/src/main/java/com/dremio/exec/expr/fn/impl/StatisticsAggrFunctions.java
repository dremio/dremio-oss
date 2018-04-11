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

/*
 * This class is automatically generated from AggrTypeFunctions2.tdd using FreeMarker.
 */

package com.dremio.exec.expr.fn.impl;


import java.io.IOException;

import javax.inject.Inject;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.ObjectHolder;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;

import io.netty.buffer.ArrowBuf;

@SuppressWarnings("unused")
public class StatisticsAggrFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatisticsAggrFunctions.class);

  /**
   * The size of the array used in hll will be of size 2^x/6
   * The accuracy is 1.0/sqrt(2^x)
   * Value of 12 is the highest value where the array is less than 1KB; it still gives error < 3%
   */
  public static final int HLL_ACCURACY = 12;

  @FunctionTemplate(name = "statcount", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class StatCount implements AggrFunction {
    @Param
    FieldReader in;
    @Workspace
    BigIntHolder count;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      count.value++;
    }

    @Override
    public void output() {
      out.isSet = 1;
      out.value = count.value;
    }

    @Override
    public void reset() {
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "nonnullstatcount", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NonNullStatCount implements AggrFunction {
    @Param FieldReader in;
    @Workspace BigIntHolder count;
    @Output NullableBigIntHolder out;

    @Override
    public void setup() {
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet()) {
        count.value++;
      }
    }

    @Override
    public void output() {
      out.isSet = 1;
      out.value = count.value;
    }

    @Override
    public void reset() {
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBitNDVFunction implements AggrFunction {
    @Param
    NullableBitHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntNDVFunction implements AggrFunction {
    @Param
    NullableIntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBigIntNDVFunction implements AggrFunction {
    @Param
    NullableBigIntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat4NDVFunction implements AggrFunction {
    @Param
    NullableFloat4Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat8NDVFunction implements AggrFunction {
    @Param
    NullableFloat8Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDateNDVFunction implements AggrFunction {
    @Param
    NullableDateMilliHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeNDVFunction implements AggrFunction {
    @Param
    NullableTimeMilliHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeStampNDVFunction implements AggrFunction {
    @Param
    NullableTimeStampMilliHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarCharNDVFunction implements AggrFunction {
    @Param
    NullableVarCharHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          byte[] buf = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
            in.start, in.end, in.buffer).getBytes();
          hll.offer(buf);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarBinaryNDVFunction implements AggrFunction {
    @Param
    NullableVarBinaryHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          byte[] buf = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8
            (in.start, in.end, in.buffer).getBytes();
          hll.offer(buf);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "hll_decode", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class HllDecode implements SimpleFunction {

    @Param
    NullableVarBinaryHolder in;
    @Output
    BigIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.value = -1;

      if (in.isSet != 0) {
        byte[] din = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, din);
        try {
          out.value = com.clearspring.analytics.stream.cardinality.HyperLogLog.Builder.build(din).cardinality();
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failure evaluating hll_decode", e);
        }
      }
    }
  }

  @FunctionTemplate(name = "hll_merge", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class HllMerge implements AggrFunction {
    @Param NullableVarBinaryHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;


        if (in.isSet == 0) { // No hll structure to merge
          return;
        }
        // fall through //
        try {
          byte[] buf = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
            in.start, in.end, in.buffer).getBytes();
          com.clearspring.analytics.stream.cardinality.HyperLogLog other =
            com.clearspring.analytics.stream.cardinality.HyperLogLog.Builder.build(buf);
          hll.addAll(other);
        } catch (Exception e) {
          throw new java.lang.RuntimeException("Failed to merge HyperLogLog output", e);
        }
        work.obj = null;
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          buffer = buffer.reallocIfNeeded(ba.length);
          out.buffer = buffer;
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "ndv_merge", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NdvMerge implements AggrFunction {
    @Param NullableVarBinaryHolder in;
    @Workspace ObjectHolder work;
    @Output NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;


        if (in.isSet == 0) { // No hll structure to merge
          return;
        }
        try {
          byte[] buf = new byte[in.end - in.start];
          in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
          com.clearspring.analytics.stream.cardinality.HyperLogLog other =
            com.clearspring.analytics.stream.cardinality.HyperLogLog.Builder.build(buf);
          hll.addAll(other);
        } catch (Exception e) {
          throw new java.lang.RuntimeException("Failed to merge HyperLogLog output", e);
        }
      }
    }

    @Override
    public void output() {
      out.isSet = 1;
      if (work.obj != null) {
        out.value = ((com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj).cardinality();
      } else {
        out.value = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBitHLLFunction implements AggrFunction {
    @Param
    NullableBitHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          buffer = buffer.reallocIfNeeded(ba.length);
          out.buffer = buffer;
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntHLLFunction implements AggrFunction {
    @Param
    NullableIntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          buffer = buffer.reallocIfNeeded(ba.length);
          out.buffer = buffer;
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBigIntHLLFunction implements AggrFunction {
    @Param
    NullableBigIntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          buffer = buffer.reallocIfNeeded(ba.length);
          out.buffer = buffer;
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat4HLLFunction implements AggrFunction {
    @Param
    NullableFloat4Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          buffer = buffer.reallocIfNeeded(ba.length);
          out.buffer = buffer;
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat8HLLFunction implements AggrFunction {
    @Param
    NullableFloat8Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          buffer = buffer.reallocIfNeeded(ba.length);
          out.buffer = buffer;
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDateHLLFunction implements AggrFunction {
    @Param
    NullableDateMilliHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          buffer = buffer.reallocIfNeeded(ba.length);
          out.buffer = buffer;
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeHLLFunction implements AggrFunction {
    @Param
    NullableTimeMilliHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          buffer = buffer.reallocIfNeeded(ba.length);
          out.buffer = buffer;
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeStampHLLFunction implements AggrFunction {
    @Param
    NullableTimeStampMilliHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          buffer = buffer.reallocIfNeeded(ba.length);
          out.buffer = buffer;
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarCharHLLFunction implements AggrFunction {
    @Param
    NullableVarCharHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          byte[] buf = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
            in.start, in.end, in.buffer).getBytes();
          hll.offer(buf);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          buffer = buffer.reallocIfNeeded(ba.length);
          out.buffer = buffer;
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }


  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarBinaryHLLFunction implements AggrFunction {
    @Param
    NullableVarBinaryHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          byte[] buf = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8
            (in.start, in.end, in.buffer).getBytes();
          hll.offer(buf);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          buffer = buffer.reallocIfNeeded(ba.length);
          out.buffer = buffer;
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new java.lang.RuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(com.dremio.exec.expr.fn.impl.StatisticsAggrFunctions.HLL_ACCURACY);
    }
  }


  public static void main(String[] args) throws IOException, CardinalityMergeException {
    HyperLogLog hll = new HyperLogLog(12);
    for (int i = 0; i < 25; i++) {
      hll.offer(i);
    }
    System.out.println(hll.cardinality());
    byte[] bytes =hll.getBytes();
    HyperLogLog hll2 = new HyperLogLog(12);
    HyperLogLog other = HyperLogLog.Builder.build(bytes);
    hll2.addAll(other);
    System.out.println(hll2.cardinality());
  }
}

