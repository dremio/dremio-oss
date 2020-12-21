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

package com.dremio.exec.expr.fn.hll;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalYearHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.ObjectHolder;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;

@SuppressWarnings({"deprecation"})
public class StatisticsAggrFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatisticsAggrFunctions.class);

  /**
   * The size of the array used in hll will be of size 2^x/6
   * The accuracy is 1.0/sqrt(2^x)
   * Value of 12 is the highest value where the array is less than 1KB; it still gives error < 3%
   */
  public static final int HLL_ACCURACY = 12;
  public static final int HLL_BYTES = (int) Math.ceil(Math.pow(2, HLL_ACCURACY)/6.0) * 4;

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
      out.value = com.dremio.exec.expr.fn.hll.HLLAccum.getEstimate(in.buffer, in.start, in.end);
    }
  }


  @FunctionTemplate(name = "hll_merge", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class HllMerge implements AggrFunction {
    @Param NullableVarBinaryHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
        work = com.dremio.exec.expr.fn.hll.UnionAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.UnionAccum) work.obj).addHll(in.buffer, in.start, in.end);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.UnionAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.UnionAccum) work.obj).reset();
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
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addInt(in.value);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
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
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addInt(in.value);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
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
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
        work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addLong(in.value);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
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
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
        work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addFloat(in.value);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
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
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
        work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addDouble(in.value);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
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
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
        work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addLong(in.value);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
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
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
        work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addDouble(in.value);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
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
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
        work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addLong(in.value);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
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
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
        work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addBytes(in.buffer, in.start, in.end);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
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
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
        work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addBytes(in.buffer, in.start, in.end);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntDayHLLFunction implements AggrFunction {
    @Param
    NullableIntervalDayHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
        work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addInt(in.milliseconds);;
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntYearHLLFunction implements AggrFunction {
    @Param
    NullableIntervalYearHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
        work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addInt(in.value);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
    }
  }

  @FunctionTemplate(name = "hll_v2", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimalHLLFunction implements AggrFunction {
    @Param
    NullableDecimalHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject org.apache.arrow.memory.BufferManager manager;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
      work = com.dremio.exec.expr.fn.hll.HLLAccum.create(work, manager);
    }

    @Override
    public void add() {
      if(in.isSet == 1) {
        ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).addBytes(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start),
          org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start) + org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      }
    }

    @Override
    public void output() {
      byte[] ba = ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).getOutputBytes();
      buffer = buffer.reallocIfNeeded(ba.length);
      out.buffer = buffer;
      out.start = 0;
      out.end = ba.length;
      out.buffer.setBytes(0, ba);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      ((com.dremio.exec.expr.fn.hll.HLLAccum) work.obj).reset();
    }
  }
}

