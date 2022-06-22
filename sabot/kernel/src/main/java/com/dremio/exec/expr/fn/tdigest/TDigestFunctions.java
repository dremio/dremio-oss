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

package com.dremio.exec.expr.fn.tdigest;


import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.ObjectHolder;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.options.OptionResolver;

@SuppressWarnings({"deprecation"})
public class TDigestFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TDigestFunctions.class);

  /**
   * Computes the tdigest for a column of doubles
   */
  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat8TDigestSampleFunction implements AggrFunction {
    @Param
    private NullableFloat8Holder in;
    @Param
    private NullableBitHolder sample;
    @Workspace
    private ObjectHolder digest;
    @Output
    private NullableVarBinaryHolder out;
    @Inject
    private ArrowBuf buffer;
    @Workspace
    IntHolder compression;
    @Inject
    OptionResolver options;

    public void setup() {
      digest = new ObjectHolder();
      compression.value = (int) options.getOption(com.dremio.exec.ExecConstants.TDIGEST_COMPRESSION);
      digest.obj = new com.tdunning.math.stats.MergingDigest(compression.value);
    }

    @Override
    public void add() {
      if (in.isSet == 1 && sample.isSet == 1 && sample.value == 1) {
        ((com.tdunning.math.stats.TDigest) digest.obj).add(in.value);
      }
    }

    @Override
    public void output() {
      com.tdunning.math.stats.TDigest tdigest = ((com.tdunning.math.stats.TDigest) digest.obj);
      int size = tdigest.smallByteSize();
      buffer = buffer.reallocIfNeeded(size);
      tdigest.asSmallBytes(buffer.nioBuffer(0, size));
      out.isSet = 1;
      out.start = 0;
      out.buffer = buffer;
      out.end = size;
    }

    @Override
    public void reset() {
      digest.obj = com.tdunning.math.stats.TDigest.createMergingDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDateTDigestSampleFunction implements AggrFunction {
    @Param
    private NullableDateMilliHolder in;
    @Param
    private NullableBitHolder sample;
    @Workspace
    private ObjectHolder digest;
    @Output
    private NullableVarBinaryHolder out;
    @Inject
    private ArrowBuf buffer;
    @Workspace
    IntHolder compression;
    @Inject
    OptionResolver options;

    public void setup() {
      digest = new ObjectHolder();
      compression.value = (int) options.getOption(com.dremio.exec.ExecConstants.TDIGEST_COMPRESSION);
      digest.obj = new com.tdunning.math.stats.MergingDigest(compression.value);
    }

    @Override
    public void add() {
      if (in.isSet == 1 && sample.isSet == 1 && sample.value == 1) {
        ((com.tdunning.math.stats.TDigest) digest.obj).add(in.value);
      }
    }

    @Override
    public void output() {
      com.tdunning.math.stats.TDigest tdigest = ((com.tdunning.math.stats.TDigest) digest.obj);
      int size = tdigest.smallByteSize();
      buffer = buffer.reallocIfNeeded(size);
      tdigest.asSmallBytes(buffer.nioBuffer(0, size));
      out.isSet = 1;
      out.start = 0;
      out.buffer = buffer;
      out.end = size;
    }

    @Override
    public void reset() {
      digest.obj = com.tdunning.math.stats.TDigest.createMergingDigest(compression.value);
    }
  }


  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeTDigestSampleFunction implements AggrFunction {
    @Param
    private NullableTimeMilliHolder in;
    @Param
    private NullableBitHolder sample;
    @Workspace
    private ObjectHolder digest;
    @Output
    private NullableVarBinaryHolder out;
    @Inject
    private ArrowBuf buffer;
    @Workspace
    IntHolder compression;
    @Inject
    OptionResolver options;

    public void setup() {
      digest = new ObjectHolder();
      compression.value = (int) options.getOption(com.dremio.exec.ExecConstants.TDIGEST_COMPRESSION);
      digest.obj = new com.tdunning.math.stats.MergingDigest(compression.value);
    }

    @Override
    public void add() {
      if (in.isSet == 1 && sample.isSet == 1 && sample.value == 1) {
        ((com.tdunning.math.stats.TDigest) digest.obj).add(in.value);
      }
    }

    @Override
    public void output() {
      com.tdunning.math.stats.TDigest tdigest = ((com.tdunning.math.stats.TDigest) digest.obj);
      int size = tdigest.smallByteSize();
      buffer = buffer.reallocIfNeeded(size);
      tdigest.asSmallBytes(buffer.nioBuffer(0, size));
      out.isSet = 1;
      out.start = 0;
      out.buffer = buffer;
      out.end = size;
    }

    @Override
    public void reset() {
      digest.obj = com.tdunning.math.stats.TDigest.createMergingDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeStampTDigestSampleFunction implements AggrFunction {
    @Param
    private NullableTimeStampMilliHolder in;
    @Param
    private NullableBitHolder sample;
    @Workspace
    private ObjectHolder digest;
    @Output
    private NullableVarBinaryHolder out;
    @Inject
    private ArrowBuf buffer;
    @Workspace
    IntHolder compression;
    @Inject
    OptionResolver options;

    public void setup() {
      digest = new ObjectHolder();
      compression.value = (int) options.getOption(com.dremio.exec.ExecConstants.TDIGEST_COMPRESSION);
      digest.obj = new com.tdunning.math.stats.MergingDigest(compression.value);
    }

    @Override
    public void add() {
      if (in.isSet == 1 && sample.isSet == 1 && sample.value == 1) {
        ((com.tdunning.math.stats.TDigest) digest.obj).add(in.value);
      }
    }

    @Override
    public void output() {
      com.tdunning.math.stats.TDigest tdigest = ((com.tdunning.math.stats.TDigest) digest.obj);
      int size = tdigest.smallByteSize();
      buffer = buffer.reallocIfNeeded(size);
      tdigest.asSmallBytes(buffer.nioBuffer(0, size));
      out.isSet = 1;
      out.start = 0;
      out.buffer = buffer;
      out.end = size;
    }

    @Override
    public void reset() {
      digest.obj = com.tdunning.math.stats.TDigest.createMergingDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBitTDigestSampleFunction implements AggrFunction {
    @Param
    private NullableBitHolder in;
    @Param
    private NullableBitHolder sample;
    @Workspace
    private ObjectHolder digest;
    @Output
    private NullableVarBinaryHolder out;
    @Inject
    private ArrowBuf buffer;
    @Workspace
    IntHolder compression;
    @Inject
    OptionResolver options;

    public void setup() {
      digest = new ObjectHolder();
      compression.value = (int) options.getOption(com.dremio.exec.ExecConstants.TDIGEST_COMPRESSION);
      digest.obj = new com.tdunning.math.stats.MergingDigest(compression.value);
    }

    @Override
    public void add() {
      if (in.isSet == 1 && sample.isSet == 1 && sample.value == 1) {
        ((com.tdunning.math.stats.TDigest) digest.obj).add(in.value);
      }
    }

    @Override
    public void output() {
      com.tdunning.math.stats.TDigest tdigest = ((com.tdunning.math.stats.TDigest) digest.obj);
      int size = tdigest.smallByteSize();
      buffer = buffer.reallocIfNeeded(size);
      tdigest.asSmallBytes(buffer.nioBuffer(0, size));
      out.isSet = 1;
      out.start = 0;
      out.buffer = buffer;
      out.end = size;
    }

    @Override
    public void reset() {
      digest.obj = com.tdunning.math.stats.TDigest.createMergingDigest(compression.value);
    }
  }

  /**
   * Merges the tdigest produced by the tdigest functions to produce a new tdigest
   */
  @FunctionTemplate(name = "tdigest_merge", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarBinaryTDigestMerge implements AggrFunction {
    @Param
    private NullableVarBinaryHolder in;
    @Workspace
    private ObjectHolder digest;
    @Output
    private NullableVarBinaryHolder out;
    @Inject
    private ArrowBuf buffer;
    @Workspace
    IntHolder compression;
    @Inject
    OptionResolver options;

    public void setup() {
      digest = new ObjectHolder();
      compression.value = (int) options.getOption(com.dremio.exec.ExecConstants.TDIGEST_COMPRESSION);
      digest.obj = new com.tdunning.math.stats.MergingDigest(compression.value);
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        com.tdunning.math.stats.TDigest tDigest = com.tdunning.math.stats.MergingDigest.fromBytes(in.buffer.nioBuffer(in.start, in.end - in.start));
        ((com.tdunning.math.stats.TDigest) digest.obj).add(tDigest);
      }
    }

    @Override
    public void output() {
      com.tdunning.math.stats.TDigest tdigest = ((com.tdunning.math.stats.TDigest) digest.obj);
      int size = tdigest.smallByteSize();
      buffer = buffer.reallocIfNeeded(size);
      tdigest.asSmallBytes(buffer.nioBuffer(0, size));
      out.isSet = 1;
      out.start = 0;
      out.buffer = buffer;
      out.end = size;
    }

    @Override
    public void reset() {
      digest.obj = com.tdunning.math.stats.TDigest.createMergingDigest(compression.value);
    }
  }


}
