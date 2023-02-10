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
package com.dremio.sabot.op.join.vhash.spill.list;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;


/**
 * A structure that holds the input and output arguments for a vectorized probe
 * operation. Used to simplify passing these structures around in a clear way.
 */
public class ProbeBuffers implements AutoCloseable {

  private final BufferAllocator allocator;

  /**
   * A list of 4B offsets that describe which value each ordinal matches.
   */
  private ArrowBuf inTableMatchOrdinals4B;

  /**
   * An output list of 2B offsets that describe which items should be projected
   * from the probe side of the join.
   */
  private final ArrowBuf outProbeProjectOffsets2B;

  /**
   * An output list of 6B offsets that describe which items should be projected
   * from the build side of the join. -1 means nothing should be projected.
   */
  private final ArrowBuf outBuildProjectOffsets6B;

  /**
   * An output list of 4B offsets that describe the table ordinals for the keys
   * that should be projected from the build side of the join. Used only for
   * right join and full joins.
   */
  private final ArrowBuf outBuildProjectKeyOrdinals4B;

  /**
   * An output list of keys that should be null-ed out of the build side. To avoid
   * keeping two copies of keys in memory, at the end of the join operation we
   * copy all keys from the probe side to the build side. We then overwrite the
   * validity bits for all position where we should have skipped projection so
   * that the keys on the build side show up as nulls for outer join matches.
   */
  private final ArrowBuf outInvalidBuildKeyOffsets2B;

  public ProbeBuffers(int targetRecordsPerBatch, BufferAllocator allocator) {
    this.allocator = allocator;
    this.inTableMatchOrdinals4B = allocator.buffer(targetRecordsPerBatch * 4);
    this.outProbeProjectOffsets2B = allocator.buffer(targetRecordsPerBatch * 2);
    this.outBuildProjectOffsets6B = allocator.buffer(targetRecordsPerBatch * 6);
    this.outInvalidBuildKeyOffsets2B = allocator.buffer(targetRecordsPerBatch * 2);
    this.outBuildProjectKeyOrdinals4B = allocator.buffer(targetRecordsPerBatch * 4);
  }

  /**
   * Make sure the input buffer allows up to the requested number of records.
   * @param records
   */
  public void ensureInputCapacity(int records) {
    if (inTableMatchOrdinals4B.capacity() < records * 4){
      inTableMatchOrdinals4B.close();
      inTableMatchOrdinals4B = allocator.buffer(records * 4);
    }
  }

  public ArrowBuf getInTableMatchOrdinals4B() {
    return inTableMatchOrdinals4B;
  }

  public ArrowBuf getOutProbeProjectOffsets2B() {
    return outProbeProjectOffsets2B;
  }

  public ArrowBuf getOutBuildProjectOffsets6B() {
    return outBuildProjectOffsets6B;
  }

  public ArrowBuf getOutInvalidBuildKeyOffsets2B() {
    return outInvalidBuildKeyOffsets2B;
  }

  public ArrowBuf getOutBuildProjectKeyOrdinals4B() {
    return outBuildProjectKeyOrdinals4B;
  }

  @Override
  public void close() {
    try {
      AutoCloseables.close(inTableMatchOrdinals4B, outProbeProjectOffsets2B, outBuildProjectOffsets6B,
        outInvalidBuildKeyOffsets2B, outBuildProjectKeyOrdinals4B);
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
