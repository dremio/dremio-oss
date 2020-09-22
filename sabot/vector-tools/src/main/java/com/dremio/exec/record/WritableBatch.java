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
package com.dremio.exec.record;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;

import com.dremio.common.util.Numbers;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.RecordBatchDef;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.netty.buffer.NettyArrowBuf;

/**
 * A specialized version of record batch that can moves out buffers and preps them for writing.
 */
public class WritableBatch implements AutoCloseable {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WritableBatch.class);

  private final RecordBatchDef def;
  private final ArrowBuf[] buffers;
  private boolean cleared = false;

  private WritableBatch(RecordBatchDef def, List<ArrowBuf> buffers) {
    this.def = def;
    this.buffers = buffers.toArray(new ArrowBuf[buffers.size()]);
  }

  private WritableBatch(RecordBatchDef def, ArrowBuf[] buffers) {
    super();
    this.def = def;
    this.buffers = buffers;
  }

  public WritableBatch transfer(BufferAllocator allocator) {
    List<ArrowBuf> newBuffers = Lists.newArrayList();
    for (ArrowBuf buf : buffers) {
      long writerIndex = buf.writerIndex();
      ArrowBuf newBuf = buf.getReferenceManager().transferOwnership(buf, allocator).getTransferredBuffer();
      newBuf.writerIndex(writerIndex);
      newBuffers.add(newBuf);
    }
    clear();
    return new WritableBatch(def, newBuffers);
  }

  public RecordBatchDef getDef() {
    return def;
  }

  public NettyArrowBuf[] getBuffers() {
    NettyArrowBuf [] nettyBuffers = new NettyArrowBuf[buffers.length];
    return Arrays.stream(buffers).map(buf -> buf
      .asNettyBuffer()).collect(Collectors.toList()).toArray(nettyBuffers);
  }

  /**
   * @return total length that needs to be allocated to deserialize this batch
   */
  public int getLength() {
    long length = 0;
    final List<SerializedField> fields = def.getFieldList();
    for (SerializedField field : fields) {
      final int dataLength = field.getBufferLength();
      length += Numbers.nextPowerOfTwo(dataLength);
    }
    return (int) length;
  }

  public void reconstructContainer(BufferAllocator allocator, VectorContainer container) {
    Preconditions.checkState(!cleared,
        "Attempted to reconstruct a container from a WritableBatch after it had been cleared");
    if (buffers.length > 0) { /* If we have ArrowBuf's associated with value vectors */
      int len = 0;
      for (ArrowBuf b : buffers) {
        len += b.capacity();
      }

      ArrowBuf newBuf = allocator.buffer(len);
      try {
        /* Copy data from each buffer into the compound buffer */
        int offset = 0;
        for (ArrowBuf buf : buffers) {
          newBuf.setBytes(offset, buf);
          offset += buf.capacity();
          buf.release();
        }

        List<SerializedField> fields = def.getFieldList();

        int bufferOffset = 0;

        /*
         * For each value vector slice up the appropriate size from the compound buffer and load it into the value vector
         */
        int vectorIndex = 0;

        for (VectorWrapper<?> vv : container) {
          SerializedField fmd = fields.get(vectorIndex);
          ValueVector v = vv.getValueVector();
          ArrowBuf bb = newBuf.slice(bufferOffset, fmd.getBufferLength());
//        v.load(fmd, cbb.slice(bufferOffset, fmd.getBufferLength()));
          TypeHelper.load(v, fmd, bb);
          vectorIndex++;
          bufferOffset += fmd.getBufferLength();
        }
      } finally {
        // Any vectors that loaded material from newBuf slices above will retain those.
        newBuf.release(1);
      }
    }

    SelectionVectorMode svMode;
    if (def.hasCarriesTwoByteSelectionVector() && def.getCarriesTwoByteSelectionVector()) {
      svMode = SelectionVectorMode.TWO_BYTE;
    } else {
      svMode = SelectionVectorMode.NONE;
    }
    container.buildSchema(svMode);

    /* Set the record count in the value vector */
    for (VectorWrapper<?> v : container) {

      v.getValueVector().setValueCount(def.getRecordCount());
    }
  }

  public void clear() {
    if(cleared) {
      return;
    }
    for (ArrowBuf buf : buffers) {
      buf.release();
    }
    cleared = true;
  }

  public static WritableBatch getBatchNoHVWrap(int recordCount, Iterable<VectorWrapper<?>> vws, boolean isSV2) {
    List<ValueVector> vectors = Lists.newArrayList();
    for (VectorWrapper<?> vw : vws) {
      Preconditions.checkArgument(!vw.isHyper());
      vectors.add(vw.getValueVector());
    }
    return getBatchNoHV(recordCount, vectors, isSV2);
  }

  public static WritableBatch getBatchNoHV(int recordCount, Iterable<ValueVector> vectors, boolean isSV2) {
    List<ArrowBuf> buffers = Lists.newArrayList();
    List<SerializedField> metadata = Lists.newArrayList();

    for (ValueVector vv : vectors) {
      metadata.add(TypeHelper.getMetadata(vv));

      // don't try to get the buffers if we don't have any records. It is possible the buffers are dead buffers.
      if (recordCount == 0) {
        vv.clear();
        continue;
      }

      for (ArrowBuf b : vv.getBuffers(true)) {
        buffers.add(b);
      }
      // remove vv access to buffers.
      vv.clear();
    }

    RecordBatchDef batchDef = RecordBatchDef.newBuilder().addAllField(metadata).setRecordCount(recordCount)
        .setCarriesTwoByteSelectionVector(isSV2).build();
    WritableBatch b = new WritableBatch(batchDef, buffers);
    return b;
  }

  private static void validate(WritableBatch b) {
    for (SerializedField f : b.getDef().getFieldList()) {
      if (!f.hasNamePart()) {
        throw new RuntimeException("Field is missing name");
      }
      if (f.getNamePart().getName().equals("")) {
        throw new RuntimeException("Field name is empty");
      }
    }
  }

  public static WritableBatch get(VectorAccessible batch) {
    if (batch.getSchema() != null && batch.getSchema().getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE) {
      throw new UnsupportedOperationException("Only batches without hyper selections vectors are writable.");
    }

    boolean sv2 = (batch.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE);
    return getBatchNoHVWrap(batch.getRecordCount(), batch, sv2);
  }

  public void retainBuffers(final int increment) {
    for (final ArrowBuf buf : buffers) {
      buf.retain(increment);
    }
  }

  @Override
  public void close() {
    // close may be closed more than once, clear() will make sure it will not release the buffers more than once
    clear();
  }

}
