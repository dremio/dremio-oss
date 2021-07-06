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
package com.dremio.exec.store.easy.arrow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.cache.VectorAccessibleFlatBufSerializable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.io.FSOutputStream;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;
import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.util.internal.PlatformDependent;

/**
 * {@link RecordWriter} implementation for Arrow format files using flatbuffer serialization
 *
 * The format for the file is as follows:
 *    <MAGIC_STRING>
 *    Array<SerializedArrowRecordBatch>
 *    <Footer>
 *    <MAGIC_STRING>
 *
 * SerializedArrowRecordBatch: described in {@link VectorAccessibleFlatBufSerializable}
 *
 * Footer:
 *    <4byte schema len>
 *    <serialized schema>
 *    <4 byte offset array len>
 *    Array<8 byte batch offsets>
 *    Array<4 byte batch counts>
 *    <8 byte footer start offset></8>
 */
public class ArrowFlatBufRecordWriter implements RecordWriter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowFlatBufRecordWriter.class);
  private static final String MAGIC_STRING = "DREMARROWFLATBUF";

  private final OperatorContext context;
  private FSOutputStream outputStream;

  private VectorAccessible incoming;
  private BatchSchema incomingSchema;
  private OutputEntryListener outputEntryListener;
  private WriteStatsListener writeStatsListener;

  private long recordCount;

  private List<Long> batchOffsets = new ArrayList<>();
  private final List<Integer> batchSizes = new ArrayList<>();
  private boolean isClosed;

  public ArrowFlatBufRecordWriter(OperatorContext context, FSOutputStream outputStream) {
    this.context = context;
    this.outputStream = outputStream;
  }

  @Override
  public void setup(VectorAccessible incoming, OutputEntryListener listener, WriteStatsListener statsListener) throws IOException {
    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.NONE, "SelectionVector remover is not supported.");

    this.incoming = incoming;
    this.incomingSchema = incoming.getSchema();
    this.outputEntryListener = listener;
    this.writeStatsListener = statsListener;

    // write magic word bytes
    outputStream.write(MAGIC_STRING.getBytes());
  }

  @Override
  public void startPartition(WritePartition partition) throws Exception {
    if(!partition.isSinglePartition()){
      throw UserException.dataWriteError().message("ArrowFlatBufRecordWriter doesn't support data partitioning.").build(logger);
    }
  }

  @Override
  public int writeBatch(int offset, int length) throws IOException {
    if(offset != 0 || length != incoming.getRecordCount()){
      throw UserException.dataWriteError().message("You cannot partition data written in Arrow format.").build(logger);
    }
    final int recordCount = incoming.getRecordCount();
    if (recordCount == 0) {
      return 0;
    }
    final long startOffset = outputStream.getPosition();


    final VectorAccessibleFlatBufSerializable serializable = new VectorAccessibleFlatBufSerializable(incoming, null);
    serializable.writeToStream(outputStream);

    this.recordCount += recordCount;
    writeStatsListener.bytesWritten(serializable.getBytesWritten());

    batchOffsets.add(startOffset);
    batchSizes.add(length);

    return recordCount;
  }

  @Override
  public void abort() throws IOException {}

  @Override
  public void close() throws Exception {
    if (!isClosed) {
      Preconditions.checkArgument(outputStream != null);
      // Save the footer starting offset
      final long footerStartOffset = outputStream.getPosition();

      // write the footer
      // serialize schema
      FlatBufferBuilder builder = new FlatBufferBuilder();
      builder.finish(incomingSchema.serialize(builder));
      ByteBuffer schema = builder.dataBuffer();
      int schemaLen = schema.remaining();

      // footer size
      int footersize = Integer.BYTES + schemaLen              // schema
          + Integer.BYTES + batchOffsets.size() * Long.BYTES  // batch offsets
          + batchSizes.size() * Integer.BYTES
          + Long.BYTES;                                       // footer start offset
      byte[] footer = new byte[footersize];

      int index = 0;
      // write schema
      PlatformDependent.putInt(footer, index, schemaLen);
      index += Integer.BYTES;
      schema.get(footer, index, schemaLen);
      index += schemaLen;

      // write batchOffsets
      PlatformDependent.putInt(footer, index, batchOffsets.size());
      index += Integer.BYTES;
      for (long offset: batchOffsets) {
        PlatformDependent.putLong(footer, index, offset);
        index += Long.BYTES;
      }

      // write batch size information
      for (int count: batchSizes) {
        PlatformDependent.putInt(footer, index, count);
        index += Integer.BYTES;
      }

      // write footerStartOffset
      PlatformDependent.putLong(footer, index, footerStartOffset);
      index += Long.BYTES;
      Preconditions.checkArgument(index == footer.length);

      outputStream.write(footer);

      outputStream.write(MAGIC_STRING.getBytes());

      final long fileSize = outputStream.getPosition();

      outputEntryListener.recordsWritten(recordCount, fileSize, null, null, null, null, null);
      isClosed = true;
    }
  }

}
