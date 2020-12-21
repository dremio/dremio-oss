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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.exec.proto.ExecRPC.FragmentRecordBatch;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;

public class FragmentWritableBatch{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentWritableBatch.class);

  private final ByteBuf[] buffers;
  private final FragmentRecordBatch header;
  private final int recordCount;

  public static FragmentWritableBatch create(
    final QueryId queryId,
    final int sendMajorFragmentId,
    final int sendMinorFragmentId,
    final int receiveMajorFragmentId,
    final VectorAccessible batch,
    final int receiveMinorFragmentId) {

    ArrowRecordBatch recordBatch = getArrowRecordBatch(batch);

    return new FragmentWritableBatch(
      queryId,
      sendMajorFragmentId,
      sendMinorFragmentId,
      receiveMajorFragmentId,
      recordBatch,
      receiveMinorFragmentId
    );
  }

  public static ArrowRecordBatch getArrowRecordBatch(final VectorAccessible batch) {
    VectorSchemaRoot root = getVectorSchemaRoot(batch);
    VectorUnloader unloader = new VectorUnloader(root, false, false);
    ArrowRecordBatch recordBatch = unloader.getRecordBatch();
    return recordBatch;
  }

  public static VectorSchemaRoot getVectorSchemaRoot(final VectorAccessible batch) {
    List<FieldVector> fieldVectors = FluentIterable.from(batch)
        .transform(new Function<VectorWrapper<?>, FieldVector>() {
          @Override
          public FieldVector apply(VectorWrapper<?> vectorWrapper) {
            return (FieldVector)vectorWrapper.getValueVector();
          }
        }).toList();
    int rowCount = batch.getRecordCount();
    List<Field> fields = batch.getSchema().getFields();
    VectorSchemaRoot root = new VectorSchemaRoot(fields , fieldVectors, rowCount);
    return root;
  }

  public static ArrowBuf[] getBuffers(VectorAccessible batch) {
    return FluentIterable.from(batch)
      .transformAndConcat(new Function<VectorWrapper<?>, Iterable<ArrowBuf>>() {
        @Override
        public Iterable<ArrowBuf> apply(VectorWrapper<?> vectorWrapper) {
          return Arrays.asList(vectorWrapper.getValueVector().getBuffers(true));
        }
      }).toList().toArray(new ArrowBuf[0]);
  }

  public FragmentWritableBatch(
      final QueryId queryId,
      final int sendMajorFragmentId,
      final int sendMinorFragmentId,
      final int receiveMajorFragmentId,
      ArrowRecordBatch recordBatch,
      final int... receiveMinorFragmentId){
    this.buffers = recordBatch.getBuffers().stream().map(buf -> NettyArrowBuf.unwrapBuffer(buf)).collect
      (Collectors.toList()).toArray(new ByteBuf[0]);
    this.recordCount = recordBatch.getLength();
    FlatBufferBuilder fbbuilder = new FlatBufferBuilder();
    fbbuilder.finish(recordBatch.writeTo(fbbuilder));
    ByteBuffer arrowRecordBatch = fbbuilder.dataBuffer();
    final FragmentRecordBatch.Builder builder = FragmentRecordBatch.newBuilder()
        .setArrowRecordBatch(ByteString.copyFrom(arrowRecordBatch))
        .setQueryId(queryId)
        .setReceivingMajorFragmentId(receiveMajorFragmentId)
        .setSendingMajorFragmentId(sendMajorFragmentId)
        .setSendingMinorFragmentId(sendMinorFragmentId);

    for(final int i : receiveMinorFragmentId){
      builder.addReceivingMinorFragmentId(i);
    }

    this.header = builder.build();
  }

  public ByteBuf[] getBuffers(){
    return buffers;
  }

  public long getByteCount() {
    long n = 0;
    for (final ByteBuf buf : buffers) {
      n += buf.readableBytes();
    }
    return n;
  }

  public FragmentRecordBatch getHeader() {
    return header;

  }

  public int getRecordCount() {
    return recordCount;
  }
}
