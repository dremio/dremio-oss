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
package com.dremio.service.sysflight;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.dremio.common.AutoCloseables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import io.grpc.stub.StreamObserver;

/**
 * Stream observer implementation to read gRPC stream & relay to flight stream listener in batches
 * @param <E>
 */
public class SysFlightStreamObserver<E extends Message> implements StreamObserver<E> {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SysFlightStreamObserver.class);

  private final BufferAllocator allocator;
  private final ServerStreamListener listener;
  private final int recordBatchSize;

  private final Map<String, ValueVector> vectorMap;
  private final VectorSchemaRoot root;
  private final AtomicInteger count = new AtomicInteger(0);

  public SysFlightStreamObserver(BufferAllocator allocator,
                                 ServerStreamListener listener,
                                 Descriptors.Descriptor descriptor,
                                 int recordBatchSize) {
    this.allocator = allocator.newChildAllocator("sys-flight-stream-observer-allocator", 0, Long.MAX_VALUE);
    this.listener = listener;
    this.recordBatchSize = recordBatchSize;

    vectorMap = ProtobufRecordReader.setup(descriptor, allocator);
    root = VectorSchemaRoot.create(ProtobufRecordReader.getSchema(descriptor), allocator);
    listener.start(root);
  }

  @Override
  public void onNext(E e) {
    ProtobufRecordReader.handleMessage(e, root, vectorMap, allocator, listener, count, recordBatchSize);
  }

  @Override
  public void onError(Throwable th) {
    try {
      close(th);
    } catch (Exception ex) {
      LOGGER.error("Exception while closing SysFlightStreamObserver: ", ex);
    }
  }

  @Override
  public void onCompleted() {
    try {
      close(null);
    } catch (Throwable ex) {
      listener.error(ex);
      LOGGER.error("Exception while closing SysFlightStreamObserver: ", ex);
    }
  }

  private void close(Throwable ex) throws Exception {
    try {
      if (ex == null) {
        if(count.get() > 0) {
          ProtobufRecordReader.stream(vectorMap, count.get(), root, allocator, listener, true);
          count.set(0);
        }
        listener.completed();
      } else {
        listener.error(ex);
      }
    } finally {
      AutoCloseables.close(AutoCloseables.all(vectorMap.values()), root, allocator);
    }
  }
}
