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

import static com.dremio.service.sysflight.ProtobufRecordReader.allocateNewUtil;

import com.dremio.common.AutoCloseables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Stream observer implementation to read gRPC stream & relay to flight stream listener in batches
 *
 * @param <E>
 */
public class SysFlightStreamObserver<E extends Message> implements StreamObserver<E> {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(SysFlightStreamObserver.class);

  private final BufferAllocator allocator;
  private final ServerStreamListener listener;
  private final Descriptors.Descriptor descriptor;
  private final int recordBatchSize;

  private final Map<String, ValueVector> vectorMap;
  private final VectorSchemaRoot root;

  private final AtomicInteger totalMsgCount = new AtomicInteger(0);
  private final List<E> batch = new ArrayList<>();

  public SysFlightStreamObserver(
      BufferAllocator allocator,
      ServerStreamListener listener,
      Descriptors.Descriptor descriptor,
      int recordBatchSize) {
    LOGGER.info("Requesting to fetch system table data for {}", descriptor.getFullName());
    this.allocator =
        allocator.newChildAllocator("sys-flight-stream-observer-allocator", 0, Long.MAX_VALUE);
    this.listener = listener;
    this.descriptor = descriptor;
    this.recordBatchSize = recordBatchSize;

    vectorMap = ProtobufRecordReader.setup(descriptor, allocator);
    for (Map.Entry<String, ValueVector> vectorEntry : vectorMap.entrySet()) {
      AllocationHelper.allocateNew(vectorEntry.getValue(), recordBatchSize);
    }
    root = VectorSchemaRoot.create(ProtobufRecordReader.getSchema(descriptor), allocator);
    listener.start(root);
  }

  @Override
  public void onNext(E e) {
    totalMsgCount.getAndAdd(1);
    LOGGER.debug("Received data {}:{} for {}", totalMsgCount, e, descriptor.getFullName());
    batch.add(e);
    if (batch.size() == recordBatchSize) {
      ProtobufRecordReader.handleBatch(root, vectorMap, allocator, listener, batch);
      batch.clear();
      allocateNewUtil(vectorMap, recordBatchSize);
    }
  }

  @Override
  public void onError(Throwable th) {
    try {
      LOGGER.error("Exception fetching system table data for {}", descriptor.getFullName(), th);
      close(th);
    } catch (Exception ex) {
      LOGGER.error(
          "Exception closing SysFlightStreamObserver:onError for {}", descriptor.getFullName(), ex);
    }
  }

  @Override
  public void onCompleted() {
    try {
      LOGGER.debug(
          "Completed receiving data for {}, total received {}",
          descriptor.getFullName(),
          totalMsgCount);
      close(null);
    } catch (Throwable ex) {
      listener.error(ex);
      LOGGER.error(
          "Exception closing SysFlightStreamObserver:onCompleted for {}",
          descriptor.getFullName(),
          ex);
    }
  }

  private void close(Throwable ex) throws Exception {
    try {
      if (ex == null) {
        if (batch.size() > 0) {
          ProtobufRecordReader.handleBatch(root, vectorMap, allocator, listener, batch);
          batch.clear();
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
