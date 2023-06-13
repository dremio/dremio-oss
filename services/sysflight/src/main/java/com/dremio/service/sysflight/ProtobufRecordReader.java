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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.record.VectorContainer;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;

/**
 * Utility class for getting schema and populating the vectors automatically. Supports only primitve types.
 */
public class ProtobufRecordReader {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ProtobufRecordReader.class);

  /**
   * Builds the schema based on the field types of the gRPC message, fields should be of
   * primitive type only
   * @param desc - the descriptor of the gRPC message corresponding to which
   *             we need the schema.
   * @return a new BatchSchema
   */
  public static BatchSchema getSchema(Descriptors.Descriptor desc)  {
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();

    desc.getFields().forEach(field -> {
      if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.INT) {
        schemaBuilder.addField(CompleteType.INT.toField(field.getName()));

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.LONG) {
        schemaBuilder.addField(CompleteType.BIGINT.toField(field.getName()));

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.FLOAT) {
        schemaBuilder.addField(CompleteType.FLOAT.toField(field.getName()));

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.DOUBLE) {
        schemaBuilder.addField(CompleteType.DOUBLE.toField(field.getName()));

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.BOOLEAN) {
        schemaBuilder.addField(CompleteType.BIT.toField(field.getName()));

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING) {
        schemaBuilder.addField(CompleteType.VARCHAR.toField(field.getName()));

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && field.getMessageType().getFullName().equals("google.protobuf.Timestamp")){
        schemaBuilder.addField(CompleteType.TIMESTAMP.toField(field.getName()));
      } else {
        LOGGER.debug("{} is of type {} which is not a primitive type. ", field.getName(),field.getJavaType());
      }
    });
    BatchSchema schema = schemaBuilder.build();
    return schema;
  }

  public static Map<String, ValueVector> setup(Descriptors.Descriptor descriptor, BufferAllocator allocator) {
    LOGGER.debug("Setting up ValueVectors");
    //Using LinkedHashMap important for maintaining order of vectors between the schema generated from BatchData and the gRPC message descriptor.
    Map<String, ValueVector> vectorMap = new LinkedHashMap<>();
    descriptor.getFields().forEach(field -> {

      if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.INT) {
        IntVector e = new IntVector(CompleteType.INT.toField(field.getName()), allocator);
        vectorMap.put(field.getName(), e);

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.LONG) {
        BigIntVector e = new BigIntVector(CompleteType.BIGINT.toField(field.getName()), allocator);
        vectorMap.put(field.getName(), e);

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.FLOAT) {
        Float4Vector e = new Float4Vector(CompleteType.BIGINT.toField(field.getName()), allocator);
        vectorMap.put(field.getName(), e);

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.DOUBLE) {
        Float8Vector e = new Float8Vector(CompleteType.BIGINT.toField(field.getName()), allocator);
        vectorMap.put(field.getName(), e);

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.BOOLEAN) {
        BitVector e = new BitVector(CompleteType.BIT.toField(field.getName()), allocator);
        vectorMap.put(field.getName(), e);

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING) {
        VarCharVector e = new VarCharVector(CompleteType.VARCHAR.toField(field.getName()), allocator);
        vectorMap.put(field.getName(), e);

      } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && field.getMessageType().getFullName().equals("google.protobuf.Timestamp")){
        TimeStampVector e = new TimeStampMilliVector(CompleteType.TIMESTAMP.toField(field.getName()), allocator);
        vectorMap.put(field.getName(), e);

      } else {
        LOGGER.debug("{} is of type {} which is not a primitive type. ", field.getName(),field.getJavaType());
      }
    });
    return vectorMap;
  }

  /**
   * Fills in data recieved from gRPC API call and streams it in RecordBatches.
   * @param allocator
   * @param messageIterator
   * @param descriptor
   * @param listener
   * @param recordBatchSize
   * @param <T>
   */
  public static <T extends Message> void streamData(BufferAllocator allocator,
                                                    Iterator<T> messageIterator,
                                                    Descriptors.Descriptor descriptor,
                                                    ServerStreamListener listener,
                                                    int recordBatchSize) throws Exception {
    LOGGER.debug("Got request to stream Arrowbatches");

    Map<String, ValueVector> vectorMap = setup(descriptor,allocator);
    final AtomicInteger count = new AtomicInteger(0);

    try(VectorSchemaRoot root = VectorSchemaRoot.create(getSchema(descriptor), allocator)) {
      listener.start(root);
      allocateNewUtil(vectorMap);

      while(messageIterator.hasNext()) {
        T message = messageIterator.next();
        handleMessage(message, root, vectorMap, allocator, listener, count, recordBatchSize);
      }

      if(count.get() > 0) {
        stream(vectorMap, count.get(), root, allocator, listener, true);
      }
    }
    closeResources(vectorMap, listener);
  }

  //Flight streamer method
  public static void stream(Map<String,ValueVector> vectorMap,
                            int count,
                            VectorSchemaRoot root,
                            BufferAllocator allocator,
                            ServerStreamListener listener,
                            boolean isLastBatch) {

    try(VectorContainer container = new VectorContainer()) {
      setValueCount(count, vectorMap);

      List<ValueVector> vectorList = new ArrayList<>(vectorMap.values());
      container.addCollection(vectorList);
      container.buildSchema();
      container.setRecordCount(count);

      try(RecordBatchData recordBatchData = new  RecordBatchData(container, allocator)) {
        streamHelper(recordBatchData, listener, root);
        if (!isLastBatch) {
          allocateNewUtil(vectorMap);
        }
      }
    }
  }

  private static void streamHelper(RecordBatchData recordBatch,
                                   ServerStreamListener serverStreamListener,
                                   VectorSchemaRoot root) {
    int rowCount = recordBatch.getRecordCount();
    BatchSchema schema = recordBatch.getSchema();

    for(int i = 0; i < schema.getFields().size(); i++) {
      ValueVector vector = root.getVector(schema.getFields().get(i).getName());
      ValueVector dataVector = recordBatch.getVectors().get(i);

      for(int j = 0; j < rowCount; j++) {
        vector.copyFromSafe(j, j, dataVector);
      }
      vector.setValueCount(rowCount);
      root.setRowCount(rowCount);
      dataVector.close();
    }
    recordBatch.close();

    //TODO: wait for client to be ready
    serverStreamListener.putNext();
    root.allocateNew();
  }

  static <T extends Message> void handleMessage(T message, VectorSchemaRoot root,
    Map<String, ValueVector> vectorMap, BufferAllocator allocator, ServerStreamListener listener,
    AtomicInteger count, int recordBatchSize) {
    message.getDescriptorForType().getFields().forEach(field -> {
      ValueVector v = vectorMap.get(field.getName());
      if (v != null) {

        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.INT) {
          ((IntVector) v).setSafe(count.get(), Integer.parseInt(message.getField(field).toString()));

        } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.LONG) {
          ((BigIntVector) v).setSafe(count.get(), Long.parseLong(message.getField(field).toString()));

        } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.FLOAT) {
          ((Float4Vector) v).setSafe(count.get(), Float.parseFloat(message.getField(field).toString()));

        } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.DOUBLE) {
          ((Float8Vector) v).setSafe(count.get(), Double.parseDouble(message.getField(field).toString()));

        } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.BOOLEAN) {
          boolean flag = Boolean.parseBoolean(message.getField(field).toString());
          ((BitVector) v).setSafe(count.get(), flag ? 1 : 0);

        } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING) {
          ((VarCharVector) v).setSafe(count.get(), message.getField(field).toString().getBytes());

        } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && field.getMessageType().getFullName().equals("google.protobuf.Timestamp")) {
          ((TimeStampVector) v).setSafe(count.get(), ((Timestamp) (message.getField(field))).getSeconds());

        } else {
          LOGGER.debug("ProtobufRecordReader doesn't handle non-primitive types, {} is of type {}.", field.getName(), field.getJavaType());
        }
      }
    });
    count.getAndAdd(1);

    if(count.get() == recordBatchSize) {
      LOGGER.debug("Sending a RecordBatch of size {} to SysFlight stream.", recordBatchSize);
      stream(vectorMap, count.get(), root, allocator, listener, false);
      count.set(0);
    }
  }

  static void allocateNewUtil(Map<String, ValueVector> vectorMap) {
    for (ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  private static void setValueCount(int i, Map<String,ValueVector> vectorMap) {
    for (ValueVector v : vectorMap.values()) {
      v.setValueCount(i);
    }
  }

  private static void closeResources(Map<String, ValueVector> vectorMap, ServerStreamListener listener)
    throws Exception {
    try{
      listener.completed();
    } finally {
      AutoCloseables.close(vectorMap.values());
    }
  }
}
