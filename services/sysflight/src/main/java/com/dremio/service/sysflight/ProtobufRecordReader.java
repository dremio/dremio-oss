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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.record.VectorContainer;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

/**
 * Utility class for getting schema and populating the vectors automatically. Supports only primitve types.
 */
public class ProtobufRecordReader {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ProtobufRecordReader.class);
  //TODO: Add support for timesptamp datafields(BigInt to TimeStamp).

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

      } else {
        LOGGER.debug("{} is of type {} which is not a primitive type. ", field.getName(),field.getJavaType());
      }
    });
    BatchSchema schema = schemaBuilder.build();
    LOGGER.debug("BatchSchema {}", schema);
    return schema;
  }

  private static Map<String, ValueVector> setup(Descriptors.Descriptor descriptor,BufferAllocator allocator){
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
   * @param RECORD_BATCH_SIZE
   * @param <T>
   */
  public static <T extends Message> void streamData(BufferAllocator allocator,
                                                    Iterator<T> messageIterator,
                                                    Descriptors.Descriptor descriptor,
                                                    ServerStreamListener listener,
                                                    int RECORD_BATCH_SIZE) {
    LOGGER.debug("Got request to stream Arrowbatches");

    Map<String, ValueVector> vectorMap = setup(descriptor,allocator);
    final AtomicInteger count = new AtomicInteger(0);
    final AtomicInteger numRecordBatchesSent = new AtomicInteger(0);

    try( VectorSchemaRoot root = VectorSchemaRoot.create(getSchema(descriptor), allocator)) {
      listener.start(root);
      allocateNewUtil(vectorMap);

      while(messageIterator.hasNext()) {
        T message = messageIterator.next();

        message.getDescriptorForType().getFields().forEach(field -> {
          ValueVector v = vectorMap.get(field.getName());

          if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.INT) {
            ((IntVector) v).setSafe(count.get(),Integer.parseInt(message.getField(field).toString()));

          } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.LONG) {
            ((BigIntVector) v).setSafe(count.get(), Long.parseLong(message.getField(field).toString()));

          } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.FLOAT) {
            ((Float4Vector) v).setSafe(count.get(), Float.parseFloat(message.getField(field).toString()));

          } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.DOUBLE) {
            ((Float8Vector) v).setSafe(count.get(), Double.parseDouble(message.getField(field).toString()));

          } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.BOOLEAN) {
            boolean flag = Boolean.parseBoolean(message.getField(field).toString());
            ((BitVector) v).setSafe(count.get(), flag ? 1:0);

          } else if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING) {
            ((VarCharVector) v).setSafe(count.get(), message.getField(field).toString().getBytes());

          } else {
            LOGGER.debug("{} is of type {} which is not a primitive type. ", field.getName(),field.getJavaType());
          }
        });
        count.getAndAdd(1);

        if(count.get() == RECORD_BATCH_SIZE) {
          streamHelper(vectorMap, count.get(), root, allocator, listener);
          numRecordBatchesSent.getAndAdd(1);
          count.set(0);
        }
      }

      if(count.get() > 0) {
        streamHelper(vectorMap, count.get(), root, allocator, listener);
        numRecordBatchesSent.getAndAdd(1);
      }
    }
    closeResources(vectorMap, listener);
  }

  private static void streamHelper(Map<String,ValueVector> vectorMap,
                            int count,
                            VectorSchemaRoot root,
                            BufferAllocator allocator,
                            ServerStreamListener listener) {

    try(VectorContainer container = new VectorContainer()){
      setValueCount(count, vectorMap);

      List<ValueVector> vectorList = new ArrayList<>(vectorMap.values());
      container.addCollection(vectorList);
      container.buildSchema();
      container.setRecordCount(count);

      try(RecordBatchData recordBatchData = new  RecordBatchData(container, allocator)) {
        stream(recordBatchData, listener, root);
        allocateNewUtil(vectorMap);
      }
    }
  }

  //Flight streamer method
  private static void stream(RecordBatchData recordBatch,
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

  private static void setValueCount(int i, Map<String,ValueVector> vectorMap) {
    for (ValueVector v : vectorMap.values()) {
      v.setValueCount(i);
    }
  }

  private static void allocateNewUtil(Map<String, ValueVector> vectorMap) {
    for (ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  private static void closeResources(Map<String, ValueVector> vectorMap, ServerStreamListener listener){
    for(ValueVector v : vectorMap.values()) {
      v.close();
    }
    listener.completed();
  }
}
