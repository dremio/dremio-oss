/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.work.protector;

import java.util.List;
import java.util.Map;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.RecordBatchDef;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.options.OptionManager;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;

/**
 * ReAttempt logic for external queries.<br>
 * If we didn't send anything to the client or if we just returned the schema without any data we can still finish the
 * query as long as all fields of the old schema are part of the new schema with the same exact types.
 */
class ExternalAttemptHandler  extends BaseAttemptHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReAttemptHandler.class);

  /** schema sent to the client */
  private Map<String, MajorType> schemaFields;
  private boolean patchResults; // true if data batches should be converted
  private volatile boolean firstBatch;

  ExternalAttemptHandler(OptionManager options) {
    super(options);
  }

  @Override
  public void newAttempt() {
    firstBatch = true;
    super.newAttempt();
  }

  @Override
  public QueryWritableBatch convertIfNecessary(QueryWritableBatch result) {

    if (firstBatch) {
      // in the same attempt, we always expect the same schema to be sent to the client. So we only need to this once
      firstBatch = false;
      patchResults = false;

      final Map<String, MajorType> fieldsMap = extractSchema(result.getHeader().getDef());

      if (schemaFields == null) {
        // this is the first attempt where a schema made it to the client
        // Keep a copy of the schema's fields so we can assess if re-attempts are possible later on
        schemaFields = fieldsMap;
      } else {
        if (!isSchemaConvertible(schemaFields, fieldsMap)) {
          throw UserException.schemaChangeError().message("Unsupported schema change").build(logger);
        } else if (fieldsMap.size() > schemaFields.size()) {
          // we need to discard the "extra" fields from the results sent to the client
          patchResults = true;
        }
      }
    }

    if (patchResults) {
      return patchWritableBatch(result, schemaFields);
    }

    return super.convertIfNecessary(result);
  }

  /**
   * we already sent a schema to the user, this method checks if the new schema can be converted to match the one already
   * sent
   * @param oldSchema schema sent to the user
   * @param newSchema new schema
   * @return true if we can convert the schema, false otherwise
   */
  private static boolean isSchemaConvertible(final Map<String, MajorType> oldSchema,
                                             final Map<String, MajorType> newSchema) {

    int numMatchedFields = 0;

    // for every field of the new schema
    for (String fieldName : newSchema.keySet()) {
      final MajorType fieldType = oldSchema.get(fieldName);
      // check if it's part of the old schema
      if (fieldType != null) { // if it is confirm if it has the same type
        if (!fieldType.equals(newSchema.get(fieldName))) {
          return false; // field has a different type, we cannot convert it
        }

        // field has the same type
        numMatchedFields++;
      }
    }

    // if any of the old schema fields is missing from the schema, we won't be able to convert
    return oldSchema.size() <= numMatchedFields;
  }

  /**
   * Converts a {@link QueryWritableBatch} to a specific schema by removing any field, and its corresponding buffers,
   * that are not in the passed schema
   * @param result batch we want to convert
   * @param schemaFields target schema we are converting to
   * @return converted batch
   */
  private static QueryWritableBatch patchWritableBatch(QueryWritableBatch result, Map<String, MajorType> schemaFields) {
    RecordBatchDef batchDef = result.getHeader().getDef();
    final List<SerializedField> fields = batchDef.getFieldList();
    final ByteBuf[] oldBuffers = result.getBuffers();

    RecordBatchDef.Builder patchedDef = RecordBatchDef.newBuilder(batchDef);
    patchedDef.clearField(); // remove all fields, we'll add the ones that are part of the old schema

    // we instantiate a List<ArrowBuf> instead of List<ByteBuf> as we'll be getting an array out of it
    // and other parts of the code (e.g. CloseableBuffers) will fail otherwise when casting the array
    // to AutoCloseable[]
    List<ArrowBuf> patchedBuffers = Lists.newArrayList();

    int bufferIndex = 0;
    for (SerializedField field : fields) {
      // how many buffers are part of field ?
      int fieldBuffersLength = fieldBuffersCount(field, oldBuffers, bufferIndex);
      final String fieldName = field.getNamePart().getName();
      // is the field part of the new schema, should we keep it ?
      if (schemaFields.containsKey(fieldName)) { // yep
        patchedDef.addField(field);
        // copy the field's buffers into newBuffers
        for (int index = 0; index < fieldBuffersLength; index++, bufferIndex++) {
          patchedBuffers.add(((ArrowBuf) oldBuffers[bufferIndex]));
        }
      } else { // nope
        // skip field and release its buffers
        for (int index = 0; index < fieldBuffersLength; index++, bufferIndex++) {
          oldBuffers[bufferIndex].release();
        }
      }
    }

    // copy trailing buffers, they should all be empty
    while (bufferIndex < oldBuffers.length && oldBuffers[bufferIndex].readableBytes() == 0) {
      patchedBuffers.add(((ArrowBuf) oldBuffers[bufferIndex++]));
    }

    if (bufferIndex != oldBuffers.length) {
      throw new IllegalStateException("Fields should have consumed all the buffers. Instead " +
        (oldBuffers.length - bufferIndex) + " buffers are left");
    }

    return new QueryWritableBatch(QueryData.newBuilder(result.getHeader()).setDef(patchedDef).build(),
            patchedBuffers.toArray(new ArrowBuf[0]));
  }

  /**
   * computes the number of buffers for a given serialized field
   * @param field serialized field
   * @param buffers total buffers in the batch
   * @param buffersStart starting buffer for the passed field
   *
   * @return number of buffers for the field
   */
  private static int fieldBuffersCount(SerializedField field, ByteBuf[] buffers, final int buffersStart) {
    int totalBufferWidth = 0;
    int lastIndex = buffersStart;
    while (totalBufferWidth < field.getBufferLength() && lastIndex < buffersStart + buffers.length) {
      ByteBuf buf = buffers[lastIndex];
      totalBufferWidth += buf.readableBytes();
      ++lastIndex;
    }
    if (totalBufferWidth != field.getBufferLength()) {
      throw new IllegalStateException("not enough buffers for field " + field.getNamePart().getName() +
        " of type " + field.getMajorType());
    }
    return lastIndex - buffersStart;
  }

  private static Map<String, MajorType> extractSchema(final RecordBatchDef batchDef) {
    final List<SerializedField> serializedFields = batchDef.getFieldList();
    final Map<String, MajorType> fieldsMap = Maps.newHashMap();
    for (SerializedField field : serializedFields) {
      fieldsMap.put(field.getNamePart().getName(), field.getMajorType());
    }

    return fieldsMap;
  }
}
