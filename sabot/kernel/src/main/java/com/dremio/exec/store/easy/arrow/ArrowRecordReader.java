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
package com.dremio.exec.store.easy.arrow;

import static com.dremio.exec.cache.VectorAccessibleSerializable.readIntoArrowBuf;
import static com.dremio.exec.store.easy.arrow.ArrowFormatPlugin.FOOTER_OFFSET_SIZE;
import static com.dremio.exec.store.easy.arrow.ArrowFormatPlugin.MAGIC_STRING;
import static com.dremio.exec.store.easy.arrow.ArrowFormatPlugin.MAGIC_STRING_LENGTH;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.easy.arrow.ArrowFileFormat.ArrowFileFooter;
import com.dremio.exec.store.easy.arrow.ArrowFileFormat.ArrowRecordBatchSummary;
import com.dremio.exec.vector.complex.fn.FieldSelection;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.netty.buffer.ArrowBuf;

/**
 * {@link RecordReader} implementation for reading Arrow format files. Currently this reader can only read files written
 * by writer {@link ArrowRecordWriter}.
 */
public class ArrowRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowRecordReader.class);

  private final FileSystemWrapper dfs;
  private final Path path;
  private final byte[] copyBuffer = new byte[64*1024];

  private FSDataInputStream inputStream;
  private ArrowFileFooter footer;
  private BatchSchema footerSchema;
  private BufferAllocator allocator;

  private Map<Integer, ValueVector> vectors = Maps.newHashMap(); // map of column index and output vector

  /**
   * File can contain several record batches. This index points to the next record batch.
   */
  private int nextBatchIndex;

  public ArrowRecordReader(final OperatorContext context, final FileSystemWrapper dfs, final Path path,
      List<SchemaPath> columns) {
    super(context, columns);
    this.dfs = dfs;
    this.path = path;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      allocator = context.getAllocator();

      inputStream = dfs.open(path);

      final FileStatus fileStatus = dfs.getFileStatus(path);
      final long len = fileStatus.getLen();

      // Make sure the file size is at least the 2 * (Magic word size) + Footer offset size
      // We write magic word both at the beginning and at the end of the file.
      if (len < 2*MAGIC_STRING_LENGTH + FOOTER_OFFSET_SIZE) {
        throw UserException.dataReadError()
            .message("File is too small to be an Arrow format file")
            .addContext("path", path.toString())
            .build(logger);
      }

      inputStream.seek(len - (MAGIC_STRING_LENGTH + FOOTER_OFFSET_SIZE));

      final long footerOffset = inputStream.readLong();

      final byte[] magic = new byte[MAGIC_STRING_LENGTH];
      inputStream.readFully(magic);
      // Make sure magic word matches
      if (!Arrays.equals(magic, MAGIC_STRING.getBytes())) {
        throw UserException.dataReadError()
            .message("Invalid magic word. File is not an Arrow format file")
            .addContext("path", path.toString())
            .build(logger);
      }

      // Make sure the footer offset is valid
      if (footerOffset < MAGIC_STRING_LENGTH || footerOffset >= (len - (MAGIC_STRING_LENGTH + FOOTER_OFFSET_SIZE))) {
        throw UserException.dataReadError()
            .message("Invalid footer offset")
            .addContext("filePath", path.toString())
            .addContext("invalid footer offset", String.valueOf(footerOffset))
            .build(logger);
      }

      // Read the footer
      inputStream.seek(footerOffset);
      footer = ArrowFileFooter.parseDelimitedFrom(inputStream);
      footerSchema = BatchSchema.newBuilder().addSerializedFields(footer.getFieldList()).build();

      // From the footer, construct selected column field type info and vectors. Also add the types to output mutator.
      int i=0;
      if(!getColumns().isEmpty()){
        final FieldSelection select = FieldSelection.getFieldSelection(Lists.newArrayList(getColumns()));
        for(final SerializedField field : footer.getFieldList()) {
          final Field f = SerializedFieldHelper.create(field);
          if (!select.getChild(f.getName()).isNeverValid()) {
            // We come here in two cases:
            // 1. When the projected field is directly matched.
            // 2. When the projected field is a child of current field (possible when the field is a map).
            vectors.put(i, (output.addField(f, (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(f))));
          }
          i++;
        }
      }

      // Reset to beginning of the file
      inputStream.seek(0);
      nextBatchIndex = 0;
    } catch (final Exception e) {
      throw UserException.dataReadError(e)
          .message("Failed to read the Arrow formatted file.")
          .build(logger);
    }
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    // no-op as this allocates buffers based on the size of the buffers in file.
  }

  @Override
  public int next() {
    if (nextBatchIndex >= footer.getBatchCount()) {
      return 0;
    }

    try {
      // Get the next batch info and seek to the location where the batch starts
      ArrowRecordBatchSummary batchSummary = footer.getBatch(nextBatchIndex);

      while (batchSummary.getRecordCount() == 0) {
        // If the batch has no records, go to the next non-zero record batch. Returning a zero record batch to ScanBatch
        // ends up closing the RecordReader as it assumes there are no more records in the reader.
        if (nextBatchIndex == footer.getBatchCount()) {
          // no more batches in the file
          return 0;
        }

        batchSummary = footer.getBatch(++nextBatchIndex);
      }

      inputStream.seek(batchSummary.getOffset());

      // Read the RecordBatchDef
      final UserBitShared.RecordBatchDef batchDef = UserBitShared.RecordBatchDef.parseDelimitedFrom(inputStream);
      final int recordCount = batchDef.getRecordCount();
      if (batchDef.hasCarriesTwoByteSelectionVector() && batchDef.getCarriesTwoByteSelectionVector()) {
        // We shouldn't get into this condition as the writer never gets a batch with SV2.
        throw UserException.unsupportedError()
            .message("Selection vector is not supported")
            .build(logger);
      }

      final List<SerializedField> fieldListFromBatch = batchDef.getFieldList();

      BatchSchema batchSchema = BatchSchema.newBuilder().addSerializedFields(fieldListFromBatch).build();

      // Compare the filed types given in batch definition and footer.
      if (!footerSchema.equals(batchSchema)) {
        throw UserException.dataReadError()
            .message("RecordBatch has different schema than the one stored in footer")
            .addContext("filePath", path.toString())
            .addContext("RecordBatch schema", batchSchema.toString())
            .addContext("Schema in footer", footerSchema)
            .build(logger);
      }

      // Read the buffers and load into vectors
      int loadedVectors = vectors.size();

      for(int i=0; i<fieldListFromBatch.size(); i++) {
        final SerializedField serializedField = fieldListFromBatch.get(i);
        final int dataLength = serializedField.getBufferLength();
        // if this field is selected read, otherwise skip the buffers
        if (vectors.containsKey(i)) {
          try(ArrowBuf buf = allocator.buffer(dataLength)) {
            readIntoArrowBuf(inputStream, buf, dataLength, copyBuffer);
            TypeHelper.load(vectors.get(i), serializedField, buf);
            loadedVectors--;
            if (loadedVectors == 0) {
              break;
            }
          }
        } else {
          inputStream.skip(dataLength);
        }
      }

      nextBatchIndex++;

      return recordCount;
    } catch (final Exception e) {
      throw UserException.dataReadError(e)
          .message("Failed to read data from Arrow format file.")
          .addContext("filePath", path.toString())
          .addContext("currentBatchIndex", nextBatchIndex)
          .build(logger);
    }
  }



  @Override
  protected boolean supportsSkipAllQuery() {
    return true;
  }

  /**
   * Make sure the type info in given SerilizedFields is same. SerializedField contains additional info such as
   * buffer lengths which we are not interested.
   */
  private static boolean equalSchema(final List<SerializedField> fieldSet1, final List<SerializedField> fieldSet2) {
    if (fieldSet1.size() != fieldSet2.size()) {
      return false;
    }

    for(int i=0; i<fieldSet1.size(); i++) {
      if (!typeEqual(fieldSet1.get(i), fieldSet2.get(i))) {
        return false;
      }
    }

    return true;
  }

  private static boolean typeEqual(SerializedField field1, SerializedField field2) {
    if (!field1.getMajorType().equals(field2.getMajorType())) {
      return false;
    }

    if (field1.getChildCount() != field2.getChildCount()) {
      return false;
    }

    for(int i=0; i<field1.getChildCount(); i++) {
      final SerializedField childField1 = field1.getChild(i);
      final SerializedField childField2 = field2.getChild(i);
      if (!typeEqual(childField1, childField2)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void close() throws Exception {
    if (inputStream != null) {
      inputStream.close();
    }
  }
}
