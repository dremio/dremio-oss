/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.jobs;

import static com.dremio.exec.store.easy.arrow.ArrowFormatPlugin.FOOTER_OFFSET_SIZE;
import static com.dremio.exec.store.easy.arrow.ArrowFormatPlugin.MAGIC_STRING;
import static com.dremio.exec.store.easy.arrow.ArrowFormatPlugin.MAGIC_STRING_LENGTH;
import static com.dremio.service.jobs.RecordBatchHolder.newRecordBatchHolder;
import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.Field;
//import com.dremio.exec.util.AssertionUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.DataMode;
import com.dremio.common.types.MajorType;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.cache.VectorAccessibleSerializable;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.beans.NamePart;
import com.dremio.exec.proto.beans.NamePart.Type;
import com.dremio.exec.proto.beans.SerializedField;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.easy.arrow.ArrowFileFooter;
import com.dremio.exec.store.easy.arrow.ArrowFileFormat;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.exec.store.easy.arrow.ArrowRecordBatchSummary;
import com.dremio.sabot.op.sort.external.RecordBatchData;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Reader which takes a file and reads the record batches.
 */
class ArrowFileReader implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowFileReader.class);

  private final FileSystem dfs;
  private final Path basePath;
  private final ArrowFileMetadata metadata;
  private final BufferAllocator allocator;

  private FSDataInputStream inputStream;

  ArrowFileReader(final FileSystem dfs, Path basePath, final ArrowFileMetadata metadata, final BufferAllocator allocator) {
    this.dfs = dfs;
    this.basePath = basePath;
    this.metadata = metadata;
    this.allocator = allocator;
  }

  private void openFile() throws IOException {
    final Path path = new Path(basePath, metadata.getPath());
    inputStream = dfs.open(path);

    if (false /* disable this until a PDFS getFileStatus() issue is fixed AssertionUtil.ASSERT_ENABLED */) {
      final FileStatus fileStatus = dfs.getFileStatus(path);
      final long len = fileStatus.getLen();

      // Make sure the file size is at least the 2 * (Magic word size) + Footer offset size
      // We write magic word both at the beginning and at the end of the file.
      if (len < 2 * MAGIC_STRING_LENGTH + FOOTER_OFFSET_SIZE) {
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
    }
  }

  /**
   * Read the record batches containing the rows in given range.
   * @param start Starting record number in file (0 based index)
   * @param limit number of records to read
   * @return
   */
  List<RecordBatchHolder> read(final long start, final long limit) throws IOException {
    // Make sure the range is valid according to the metadata in footer
    checkArgument(start == 0 && metadata.getRecordCount() == 0|| start >= 0 && start < metadata.getRecordCount(),
        "Invalid start index (%s). Record count in file (%s)", start, metadata.getRecordCount());
    checkArgument(start + limit <= metadata.getRecordCount(),
        "Invalid start index (%s) and limit (%s) combination. Record count in file (%s)",
        start, limit, metadata.getRecordCount());

    openFile();

    final VectorAccessibleSerializable vectorAccessibleSerializable = new VectorAccessibleSerializable(allocator);
    final List<RecordBatchHolder> batches = Lists.newArrayList();
    final ArrowFileFooter footer = metadata.getFooter();

    long runningCount = 0;
    long remaining = limit;
    final int numBatches = footer.getBatchList() == null ? 0 : footer.getBatchList().size();
    for(int batchIndex = 0; batchIndex < numBatches; batchIndex++) {
      ArrowRecordBatchSummary batchSummary = footer.getBatchList().get(batchIndex);
      // Skip past empty batches
      if (batchSummary.getRecordCount() == 0) {
        continue;
      }

      runningCount += batchSummary.getRecordCount();

      // Skip batches until we reach a batch that contains the start index
      if (start >= runningCount) {
        // valid indices in up until the current batch are in range [0, runningCount - 1]
        continue;
      }

      final long currentBatchCount = batchSummary.getRecordCount();

      // Seek to the place where the batch starts and read
      inputStream.seek(batchSummary.getOffset());
      vectorAccessibleSerializable.readFromStream(inputStream);
      final VectorContainer vectorContainer = vectorAccessibleSerializable.get();

      // Find the start and end indices within the batch.
      final int batchStart = Math.max(0, (int) (start - (runningCount - currentBatchCount)));
      final int batchEnd = (int) Math.min(currentBatchCount, batchStart + remaining);

      final RecordBatchHolder batchHolder = newRecordBatchHolder(
          new RecordBatchData(vectorContainer, allocator),
          batchStart,
          batchEnd
      );

      batches.add(batchHolder);

      remaining -= batchHolder.size();

      if (remaining == 0) {
        break;
      }
    }

    if (batches.isEmpty()) {
      final VectorContainer vectorContainer = new VectorContainer();
      for(SerializedField field : footer.getFieldList()) {
        vectorContainer.add(TypeHelper.getNewVector(getFieldForSerializedField(field), allocator));
      }
      vectorContainer.setRecordCount(0);
      vectorContainer.buildSchema();

      batches.add(newRecordBatchHolder(new RecordBatchData(vectorContainer, allocator), 0, 0));
    }

    return batches;
  }

  @Override
  public void close() throws IOException {
    if (inputStream != null) {
      inputStream.close();
    }
  }

  /**
   * Helper method to convert the protobuf message into a bean class. Currently it ignores converting the row type.
   * Add the support for row type conversion in future if needed.
   *
   * TODO: We need check if there is a way to auto convert protobuf messages to protostuff bean classes.
   *
   * @param metadata
   * @return
   */
  static ArrowFileMetadata toBean(ArrowFileFormat.ArrowFileMetadata metadata) {
    final ArrowFileMetadata beanMetadata = new ArrowFileMetadata();

    beanMetadata.setPath(metadata.getPath());
    beanMetadata.setRecordCount(metadata.getRecordCount());

    ArrowFileFooter beanFooter = new ArrowFileFooter();
    beanFooter.setBatchList(new ArrayList<ArrowRecordBatchSummary>());
    for(ArrowFileFormat.ArrowRecordBatchSummary summary : metadata.getFooter().getBatchList()) {
      ArrowRecordBatchSummary beanSummary = new ArrowRecordBatchSummary();
      beanSummary.setOffset(summary.getOffset());
      beanSummary.setRecordCount(summary.getRecordCount());

      beanFooter.getBatchList().add(beanSummary);
    }

    List<SerializedField> outputFields = Lists.newArrayList();
    for(UserBitShared.SerializedField inputField : metadata.getFooter().getFieldList()) {
      outputFields.add(toBean(inputField));
    }
    beanFooter.setFieldList(outputFields);

    beanMetadata.setFooter(beanFooter);

    return beanMetadata;
  }

  private static SerializedField toBean(UserBitShared.SerializedField input) {
    SerializedField output = new SerializedField();
    if (input.hasNamePart()) {
      output.setNamePart(toBean(input.getNamePart()));
    }

    if (input.hasBufferLength()) {
      output.setBufferLength(input.getBufferLength());
    }

    if (input.hasValueCount()) {
      output.setValueCount(input.getValueCount());
    }

    if (input.hasVarByteLength()) {
      output.setVarByteLength(input.getVarByteLength());
    }

    if (input.hasMajorType()) {
      MajorType majorTypeOutput = new MajorType();
      TypeProtos.MajorType majorTypeInput = input.getMajorType();

      if (majorTypeInput.hasMinorType()) {
        majorTypeOutput.setMinorType(com.dremio.common.types.MinorType.valueOf(majorTypeInput.getMinorType().getNumber()));
      }

      if (majorTypeInput.hasMode()) {
        majorTypeOutput.setMode(DataMode.valueOf(majorTypeInput.getMode().getNumber()));
      }

      if (majorTypeInput.hasWidth()) {
        majorTypeOutput.setWidth(majorTypeInput.getWidth());
      }

      if (majorTypeInput.hasPrecision()) {
        majorTypeOutput.setPrecision(majorTypeInput.getPrecision());
      }

      if (majorTypeInput.hasScale()) {
        majorTypeOutput.setScale(majorTypeInput.getScale());
      }

      if (majorTypeInput.hasTimeZone()) {
        majorTypeOutput.setTimeZone(majorTypeInput.getTimeZone());
      }

      List<com.dremio.common.types.MinorType> subTypes = Lists.newArrayList();
      for(TypeProtos.MinorType minorType : majorTypeInput.getSubTypeList()) {
        subTypes.add(com.dremio.common.types.MinorType.valueOf(minorType.getNumber()));
      }
      majorTypeOutput.setSubTypeList(subTypes);

      output.setMajorType(majorTypeOutput);
    }

    List<SerializedField> outputChilds = Lists.newArrayList();
    for(UserBitShared.SerializedField inputChild : input.getChildList()) {
      outputChilds.add(toBean(inputChild));
    }
    output.setChildList(outputChilds);

    return output;
  }

  private static NamePart toBean(UserBitShared.NamePart input) {
    NamePart output = new NamePart();
    if (input.hasType()) {
      switch (input.getType()) {
        case NAME:
          output.setType(Type.NAME);
          break;
        case ARRAY:
          output.setType(Type.ARRAY);
          break;
        default:
          throw new UnsupportedOperationException("Unknown NamePart type: " + input.getType());
       }
    }

    if (input.hasName()) {
      output.setName(input.getName());
    }

    if (input.hasChild()) {
      output.setChild(toBean(input.getChild()));
    }

    return output;
  }

  private static Field getFieldForSerializedField(SerializedField serializedField) {
    MinorType arrowMinorType = MajorTypeHelper.getArrowMinorType(
        TypeProtos.MinorType.valueOf(serializedField.getMajorType().getMinorType().name()));
    switch(serializedField.getMajorType().getMinorType()) {
      case LIST: {
        final String name = serializedField.getNamePart().getName();
        return new Field(name, true, arrowMinorType.getType(),
            ImmutableList.of(getFieldForSerializedField(serializedField.getChildList().get(2))));
      }
      case MAP: {
        final String name = serializedField.getNamePart().getName();
        ImmutableList.Builder<Field> builder = ImmutableList.builder();
        List<SerializedField> childList = serializedField.getChildList();
        Preconditions.checkState(childList.size() > 0, "children should start with $bits$ vector");
        SerializedField bits = childList.get(0);
        Preconditions.checkState(bits.getNamePart().getName().equals("$bits$"),
            "children should start with $bits$ vector: %s", childList);
        for (int i = 1; i < childList.size(); i++) {
          SerializedField child = childList.get(i);
          builder.add(getFieldForSerializedField(child));
        }
        return new Field(name, true, arrowMinorType.getType(), builder.build());
      }
      case UNION: {
        final String name = serializedField.getNamePart().getName();
        ImmutableList.Builder<Field> builder = ImmutableList.builder();
        final List<SerializedField> unionChilds = serializedField.getChildList().get(1).getChildList();
        final int[] typeIds = new int[unionChilds.size()];
        for (int i=0; i < unionChilds.size(); i++) {
          final Field childField = getFieldForSerializedField(unionChilds.get(i));
          builder.add(childField);
          typeIds[i] =  Types.getMinorTypeForArrowType(childField.getType()).ordinal();
        }

        // TODO: not sure the sparse mode is correct.
        final Union unionType = new Union(UnionMode.Sparse, typeIds);
        return new Field(name, true, unionType, builder.build());
      }
      case DECIMAL: {
        final String name = serializedField.getNamePart().getName();
        return new Field(name, true, new Decimal(serializedField.getMajorType().getPrecision(),
            serializedField.getMajorType().getScale()), null);
      }
      case NULL:
        return ZeroVector.INSTANCE.getField();
      default: {
        final String name = serializedField.getNamePart().getName();
        return new Field(name, true, arrowMinorType.getType(), null);
      }
    }
  }
}
