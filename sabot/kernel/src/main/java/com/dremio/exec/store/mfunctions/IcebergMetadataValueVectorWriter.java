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
package com.dremio.exec.store.mfunctions;

import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types.NestedField;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * A value vector write for iceberg metadata functions.
 */
final class IcebergMetadataValueVectorWriter {

  private final OutputMutator output;
  private final CloseableIterator<FileScanTask> iterator;
  private final int targetBatchSize;
  private final List<SchemaPath> projectedColumns;
  private final Schema icebergSchema;
  private final Map<String, Accessor<StructLike>> accessorByColumn;
  private final ArrowBuf tmpBuf;
  private CloseableIterator<StructLike> recordsIterator;

  public IcebergMetadataValueVectorWriter(OutputMutator output, int targetBatchSize, List<SchemaPath> columns, Schema icebergSchema,
                                          CloseableIterator<FileScanTask> iterator, ArrowBuf tmpBuf) {
    Preconditions.checkNotNull(tmpBuf);
    this.output = output;
    this.targetBatchSize = targetBatchSize;
    this.projectedColumns = columns;
    this.icebergSchema = icebergSchema;
    this.accessorByColumn = Maps.newHashMap();
    this.tmpBuf = tmpBuf;
    this.iterator = iterator;
    this.recordsIterator = null;
  }

  public int write() {
    int outIndex = 0;
    try {
      //Check if any records are pending from previous call with recordsIterator when ( total records > targetBatchSize)
      while (recordsIterator != null || iterator.hasNext()) {
        //Initialize recordsIterator only when it is null.
        recordsIterator = recordsIterator == null ? iterator.next().asDataTask().rows().iterator() : recordsIterator;
        while (recordsIterator.hasNext() && outIndex < targetBatchSize) {
          StructLike structLike = recordsIterator.next();
          for (SchemaPath column : projectedColumns) {
            String rootColumn = column.getRootSegment().getPath();
            ValueVector targetVector = output.getVector(rootColumn);
            Object valueToWrite = getValue(structLike, rootColumn);
            if (targetVector instanceof ListVector) {
              ListVector vector = (ListVector) targetVector;
              writeToListVector(vector, outIndex, valueToWrite);
            } else if (targetVector instanceof StructVector) {
              StructVector vector = (StructVector) targetVector;
              writeToMapVector(vector, outIndex, (Map<String, String>) valueToWrite);
            } else {
              writeToVector(targetVector, outIndex, valueToWrite);
            }
          }
          outIndex++;
        }
        //Instantiate a fresh recordsIterator after every file scan.
        recordsIterator = null;
      }
      final int valueCount = outIndex;
      output.getVectors().forEach(v -> v.setValueCount(valueCount));
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .buildSilently();
    }
    return outIndex;
  }

  /**
   * Use this method to update Struct/Map type of vector using Map as values.
   * This is very tightly coupled with Value as <String, String>.
   * Eg: Snapshots -> summary
   */
  private void writeToMapVector(StructVector vector, int outIndex, Map<String, String> data) {
    BaseWriter.StructWriter structWriter = vector.getWriter();
    structWriter.setPosition(outIndex);
    if (data == null ) {
      structWriter.writeNull();
      return;
    }
    structWriter.start();
    List<FieldVector> childrenFromFields = vector.getChildrenFromFields();
    for (FieldVector childrenFromField : childrenFromFields) {
      Field field = childrenFromField.getField();
      switch (field.getFieldType().getType().getTypeID()) {
        case Bool:
          writeBoolValue(structWriter, field.getName(),
            Optional.ofNullable(data.get(field.getName()))
              .map(Boolean::parseBoolean));
          break;
        case Int:
          writeIntValue(structWriter, field.getName(),
            Optional.ofNullable(data.get(field.getName()))
              .map(Integer::parseInt));
          structWriter.integer(field.getName()).writeInt(Integer.parseInt(data.get(field.getName())));
          break;
        case Utf8:
          writeStringValue(structWriter, field.getName(), data.get(field.getName()));
          break;
      }
    }
    structWriter.end();
  }

  private void writeStringValue(BaseWriter.StructWriter structWriter, String fieldName, String value) {
    if (value == null) {
      structWriter.varChar(fieldName).writeNull();
    } else {
      byte[] path = value.getBytes(StandardCharsets.UTF_8);
      tmpBuf.reallocIfNeeded(path.length);
      tmpBuf.setBytes(0, path);
      structWriter.varChar(fieldName).writeVarChar(0, path.length, tmpBuf);
    }
  }

  private void writeBoolValue(BaseWriter.StructWriter structWriter, String fieldName, Optional<Boolean> value) {
    if (value.isPresent()) {
      structWriter.bit(fieldName).writeBit(value.get() ? 1 : 0);
    } else {
      structWriter.bit(fieldName).writeNull();
    }
  }

  private void writeIntValue(BaseWriter.StructWriter structWriter, String fieldName, Optional<Integer> value) {
    if (value.isPresent()) {
      structWriter.integer(fieldName).writeInt(value.get());
    } else {
      structWriter.integer(fieldName).writeNull();
    }
  }

  /**
   * Use this method to write to list vector. It reads from structLike data.
   * eg: manifests -> partition_summaries
   */
  private void writeToListVector(ListVector vector, int outIndex, Object data) {
    UnionListWriter writer = vector.getWriter();
    writer.setPosition(outIndex);
    if (data == null) {
      writer.writeNull();
      return;
    }
    writer.startList();
    BaseWriter.StructWriter structWriter = writer.struct();
    List<FieldVector> childrenFromFields = vector.getChildrenFromFields().get(0).getChildrenFromFields();
    if (data instanceof List) {
      for (StructLike structLike : (List<StructLike>) data) {
        structWriter.start();
        for (int i = 0; i < childrenFromFields.size(); i++) {
          Field field = childrenFromFields.get(i).getField();
          switch (field.getFieldType().getType().getTypeID()) {
            case Bool:
              writeBoolValue(structWriter, field.getName(),
                Optional.ofNullable(structLike.get(i, Boolean.class)));
              break;
            case Utf8:
              writeStringValue(structWriter, field.getName(), structLike.get(i, String.class));
              break;
          }
        }
        structWriter.end();
      }
    }

    if (data instanceof Map) {
      for (Map.Entry<String, String> entry :  ((Map<String , String >) data).entrySet()) {
        structWriter.start();
        writeStringValue(structWriter, childrenFromFields.get(0).getName(), entry.getKey());
        writeStringValue(structWriter, childrenFromFields.get(1).getName(), entry.getValue());
        structWriter.end();
      }
    }

    writer.endList();
  }

  private Object getValue(StructLike structLike, String column) {
    Accessor<StructLike> accessor = accessorByColumn.get(column);
    if (accessor == null) {
      NestedField field = icebergSchema.caseInsensitiveFindField(column);
      Preconditions.checkNotNull(field, "Field for column '%s' not found in schema:\n%s", column, icebergSchema);
      accessor = icebergSchema.accessorForField(field.fieldId());
      Preconditions.checkNotNull(accessor, "Field accessor for column '%s' not found in schema:\n%s", column, icebergSchema);
      Accessor<StructLike> oldAccessor = accessorByColumn.put(column, accessor);
      Preconditions.checkState(oldAccessor == null, "Duplicate field accessor for column '%s' in schema:\n%s", column, icebergSchema);
    }
    return accessor.get(structLike);
  }
}
