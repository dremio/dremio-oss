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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DremioManifestReaderUtils.ManifestEntryWrapper;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Datafile processor implementation which generates data files info
 */
public class DataFileContentReader implements ManifestEntryProcessor {

  private final BatchSchema outputSchema;
  private final List<ValueVector> valueVectorList = new LinkedList<>();
  private Schema fileSchema;
  private PartitionSpec icebergPartitionSpec;
  private boolean doneWithCurrentDatafile;
  private final ArrowBuf tmpBuf;
  private Map<Integer, Type> idTypeMap;

  public DataFileContentReader(OperatorContext context, TableFunctionContext functionContext) {
    outputSchema = functionContext.getFullSchema();
    tmpBuf = context.getAllocator().buffer(4096);
  }

  @Override
  public void setup(VectorAccessible incoming, VectorAccessible outgoing) {
    for (Field field : outputSchema.getFields()) {
      ValueVector vector = outgoing.getValueAccessorById(TypeHelper.getValueVectorClass(field),
        outgoing.getSchema().getFieldId(SchemaPath.getSimplePath(field.getName())).getFieldIds()).getValueVector();
      valueVectorList.add(vector);
    }
  }

  private static class VectorValueSupplier {
    private final ValueVector valueVector;
    private final Supplier<Object> valueSupplier;

    public VectorValueSupplier(ValueVector valueVector, Supplier<Object> valueSupplier) {
      this.valueVector = valueVector;
      this.valueSupplier = valueSupplier;
    }
  }

  @Override
  public void initialise(PartitionSpec partitionSpec) {
    icebergPartitionSpec = partitionSpec;
    fileSchema = icebergPartitionSpec.schema();
    idTypeMap = fileSchema.columns().stream().collect(
      Collectors.toMap(Types.NestedField::fieldId, Types.NestedField::type));
  }

  /**
   * return each row for table_files function. After each process, maxOutputCount should be decreased most by one
   */
  @Override
  public int processManifestEntry(ManifestEntryWrapper<? extends ContentFile<?>> manifestEntry, int startOutIndex, int maxOutputCount) {
    DataFile currentDataFile = (DataFile) manifestEntry.file();
    if (!shouldProcessCurrentDatafile(maxOutputCount)) {
      return 0;
    }
    final List<VectorValueSupplier> valueSuppliers = new LinkedList<>();
    for (ValueVector vector: valueVectorList) {
      valueSuppliers.add(new VectorValueSupplier(vector, getFieldValueSupplier(vector.getField().getName(), currentDataFile)));
    }

    for (VectorValueSupplier field : valueSuppliers) {
      if (field.valueVector instanceof ListVector) {
        writeToListVector((ListVector) field.valueVector, startOutIndex, field.valueSupplier.get());
      } else {
        writeToVector(field.valueVector, startOutIndex, field.valueSupplier.get());
      }
    }
    doneWithCurrentDatafile = true;
    return 1;
  }

  private Supplier<Object> getFieldValueSupplier(String fieldName, DataFile currentDataFile) {
    switch (fieldName){
      case "content": return () -> currentDataFile.content().id();
      case "file_path":
      case "datafilePath":
        return () -> getStringValue(currentDataFile.path());
      case "file_format": return () -> currentDataFile.format().name();
      case "partition": return () -> getPartitionData(currentDataFile);
      case "existingpartitioninfo": return () -> {
        try {
          return IcebergSerDe.serializeToByteArray(IcebergPartitionData.fromStructLike(icebergPartitionSpec, currentDataFile.partition()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
      case "record_count": return currentDataFile::recordCount;
      case "file_size_in_bytes": return currentDataFile::fileSizeInBytes;
      case "column_sizes": return currentDataFile::columnSizes;
      case "value_counts": return currentDataFile::valueCounts;
      case "null_value_counts": return currentDataFile::nullValueCounts;
      case "nan_value_counts": return currentDataFile::nanValueCounts;
      case "lower_bounds": return currentDataFile::lowerBounds;
      case "upper_bounds": return currentDataFile::upperBounds;
      case "split_offsets": return currentDataFile::splitOffsets;
      case "equality_ids": return currentDataFile::equalityFieldIds;
      case "sort_order_id": return currentDataFile::sortOrderId;
      default:
        throw new UnsupportedOperationException("Invalid fieldName for table files query " + fieldName);
    }
  }

  /**
   * convert to list from Map or list<int/long>
   * @param vector
   * @param outIndex
   * @param data
   */
  private void writeToListVector(ListVector vector, int outIndex, Object data) {
    UnionListWriter writer = vector.getWriter();
    writer.setPosition(outIndex);
    if (data == null) {
      writer.writeNull();
      return;
    }
    writer.startList();
    List<FieldVector> childrenFromFields = vector.getChildrenFromFields().get(0).getChildrenFromFields();
    if (data instanceof Map) {
      BaseWriter.StructWriter structWriter = writer.struct();
      //https://iceberg.apache.org/spec/, All the Map contents from data_file should always come with key as int which refers to col id.
      // It will only use columns from current schema, For deleted cols data will not be sent for output.
      //eg: for lower_bounds -> it will only output for current cols with current dataFile. For older dataFile it will be null.
      for (Map.Entry<Object, Object> entry :  ((Map<Object , Object >) data).entrySet()) {
        if (entry.getValue() instanceof ByteBuffer) {
          if (entry.getKey() instanceof Integer && idTypeMap.containsKey(entry.getKey())) {
            structWriter.start();
            writeStringValue(structWriter, childrenFromFields.get(0).getName(), String.valueOf(entry.getKey()));
            writeStringValue(structWriter, childrenFromFields.get(1).getName(),
              IcebergUtils.getValueFromByteBuffer((ByteBuffer) entry.getValue(), idTypeMap.get(entry.getKey())).toString());
            structWriter.end();
          }
        }  else {
          structWriter.start();
          writeStringValue(structWriter, childrenFromFields.get(0).getName(), String.valueOf(entry.getKey()));
          writeStringValue(structWriter, childrenFromFields.get(1).getName(), String.valueOf(entry.getValue()));
          structWriter.end();
        }

      }
    }
    if (data instanceof List) {
      for (int j = 0; j < ((List<Object>) data).size(); j++) {
        Object value = ((List<Object>) data).get(j);
        if (value == null) {
          writer.writeNull(); //this will write like [1,null,2,null]
        } else {
          if (value instanceof Long) {
            writer.bigInt().writeBigInt((Long) value);
          }
          if (value instanceof Integer) {
            writer.integer().writeInt((Integer) value);
          }
        }
      }
    }
    writer.endList();
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

  /**
   * Convert partition by data to String. Data file return structLike. it's trusting on partition spec for correct order for position
   * @param currentDataFile
   * @return
   */
  private String getPartitionData(DataFile currentDataFile) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("{");
    List<Types.NestedField> fields = icebergPartitionSpec.partitionType().asStructType().fields();
    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField nestedField = fields.get(i);
      stringBuilder.append(currentDataFile.partition().get(i, nestedField.type().typeId().javaClass()));
      if (i != fields.size()-1) {
        stringBuilder.append(", ");
      }
    }
    stringBuilder.append("}");
    return stringBuilder.toString();
  }

  private String getStringValue(Object ob){
    if(ob!=null){
      return ob.toString();
    }
    return null;
  }

  @Override
  public void closeManifestEntry() {
    doneWithCurrentDatafile = false;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(tmpBuf);
  }

  private boolean shouldProcessCurrentDatafile(int maxOutputCount) {
    return !doneWithCurrentDatafile && maxOutputCount > 0;
  }
}
