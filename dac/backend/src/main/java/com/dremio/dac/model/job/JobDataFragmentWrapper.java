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
package com.dremio.dac.model.job;

import static com.dremio.common.perf.Timer.time;
import static com.dremio.common.util.MajorTypeHelper.getMajorTypeForField;
import static com.dremio.common.util.MajorTypeHelper.getMinorTypeFromArrowMinorType;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.util.Pair;
import org.joda.time.LocalDateTime;

import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.util.DateTimes;
import com.dremio.dac.explore.DataTypeUtil;
import com.dremio.dac.explore.model.Column;
import com.dremio.dac.explore.model.DACJobResultsSerializer;
import com.dremio.dac.explore.model.DACJobResultsSerializer.SerializationContext;
import com.dremio.dac.explore.model.DataJsonOutput;
import com.dremio.dac.model.job.JobDataFragmentWrapper.JobDataFragmentSerializer;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.EventBasedRecordWriter;
import com.dremio.sabot.op.sort.external.RecordBatchData;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.RecordBatchHolder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * A JSON serializable wrapper around {@code JobDataFragment}
 */
@JsonSerialize(using=JobDataFragmentSerializer.class)
public class JobDataFragmentWrapper implements JobDataFragment {

  private final int offsetInJobResults;
  private final com.dremio.service.jobs.JobDataFragment delegate;
  private final ImmutableList<Column> columns;
  private final ImmutableMap<String, Column> nameToColumns;

  public JobDataFragmentWrapper(int offsetInJobResults, com.dremio.service.jobs.JobDataFragment delegate) {
    this.offsetInJobResults = offsetInJobResults;
    this.delegate = delegate;
    this.nameToColumns = getColumnsFromSchema(delegate.getSchema());
    this.columns = ImmutableList.copyOf(nameToColumns.values());
  }

  public static ImmutableMap<String, Column> getColumnsFromSchema(BatchSchema schema){
    ImmutableMap.Builder<String, Column> columns = ImmutableMap.builder();
    for (int i = 0; i < schema.getFieldCount(); ++i) {
      final Field column = schema.getColumn(i);
      final String name = column.getName();
      final MajorType type = getMajorTypeForField(column);
      DataType dataType = DataTypeUtil.getDataType(type);

      columns.put(name, new Column(name, dataType, i));
    }

    return columns.build();
  }

  @Override
  public void close() throws Exception {
    delegate.close();

  }

  @Override
  public JobId getJobId() {
    return delegate.getJobId();
  }

  @Override
  public List<Column> getColumns() {
    return columns;
  }

  @Override
  public int getReturnedRowCount() {
    return delegate.getReturnedRowCount();
  }

  @Override
  public Column getColumn(String name) {
    return nameToColumns.get(name);
  }

  @Override
  public String extractString(String column, int index) {
    final Object obj = extractValue(column, index);
    if(obj != null){
      DataType cellType = getColumn(column).getType();
      if (cellType == DataType.MIXED) {
        cellType = extractType(column, index);
      }

      switch (cellType) {
        case DATE:
          return DataJsonOutput.FORMAT_DATE.print(DateTimes.toMillis((LocalDateTime)obj));
        case TIME:
          return DataJsonOutput.FORMAT_TIME.print(DateTimes.toMillis((LocalDateTime)obj));
        case DATETIME:
          return DataJsonOutput.FORMAT_TIMESTAMP.print(DateTimes.toMillis((LocalDateTime)obj));
        /**
         * Everything else is converted based on the toString method of the object
        case TEXT:
        case BINARY:
        case BOOLEAN:
        case FLOAT:
        case INTEGER:
        case DECIMAL:
        case MIXED:
        case LIST:
        case MAP:
        case GEO:
        case OTHER:
        */
        default:
          return obj.toString();
      }
    }
    return null;
  }

  @Override
  public Object extractValue(String column, int index) {
    return delegate.extractValue(column, index);
  }

  @Override
  public DataType extractType(String column, int index){
    Pair<RecordBatchData, Integer> dataBatch = find(index);
    Column columnDef = getColumn(column);
    ValueVector vv = dataBatch.getKey().getVectors().get(columnDef.getIndex());
    if (columnDef.getType() == DataType.MIXED) {
      final int typeValue = ((UnionVector)vv).getTypeValue(dataBatch.getValue());
      return DataTypeUtil.getDataType(getMinorTypeFromArrowMinorType(MinorType.values()[typeValue]));
    }

    return columnDef.getType();
  }

  private Pair<RecordBatchData, Integer> find(int index) {
    if (index >= getReturnedRowCount()) {
      throw new IllegalArgumentException(String.format("Invalid index %s", index));
    }

    // Add the offset in the first batch
    int indexWorkspace = index;
    for(RecordBatchHolder batchHolder : delegate.getRecordBatches()) {
      if (indexWorkspace < batchHolder.size()) {
        return new Pair<>(batchHolder.getData(), batchHolder.getStart() + indexWorkspace);
      }
      indexWorkspace -= batchHolder.size();
    }
    throw new IllegalArgumentException(String.format("Invalid index %s", index));
  }

  /**
   * Class to Jackson serialize data.
   */
  public static class JobDataFragmentSerializer extends JsonSerializer<JobDataFragmentWrapper> {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobDataFragmentSerializer.class);

    @Override
    public void serialize(final JobDataFragmentWrapper data, JsonGenerator generator, SerializerProvider provider)
        throws IOException {
      try (TimedBlock b = time("serialize job results VV -> JSON")) {
        /**
         * Example output:
         *
         * {
         *    "returnedRowCount" : 5,
         *    "columns": [
         *        {
         *          "name": "colBit",
         *          "type": "BOOLEAN",
         *          "index": 0,
         *          "status": null
         *        },
         *        {
         *          "name": "colTinyInt",
         *          "type": "INTEGER",
         *          "index": 1,
         *          "status": null
         *        }
         *    ],
         *    "rows" : [
         *       ... one object for each row written by {@link DACJobResultsSerializer} ...
         *       ... See {@link DACJobResultsSerializer} for details of format ...
         *    ]
         * }
         *
         */

        //final RecordBatches recordBatches = data.recordBatches;

        generator.writeStartObject();
        generator.writeFieldName("returnedRowCount");
        generator.writeNumber(data.getReturnedRowCount());
        generator.writeFieldName("columns");
        generator.writeStartArray();
        for (Column c : data.getColumns()) {
          generator.writeObject(c);
        }
        generator.writeEndArray();

        generator.writeFieldName("rows");
        generator.writeStartArray();


        SerializationContextImpl context = new SerializationContextImpl(data.getJobId().getId());

        final boolean convertNumbersToStrings = DataJsonOutput.isNumberAsString(provider);

        DACJobResultsSerializer jsonWriter =
            new DACJobResultsSerializer(generator, context, Integer.getInteger(MAX_CELL_SIZE_KEY, 100), convertNumbersToStrings);
        jsonWriter.setup();

        int currentRowInWriting = data.offsetInJobResults; // row number in complete job results
        for(RecordBatchHolder batchHolder : data.delegate.getRecordBatches()) {
          final EventBasedRecordWriter recordWriter =
              new EventBasedRecordWriter(batchHolder.getData().getContainer(), jsonWriter);

          for (int i = batchHolder.getStart(); i < batchHolder.getEnd(); i++) {
            context.setRowNum(currentRowInWriting++);
            recordWriter.writeOneRecord(i);
          }
        }

        generator.writeEndArray();
        generator.writeEndObject();
      } finally {
        if(data.delegate instanceof ReleaseAfterSerialization) {
          try {
            data.delegate.close();
          } catch (Exception e) {
            logger.error("Failure while releasing job data.", e);
          }
        }
      }
    }
  }

  static class SerializationContextImpl implements SerializationContext {

    private final String prefix;

    private int currentRowNum;

    SerializationContextImpl(final String jobId) {
      this.prefix = String.format("/job/%s/r/", jobId);
    }

    @Override
    public String getCellFetchURL(String columnName) {
      return String.format("%s%d/c/%s", prefix, currentRowNum, columnName);
    }

    public void setRowNum(final int currentRowNum) {
      this.currentRowNum = currentRowNum;
    }
  }
}

