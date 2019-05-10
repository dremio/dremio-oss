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
package com.dremio.dac.api;

import java.io.IOException;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.dac.explore.model.APIJobResultsSerializer;
import com.dremio.dac.explore.model.DataJsonOutput;
import com.dremio.exec.store.EventBasedRecordWriter;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.RecordBatchHolder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Query Job Data
 */
@JsonSerialize(using=JobData.JobDataSerializer.class)
public class JobData {
  private JobDataFragment delegate;
  private Job job;

  public JobData() {
  }

  public JobData(Job job, int offset, int limit) {
    this.delegate = job.getData().range(offset, limit);
    this.job = job;
  }

  public JobDataFragment getJobDataFragment() {
    return delegate;
  }

  public long getRowCount() {
    return job.getJobAttempt().getDetails().getOutputRecords();
  }


  /**
   * Serializer for Query data
   */
  public static class JobDataSerializer extends JsonSerializer<JobData> {
    private void writeField(Field field, JsonGenerator generator, boolean skipName) throws IOException {
      APIFieldDescriber.FieldDescriber describer = new APIFieldDescriber.FieldDescriber(generator, field, false);
      field.getType().accept(describer);
    }

    @Override
    public void serialize(JobData jobData, JsonGenerator generator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
      generator.writeStartObject();

      generator.writeFieldName("rowCount");
      generator.writeNumber(jobData.getRowCount());

      generator.writeFieldName("schema");

      generator.writeStartArray();

      for (Field field : jobData.getJobDataFragment().getSchema().getFields()) {
        writeField(field, generator, false);
      }

      generator.writeEndArray();

      generator.writeFieldName("rows");
      generator.writeStartArray();
      final boolean convertNumbersToStrings = DataJsonOutput.isNumberAsString(serializerProvider);

      final APIJobResultsSerializer jsonWriter = new APIJobResultsSerializer(generator, convertNumbersToStrings);
      jsonWriter.setup();

      for(RecordBatchHolder batchHolder : jobData.delegate.getRecordBatches()) {
        final EventBasedRecordWriter recordWriter =
          new EventBasedRecordWriter(batchHolder.getData().getContainer(), jsonWriter);

        for (int i = batchHolder.getStart(); i < batchHolder.getEnd(); i++) {
          recordWriter.writeOneRecord(i);
        }
      }

      generator.writeEndArray();
      generator.writeEndObject();
    }
  }

  /**
   * Job Data Results
   */
  public static class JobDataResults {
    private long rowCount;
    private JsonNode schema;
    private JsonNode rows;

    public JobDataResults() {
    }

    public long getRowCount() {
      return rowCount;
    }

    public void setRowCount(long rowCount) {
      this.rowCount = rowCount;
    }

    public JsonNode getSchema() {
      return schema;
    }

    public void setSchema(JsonNode schema) {
      this.schema = schema;
    }

    public JsonNode getRows() {
      return rows;
    }

    public void setRows(JsonNode rows) {
      this.rows = rows;
    }
  }
}

