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
package com.dremio.dac.explore.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.util.JSONUtil;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.service.job.proto.JobId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer.None;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Throwables;

/**
 * POJO version of {@link JobData} for deserializing from JSON value. This class is for testing purposes only to
 * deserialize the result output from server. Server has its own custom serializer and doesn't have the
 * equivalent custom deserializer.
 */
@JsonSerialize(using=None.class) // override super class custom serializer with default one
public class DataPOJO implements JobDataFragment {

  private final List<Column> columns;
  private final int returnedRowCount;
  private final List<RowPOJO> rows;

  // transient map to avoid constantly searching linear list.
  private Map<String, Column> nameToColumn;

  @JsonCreator
  public DataPOJO(
      @JsonProperty("columns") List<Column> columns,
      @JsonProperty("returnedRowCount") int returnedRowCount,
      @JsonProperty("rows") List<RowPOJO> rows) {
    this.columns = columns;
    this.returnedRowCount = returnedRowCount;
    this.rows = rows;
  }

  @Override
  public JobId getJobId() {
    return null;
  }

  public List<RowPOJO> getRows() {
    return rows;
  }

  @Override
  public List<Column> getColumns() {
    return columns;
  }

  @Override
  public List<Field> getFields() {
    return Collections.emptyList();  }

  @Override
  public List<RecordBatchHolder> getRecordBatches() {
    return Collections.emptyList();
  }

  @Override
  public int getReturnedRowCount() {
    return returnedRowCount;
  }

  @Override
  public Column getColumn(String name) {
    if(nameToColumn == null){
      nameToColumn = new HashMap<>();
      for(int i =0; i < columns.size(); i++){
        Column c = columns.get(i);
        nameToColumn.put(columns.get(i).getName(), c);
      }
    }
    return nameToColumn.get(name);
  }

  @Override
  public String extractString(String column, int index){
    final Object obj = extractValue(column, index);
    if(obj != null){
      if (obj instanceof Map || obj instanceof List) {
        try {
          return JSONUtil.mapper().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
          Throwables.propagate(e);
        }
      }
      return obj.toString();
    }
    return null;
  }

  @Override
  public Object extractValue(String column, int index){
    final RowPOJO row = rows.get(index);
    return row.getCell(getColumn(column)).getValue();
  }

  public String extractUrl(String column, int index) {
    final RowPOJO row = rows.get(index);
    return row.getCell(getColumn(column)).getUrl();
  }

  @Override
  public DataType extractType(String column, int index){
    final RowPOJO row = rows.get(index);
    return row.getCell(getColumn(column)).getType();
  }

  @Override
  public synchronized void close() {
    throw new UnsupportedOperationException("not supported in pojo class");
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }
}
