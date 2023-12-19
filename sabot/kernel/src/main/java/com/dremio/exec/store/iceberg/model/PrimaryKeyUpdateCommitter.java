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
package com.dremio.exec.store.iceberg.model;

import static com.dremio.exec.store.dfs.PrimaryKeyOperations.DREMIO_PRIMARY_KEY;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Iceberg Op committer for primary key command
 */
public class PrimaryKeyUpdateCommitter implements IcebergOpCommitter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PrimaryKeyUpdateCommitter.class);

  private final IcebergCommand command;
  private final List<Field> columns;

  private Table icebergTable;
  private String rootPointer;
  private Map<Integer, PartitionSpec> specMap;
  private Schema icebergSchema;

  public PrimaryKeyUpdateCommitter(IcebergCommand icebergCommand, List<Field> columns) {
    this.command = icebergCommand;
    this.columns = columns;
    command.beginTransaction();
  }

  @Override
  public Snapshot commit() {
    command.updateProperties(getPropertiesMap(columns), true);
    icebergTable = command.endTransaction();
    rootPointer = command.getRootPointer();
    specMap = command.getPartitionSpecMap();
    icebergSchema = icebergTable.schema();
    return icebergTable.currentSnapshot();
  }

  @Override
  public void consumeDeleteDataFile(DataFile icebergDeleteDatafile) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("consumeDeleteDataFile is not supported for PrimaryKeyUpdateCommitter");
  }

  @Override
  public void consumeDeleteDataFilePath(String icebergDeleteDatafilePath) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("consumeDeleteDataFilePath is not supported for PrimaryKeyUpdateCommitter");
  }

  @Override
  public void consumeManifestFile(ManifestFile manifestFile) {
    throw new UnsupportedOperationException("consumeManifestFile is not supported for PrimaryKeyUpdateCommitter");
  }

  @Override
  public void updateSchema(BatchSchema newSchema) {
    throw new UnsupportedOperationException("updateSchema is not supported for PrimaryKeyUpdateCommitter");
  }

  @Override
  public String getRootPointer() {
    return rootPointer;
  }

  @Override
  public Map<Integer, PartitionSpec> getCurrentSpecMap() {
    return specMap;
  }

  @Override
  public Schema getCurrentSchema() {
    return icebergSchema;
  }

  @Override
  public boolean isIcebergTableUpdated() {
    return false;
  }

  public Table getIcebergTable() {
    return icebergTable;
  }

  protected static Map<String, String> getPropertiesMap(List<Field> columns) {
    BatchSchema batchSchema = new BatchSchema(columns);
    Map<String, String> propertiesMap = new HashMap<>();
    ObjectMapper mapper = new ObjectMapper();
    String columnsJson;
    try {
      columnsJson = mapper.writeValueAsString(batchSchema);
    } catch (JsonProcessingException e) {
      String error = "Unexpected error occurred while serializing primary keys in json string. ";
      logger.error(error, e);
      throw UserException.dataWriteError(e).addContext(error).build(logger);
    }
    propertiesMap.put(DREMIO_PRIMARY_KEY, columnsJson);
    return propertiesMap;
  }
}
