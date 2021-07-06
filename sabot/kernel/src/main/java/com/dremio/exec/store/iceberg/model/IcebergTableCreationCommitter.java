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

import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;

import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;

/**
 * Class used to commit CTAS operation
 */
public class IcebergTableCreationCommitter implements IcebergOpCommitter {
  private List<ManifestFile> manifestFileList = new ArrayList<>();
  private final IcebergCommand icebergCommand;

  public IcebergTableCreationCommitter(String tableName, BatchSchema batchSchema, List<String> partitionColumnNames, IcebergCommand icebergCommand) {
    Preconditions.checkState(icebergCommand != null, "Unexpected state");
    Preconditions.checkState(batchSchema != null, "Schema must be present");
    Preconditions.checkState(tableName != null, "Table name must be present");
    this.icebergCommand = icebergCommand;
    this.icebergCommand.beginCreateTableTransaction(tableName, batchSchema, partitionColumnNames);
  }

  @Override
  public void commit() {
    icebergCommand.beginInsert();
    icebergCommand.consumeManifestFiles(manifestFileList);
    icebergCommand.finishInsert();
    icebergCommand.endCreateTableTransaction();
  }

  @Override
  public void consumeManifestFile(ManifestFile icebergManifestFile) {
    manifestFileList.add(icebergManifestFile);
  }

  @Override
  public void consumeDeleteDataFile(DataFile icebergDeleteDatafile) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Delete data file operation not allowed in Create table Transaction");
  }
}
