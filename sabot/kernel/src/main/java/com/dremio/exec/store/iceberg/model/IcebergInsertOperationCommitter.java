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

import com.google.common.base.Preconditions;

/**
 * Class used to commit insert into table operation
 */
public class IcebergInsertOperationCommitter implements IcebergOpCommitter {
  private List<ManifestFile> manifestFileList = new ArrayList<>();

  private final IcebergCommand icebergCommand;

  public IcebergInsertOperationCommitter(IcebergCommand icebergCommand) {
    Preconditions.checkState(icebergCommand != null, "Unexpected state");
    this.icebergCommand = icebergCommand;
    this.icebergCommand.beginInsertTableTransaction();
  }

  @Override
  public void commit() {
    if (manifestFileList.size() > 0) {
      icebergCommand.beginInsert();
      icebergCommand.consumeManifestFiles(manifestFileList);
      icebergCommand.finishInsert();
    }
    icebergCommand.endInsertTableTransaction();
  }

  @Override
  public void consumeManifestFile(ManifestFile icebergManifestFile) {
    manifestFileList.add(icebergManifestFile);
  }

  @Override
  public void consumeDeleteDataFile(DataFile icebergDeleteDatafile) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Delete data file Operation is not allowed for Insert Transaction");
  }
}
