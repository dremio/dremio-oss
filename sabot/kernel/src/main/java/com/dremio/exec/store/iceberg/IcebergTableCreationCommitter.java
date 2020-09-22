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

import java.util.List;

import org.apache.iceberg.DataFile;

/**
 * Class used to commit CTAS operation
 */
public class IcebergTableCreationCommitter implements IcebergOpCommitter{

  private IcebergOperation icebergOperation;
  IcebergTableCreationCommitter(IcebergOperation icebergOperation) {
    this.icebergOperation = icebergOperation;
  }

  @Override
  public void commit() {
    icebergOperation.commit();
  }

  @Override
  public void consumeData(List<DataFile> dataFiles) {
    icebergOperation.consumeData(dataFiles);
  }
}
