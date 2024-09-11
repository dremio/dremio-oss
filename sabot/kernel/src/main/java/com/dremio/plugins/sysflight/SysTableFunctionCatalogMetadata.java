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
package com.dremio.plugins.sysflight;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.proto.FlightProtos.SysTableFunction;
import com.dremio.exec.record.BatchSchema;
import java.util.List;

public class SysTableFunctionCatalogMetadata {
  private final BatchSchema batchSchema;
  private final StoragePluginId storagePluginId;
  private final List<String> functionPath;
  private final SysTableFunction sysTableFunction;

  public SysTableFunctionCatalogMetadata(
      BatchSchema batchSchema,
      StoragePluginId storagePluginId,
      List<String> functionPath,
      SysTableFunction sysTableFunction) {
    this.batchSchema = batchSchema;
    this.storagePluginId = storagePluginId;
    this.functionPath = functionPath;
    this.sysTableFunction = sysTableFunction;
  }

  public List<String> getFunctionPath() {
    return functionPath;
  }

  public StoragePluginId getStoragePluginId() {
    return storagePluginId;
  }

  public SysTableFunction getSysTableFunction() {
    return sysTableFunction;
  }

  public BatchSchema getBatchSchema() {
    return batchSchema;
  }
}
