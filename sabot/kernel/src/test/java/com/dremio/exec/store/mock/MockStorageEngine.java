/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.dremio.common.JSONOptions;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.physical.base.OldAbstractGroupScan;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.ConversionContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.mock.MockGroupScanPOP.MockScanEntry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MockStorageEngine extends AbstractStoragePlugin<ConversionContext> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockStorageEngine.class);

  private final MockStorageEngineConfig configuration;

  public MockStorageEngine(MockStorageEngineConfig configuration, SabotContext context, String name) {
    this.configuration = configuration;
  }

  @Override
  public OldAbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<String> tableSchemaPath, List<SchemaPath> columns)
      throws IOException {

    ArrayList<MockScanEntry> readEntries = selection.getListWith(new ObjectMapper(),
        new TypeReference<ArrayList<MockScanEntry>>() {
        });

    return new MockGroupScanPOP(null, readEntries);
  }

  @Override
  public StoragePluginConfig getConfig() {
    return configuration;
  }

  @Override
  public boolean folderExists(SchemaConfig schemaConfig, List folderPath) throws IOException {
    return false;
  }

}
