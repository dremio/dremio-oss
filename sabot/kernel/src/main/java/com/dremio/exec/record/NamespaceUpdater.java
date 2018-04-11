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
package com.dremio.exec.record;

import java.util.ConcurrentModificationException;
import java.util.List;

import com.dremio.sabot.driver.SchemaChangeListener;
import com.dremio.sabot.driver.SchemaChangeMutator;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

public class NamespaceUpdater implements SchemaChangeListener {

  private final NamespaceService namespace;

  public NamespaceUpdater(NamespaceService namespace) {
    this.namespace = namespace;
  }

  @Override
  public void observedSchemaChange(List<String> key, BatchSchema expectedSchema, BatchSchema newlyObservedSchema, SchemaChangeMutator schemaChangeMutator) {
    NamespaceKey namespaceKey = new NamespaceKey(key);

    boolean success;
    try {
      do {
        DatasetConfig oldConfig = namespace.getDataset(namespaceKey);
        DatasetConfig newConfig = schemaChangeMutator.updateForSchemaChange(oldConfig, expectedSchema, newlyObservedSchema);
        success = storeSchema(namespaceKey, newConfig);
      } while (!success);
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean storeSchema(NamespaceKey key, DatasetConfig config) throws NamespaceException {
    try {
      namespace.addOrUpdateDataset(key, config);
      return true;
    } catch (final ConcurrentModificationException ex) {
      return false;
    }
  }
}
