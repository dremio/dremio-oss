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
package com.dremio.exec.planner.serializer.logical;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTranslatableTable;
import com.dremio.exec.planner.serialization.DeserializationException;
import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.plan.serialization.PScanCrel;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Strings;

/** Serde for ScanCrel */
public final class ScanCrelSerde implements RelNodeSerde<ScanCrel, PScanCrel> {
  @Override
  public PScanCrel serialize(ScanCrel scan, RelToProto s) {
    if (!scan.isDirectNamespaceDescendent()) {
      throw new IllegalStateException("You can only serialize direct namespace descendents.");
    }

    if (!scan.isSubstitutable()) {
      throw new IllegalStateException("Non-substitutable scan cannot be serialized.");
    }

    PScanCrel.Builder builder = PScanCrel.newBuilder();
    if (scan.getTableMetadata().getVersion() != null) {
      builder.setDatasetVersion(scan.getTableMetadata().getVersion());
    }
    if (scan.getTableMetadata().getVersionContext() != null) {
      builder.setVersionContext(scan.getTableMetadata().getVersionContext().serialize());
    }
    return builder.addAllPath(scan.getTableMetadata().getName().getPathComponents()).build();
  }

  @Override
  public ScanCrel deserialize(PScanCrel node, RelFromProto s) {
    if (Strings.isNullOrEmpty(node.getVersionContext())) {
      DremioPrepareTable table = s.tables().getTable(new NamespaceKey(node.getPathList()));
      if (table == null) {
        throw new DeserializationException(
            "Table no longer exists in source: " + node.getPathList());
      }
      return (ScanCrel) table.toRel(s.toRelContext());
    } else {
      CatalogEntityKey catalogEntityKey =
          CatalogEntityKey.newBuilder()
              .keyComponents(node.getPathList())
              .tableVersionContext(TableVersionContext.deserialize(node.getVersionContext()))
              .build();
      DremioTranslatableTable table = s.tables().getTableSnapshot(catalogEntityKey);
      if (table == null) {
        throw new DeserializationException(
            "Table no longer exists in source: " + node.getPathList());
      }
      return (ScanCrel) table.toRel(s.toRelContext(), null);
    }
  }
}
