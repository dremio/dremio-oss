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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.schema.Function;

import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SupportsExternalQuery;
import com.dremio.exec.tablefunctions.ExternalQuery;
import com.dremio.exec.tablefunctions.ExternalQueryScanCrel;
import com.dremio.plan.serialization.PExternalQueryScanCrel;
import com.dremio.service.users.SystemUser;

/**
 * Serde for ScanCrel
 */
public final class ExternalQueryScanCrelSerde implements RelNodeSerde<ExternalQueryScanCrel, PExternalQueryScanCrel> {
  @Override
  public PExternalQueryScanCrel serialize(ExternalQueryScanCrel scan, RelToProto s) {
    return PExternalQueryScanCrel.newBuilder()
        .setPluginName(scan.getPluginId().getName())
        .setSql(scan.getSql())
        .build();
  }

  @Override
  public ExternalQueryScanCrel deserialize(PExternalQueryScanCrel node, RelFromProto s) {
    List<Function> functions = s.plugins().getPlugin(node.getPluginName()).getFunctions(Arrays.asList(node.getPluginName(),
      SupportsExternalQuery.EXTERNAL_QUERY),
      SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build());
    return (ExternalQueryScanCrel) ((ExternalQuery) functions.get(0))
      .apply(Collections.singletonList(node.getSql())).toRel(s.toRelContext(), null);
  }
}
