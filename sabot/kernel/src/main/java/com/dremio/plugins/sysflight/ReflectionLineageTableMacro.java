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

import static com.dremio.exec.catalog.CatalogServiceImpl.SYSTEM_TABLE_SOURCE_NAME;

import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;

public class ReflectionLineageTableMacro implements TableMacro {
  public static final String NAME = "reflection_lineage";
  public static final List<String> REFLECTION_LINEAGE_PATH =
      ImmutableList.of(SYSTEM_TABLE_SOURCE_NAME.toLowerCase(Locale.ROOT), NAME);
  private final ManagedStoragePlugin storagePlugin;
  private final String user;
  private static final List<FunctionParameter> FUNCTION_PARAMS =
      new ReflectiveFunctionBase.ParameterListBuilder().add(String.class, "reflection_id").build();

  public ReflectionLineageTableMacro(ManagedStoragePlugin storagePlugin, String user) {
    this.storagePlugin = storagePlugin;
    this.user = user;
  }

  @Override
  public TranslatableTable apply(List<?> arguments) {
    final String reflectionId = (String) arguments.get(0);
    return ReflectionLineageTable.create(storagePlugin, reflectionId, user);
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return FUNCTION_PARAMS;
  }

  public static boolean isReflectionLineageFunction(NamespaceKey path) {
    return REFLECTION_LINEAGE_PATH.equals(
        path.getPathComponents().stream()
            .map(s -> s.toLowerCase(Locale.ROOT))
            .collect(Collectors.toList()));
  }
}
