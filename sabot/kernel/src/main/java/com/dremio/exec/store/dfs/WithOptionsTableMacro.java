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
package com.dremio.exec.store.dfs;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.MaterializedDatasetTableProvider;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.TableInstance;
import com.google.common.base.Throwables;

/**
 * Implementation of a table macro that generates a table based on parameters
 */
public final class WithOptionsTableMacro implements TableMacro {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WithOptionsTableMacro.class);

  private final List<String> tableSchemaPath;
  private final TableInstance.TableSignature sig;
  private final FileSystemPlugin<?> plugin;
  private final SchemaConfig schemaConfig;

  WithOptionsTableMacro(
    List<String> tableSchemaPath,
    TableInstance.TableSignature sig,
    FileSystemPlugin<?> plugin,
    SchemaConfig schemaConfig
  ) {
    this.tableSchemaPath = tableSchemaPath;
    this.sig = sig;
    this.plugin = plugin;
    this.schemaConfig = schemaConfig;
  }

  @Override
  public List<FunctionParameter> getParameters() {
    List<FunctionParameter> result = new ArrayList<>();
    for (int i = 0; i < sig.getParams().size(); i++) {
      final TableInstance.TableParamDef p = sig.getParams().get(i);
      final int ordinal = i;
      result.add(new FunctionParameter() {
        @Override
        public int getOrdinal() {
          return ordinal;
        }

        @Override
        public String getName() {
          return p.getName();
        }

        @Override
        public RelDataType getType(RelDataTypeFactory typeFactory) {
          return typeFactory.createJavaType(p.getType());
        }

        @Override
        public boolean isOptional() {
          return p.isOptional();
        }
      });
    }
    return result;
  }

  @Override
  public TranslatableTable apply(final List<? extends Object> arguments) {
    try {
      final DatasetRetrievalOptions options = DatasetRetrievalOptions.DEFAULT.toBuilder()
          .setIgnoreAuthzErrors(schemaConfig.getIgnoreAuthErrors())
          .setMaxMetadataLeafColumns((int) schemaConfig.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX))
          .build();

      final FileDatasetHandle handle = plugin.getDatasetWithOptions(
          new NamespaceKey(tableSchemaPath),
          new TableInstance(sig, arguments),
          schemaConfig.getIgnoreAuthErrors(),
          schemaConfig.getUserName(),
        (int) schemaConfig.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX)
      );

      if (handle == null) {
        throw UserException.validationError()
          .message("Unable to read table %s using provided options.",
            new NamespaceKey(tableSchemaPath).toString())
          .build(logger);
      }

      return new MaterializedDatasetTableProvider(null, handle, plugin, plugin.getId(), schemaConfig, options)
        .get();
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  protected List<String> getTableSchemaPath() {
    return tableSchemaPath;
  }
}
