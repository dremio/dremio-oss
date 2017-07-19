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
package com.dremio.exec.planner.sql.handlers.direct;

import static com.dremio.exec.planner.sql.SchemaUtilities.findSchema;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.planner.logical.TableBase;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.parser.SqlRefreshMetadata;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemReadEntry;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.NamedFormatPluginConfig;
import com.dremio.exec.store.parquet.Metadata;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.work.foreman.ForemanSetupException;

public class RefreshMetadataHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RefreshMetadataHandler.class);

  private final SchemaPlus defaultSchema;

  public RefreshMetadataHandler(SchemaPlus defaultSchema) {
    this.defaultSchema = defaultSchema;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode)
      throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    final SqlRefreshMetadata refreshTable = SqlNodeUtil.unwrap(sqlNode, SqlRefreshMetadata.class);

    final SchemaPlus schema = findSchema(defaultSchema, refreshTable.getSchemaPath());

    if (schema == null) {
      throw UserException.validationError().message("Storage plugin or workspace does not exist [%s]",
          SchemaUtilities.SCHEMA_PATH_JOINER.join(refreshTable.getSchemaPath())).build(logger);
    }

    final String tableName = refreshTable.getName();

    if (tableName.contains("*") || tableName.contains("?")) {
      throw UserException.validationError().message("Glob path %s not supported for metadata refresh", tableName)
          .build(logger);
    }

    final Table table = schema.getTable(tableName);

    if (table == null) {
      throw UserException.validationError().message("Table %s does not exist.", tableName).build(logger);
    }

    if (!(table instanceof TableBase)) {
      throwUnsupportedException(tableName);
    }

    final TableBase tableBase = (TableBase) table;

    final Object selection = tableBase.getSelection();
    if (!(selection instanceof FileSystemReadEntry)) {
      throwUnsupportedException(tableName);
    }

    FileSystemReadEntry fileSystemReadEntry = (FileSystemReadEntry) selection;

    FormatPluginConfig formatConfig = fileSystemReadEntry.getFormat();
    if (!((formatConfig instanceof ParquetFormatConfig) || ((formatConfig instanceof NamedFormatPluginConfig)
        && ((NamedFormatPluginConfig) formatConfig).name.equals("parquet")))) {
      throwUnsupportedException(tableName);
    }

    FileSystemPlugin plugin = (FileSystemPlugin) tableBase.getPlugin();
    try (FileSystemWrapper fs = new FileSystemWrapper(plugin.getFsConf())) {
      String selectionRoot = fileSystemReadEntry.getSelection().selectionRoot;
      if (!fs.getFileStatus(new Path(selectionRoot)).isDirectory()) {
        throwUnsupportedException(tableName);
      }

      if (!(formatConfig instanceof ParquetFormatConfig)) {
        formatConfig = new ParquetFormatConfig();
      }
      Metadata.createMeta(fs, selectionRoot, (ParquetFormatConfig) formatConfig, plugin.getFsConf());
    }

    return Collections.singletonList(
        SimpleCommandResult.successful("Successfully updated metadata for table %s.", refreshTable.getName()));
  }

  private void throwUnsupportedException(String tbl){
    throw UserException.unsupportedError()
        .message("Table %s does not support metadata refresh. " +
            "Support is currently limited to directory-based Parquet tables.", tbl)
        .build(logger);
  }
}
