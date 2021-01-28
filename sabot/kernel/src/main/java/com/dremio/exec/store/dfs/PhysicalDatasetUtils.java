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
import java.util.Collections;
import java.util.List;

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.store.deltalake.DeltaLakeFormatConfig;
import com.dremio.exec.store.deltalake.DeltaLakeFormatPlugin;
import com.dremio.exec.store.easy.arrow.ArrowFormatPlugin;
import com.dremio.exec.store.easy.arrow.ArrowFormatPluginConfig;
import com.dremio.exec.store.easy.excel.ExcelFormatPlugin;
import com.dremio.exec.store.easy.excel.ExcelFormatPluginConfig;
import com.dremio.exec.store.easy.json.JSONFormatPlugin;
import com.dremio.exec.store.easy.text.TextFormatPlugin;
import com.dremio.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import com.dremio.exec.store.easy.text.compliant.TextParsingSettings;
import com.dremio.exec.store.iceberg.IcebergFormatConfig;
import com.dremio.exec.store.iceberg.IcebergFormatPlugin;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.store.parquet.ParquetFormatPlugin;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.ArrowFileConfig;
import com.dremio.service.namespace.file.proto.DeltalakeFileConfig;
import com.dremio.service.namespace.file.proto.ExcelFileConfig;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.file.proto.IcebergMetaStoreType;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.dremio.service.namespace.file.proto.UnknownFileConfig;
import com.dremio.service.namespace.file.proto.XlsFileConfig;

/**
 * Utility methods to talk to physical dataset service and convert dac format settings to Dremio format plugins.
 */
public class PhysicalDatasetUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalDatasetUtils.class);
  /**
   * Check with namespace format setting on a file/directory.
   * @param namespaceService PhysicalDatasetService namespaceService
   * @param schemaPath parent path
   * @param tableName table name (file/directory name)
   * @param fileSelection file/files selected under directory.
   * @return {@code FormatPluginConfig} that should be used for creating Dremio table.
   */
  public static FormatPluginConfig getPhysicalDatasetProperties(NamespaceService namespaceService,
                                                          final List<String> schemaPath, final String tableName,
                                                          final FileSelection fileSelection) {

    // If the execution engine is being tested outside the context of a Dremio instance, there won't be a dataset service.
    if(namespaceService == null) {
      return null;
    }

    final List<String> tableSchemaPath = new ArrayList<>(schemaPath);
    tableSchemaPath.addAll(PathUtils.toPathComponents(Path.of(tableName)));

    try {
      final DatasetConfig config = namespaceService.getDataset(new NamespaceKey(tableSchemaPath));
      return toFormatPlugin(config.getPhysicalDataset().getFormatSettings(), fileSelection.getExtensions());
    } catch (NamespaceException e) {
      logger.debug("Failed to get physical dataset properties for table {} error {}", PathUtils.constructFullPath(tableSchemaPath), e);
    }
    return null;
  }

  private static ParquetFormatConfig toParquetFormatConfig(ParquetFileConfig fileConfig) {
    ParquetFormatConfig parquetFormatConfig = new ParquetFormatConfig();
    parquetFormatConfig.autoCorrectCorruptDates = fileConfig.getAutoCorrectCorruptDates();
    return parquetFormatConfig;
  }

  /**
   * Convert file format settings from dac to Dremio.
   *
   * NOTE TO DEVELOPERS: Make sure this method in sync with the below {@link #toFileFormat} method. If a new parameter
   * is added to FileConfig, make sure the parameter is added to the FormatPluginConfig here, and vice versa in
   * {@link #toFileFormat} method.
   *
   * @param fileConfig Format settings set by user in DAC
   * @param extensions list of extensions found for a table
   * @return {@code FormatPluginConfig} that should be used for creating Dremio table.
   */
  public static FormatPluginConfig toFormatPlugin(final FileConfig fileConfig, final List<String> extensions) {
    assert extensions != null : "TextFormatConfig.extensions should never be null";

    switch (fileConfig.getType()) {
      case TEXT:
      case CSV:
      case TSV:
      case PSV:
        final TextFileConfig textFileConfig = (TextFileConfig)TextFileConfig.getForFile(fileConfig);
        final TextFormatConfig textFormatConfig = new TextFormatConfig();

        textFormatConfig.comment = textFileConfig.getComment().charAt(0);
        textFormatConfig.escape = textFileConfig.getEscape().charAt(0);
        textFormatConfig.extractHeader = textFileConfig.getExtractHeader();
        textFormatConfig.skipFirstLine = textFileConfig.getSkipFirstLine();
        textFormatConfig.fieldDelimiter = textFileConfig.getFieldDelimiter().charAt(0);
        textFormatConfig.lineDelimiter = textFileConfig.getLineDelimiter();
        textFormatConfig.quote = textFileConfig.getQuote().charAt(0);
        textFormatConfig.extensions = extensions;
        textFormatConfig.autoGenerateColumnNames = textFileConfig.getAutoGenerateColumnNames();
        textFormatConfig.trimHeader = textFileConfig.getTrimHeader();
        return textFormatConfig;
      case JSON:
        final JSONFormatPlugin.JSONFormatConfig jsonFormatConfig = new JSONFormatPlugin.JSONFormatConfig();
        jsonFormatConfig.extensions = extensions;
        return jsonFormatConfig;
      case PARQUET:
        final ParquetFileConfig parquetFileConfig = (ParquetFileConfig)com.dremio.service.namespace.file.FileFormat.getForFile(fileConfig);
        return toParquetFormatConfig(parquetFileConfig);
      case ARROW:
        return new ArrowFormatPluginConfig();
      case EXCEL: {
        final ExcelFileConfig excelFileConfig = (ExcelFileConfig) ExcelFileConfig.getForFile(fileConfig);
        final ExcelFormatPluginConfig excelFormatPluginConfig = new ExcelFormatPluginConfig();

        excelFormatPluginConfig.sheet = excelFileConfig.getSheetName();
        excelFormatPluginConfig.extractHeader = excelFileConfig.getExtractHeader();
        excelFormatPluginConfig.hasMergedCells = excelFileConfig.getHasMergedCells();
        excelFormatPluginConfig.xls = false;
        return excelFormatPluginConfig;
      }
      case XLS: {
        final XlsFileConfig xlsFileConfig = (XlsFileConfig) XlsFileConfig.getForFile(fileConfig);
        final ExcelFormatPluginConfig excelFormatPluginConfig = new ExcelFormatPluginConfig();

        excelFormatPluginConfig.sheet = xlsFileConfig.getSheetName();
        excelFormatPluginConfig.extractHeader = xlsFileConfig.getExtractHeader();
        excelFormatPluginConfig.hasMergedCells = xlsFileConfig.getHasMergedCells();
        excelFormatPluginConfig.xls = true;
        return excelFormatPluginConfig;
      }
      case HTTP_LOG:
        break;
      case ICEBERG:
        IcebergFileConfig icebergFileConfig = (IcebergFileConfig)com.dremio.service.namespace.file.FileFormat.getForFile(fileConfig);
        if (icebergFileConfig.getDataFormatTypeList() == null) {
          // preview sends an empty entry.
          icebergFileConfig.setMetaStoreType(IcebergMetaStoreType.HDFS);
          icebergFileConfig.setDataFormatTypeList(Collections.singletonList(FileType.PARQUET));
          icebergFileConfig.setParquetDataFormat(new ParquetFileConfig());
        } else if (icebergFileConfig.getDataFormatTypeList().size() != 1 ||
          icebergFileConfig.getDataFormatTypeList().get(0) != FileType.PARQUET ||
          icebergFileConfig.getMetaStoreType() != IcebergMetaStoreType.HDFS) {
          return null;
        }

        final IcebergFormatConfig icebergFormatConfig = new IcebergFormatConfig();
        icebergFormatConfig.setDataFormatType(icebergFileConfig.getDataFormatTypeList().get(0));
        icebergFormatConfig.setMetaStoreType(icebergFileConfig.getMetaStoreType());
        icebergFormatConfig.setDataFormatConfig(toParquetFormatConfig(icebergFileConfig.getParquetDataFormat()));
        return icebergFormatConfig;
      case DELTA:
        return new DeltaLakeFormatConfig();
      default:
        break;
    }
    return null;
  }

  private static ParquetFileConfig toParquetFileConfig(ParquetFormatConfig formatConfig) {
    return new ParquetFileConfig().setAutoCorrectCorruptDates(formatConfig.autoCorrectCorruptDates);
  }

  /**
   * Convert Dremio's format plugin config to dac's format settings.
   *
   * NOTE TO DEVELOPERS: Make sure this method in sync with the above {@link #toFormatPlugin} method. If a new
   * parameter is added to FormatPlugin, make sure the parameter is added to the FileFormat here, and vice versa in
   * {@link #toFormatPlugin} method.
   *
   * @param formatPlugin format plugin used to create the table
   * @return {@code FileFormat} for corresponding format plugin, null if not found.
   */
  public static FileFormat toFileFormat(FormatPlugin formatPlugin) {
    if (formatPlugin instanceof ParquetFormatPlugin) {
      ParquetFormatPlugin parquetFormatPlugin = (ParquetFormatPlugin)formatPlugin;
      return toParquetFileConfig(parquetFormatPlugin.getConfig());
    }
    if (formatPlugin instanceof JSONFormatPlugin) {
      return new JsonFileConfig();
    }
    if (formatPlugin instanceof ArrowFormatPlugin) {
      return new ArrowFileConfig();
    }
    if (formatPlugin instanceof TextFormatPlugin) {
      final TextFileConfig textFileConfig = new TextFileConfig();
      TextParsingSettings settings = new TextParsingSettings();
      settings.set((TextFormatConfig) formatPlugin.getConfig());
      textFileConfig.setComment(new Character((char) settings.getComment()).toString());
      textFileConfig.setEscape(new Character((char) settings.getQuoteEscape()).toString());
      textFileConfig.setFieldDelimiter(new Character((char) settings.getDelimiter()).toString());
      textFileConfig.setQuote(new Character((char) settings.getQuote()).toString());
      textFileConfig.setExtractHeader(settings.isHeaderExtractionEnabled());
      textFileConfig.setSkipFirstLine(settings.isSkipFirstLine());
      textFileConfig.setLineDelimiter(new String(settings.getNewLineDelimiter()));
      textFileConfig.setAutoGenerateColumnNames(settings.isAutoGenerateColumnNames());
      textFileConfig.setTrimHeader(settings.isTrimHeader());
      return textFileConfig;
    }
    if (formatPlugin instanceof ExcelFormatPlugin) {
      final ExcelFormatPluginConfig excelFormatPluginConfig = (ExcelFormatPluginConfig)formatPlugin.getConfig();
      if (excelFormatPluginConfig.xls) {
        final XlsFileConfig xlsFileConfig = new XlsFileConfig();
        xlsFileConfig.setExtractHeader(excelFormatPluginConfig.extractHeader);
        xlsFileConfig.setHasMergedCells(excelFormatPluginConfig.hasMergedCells);
        xlsFileConfig.setSheetName(excelFormatPluginConfig.sheet);
        return xlsFileConfig;
      } else {
        final ExcelFileConfig excelFileConfig = new ExcelFileConfig();
        excelFileConfig.setExtractHeader(excelFormatPluginConfig.extractHeader);
        excelFileConfig.setHasMergedCells(excelFormatPluginConfig.hasMergedCells);
        excelFileConfig.setSheetName(excelFormatPluginConfig.sheet);
        return excelFileConfig;
      }
    }
    if (formatPlugin instanceof IcebergFormatPlugin) {
      IcebergFormatPlugin icebergFormatPlugin = (IcebergFormatPlugin)formatPlugin;
      IcebergFormatConfig icebergFormatConfig = icebergFormatPlugin.getConfig();
      return new IcebergFileConfig()
        .setMetaStoreType(icebergFormatConfig.getMetaStoreType())
        .setDataFormatTypeList(Collections.singletonList(icebergFormatConfig.getDataFormatType()))
        .setParquetDataFormat(toParquetFileConfig((ParquetFormatConfig)icebergFormatConfig.getDataFormatConfig()));
    }
    if(formatPlugin instanceof DeltaLakeFormatPlugin) {
      return new DeltalakeFileConfig();
    }
    return new UnknownFileConfig();
  }
}
