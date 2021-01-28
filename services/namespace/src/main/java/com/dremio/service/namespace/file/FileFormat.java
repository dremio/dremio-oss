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
package com.dremio.service.namespace.file;

import static java.lang.String.format;

import java.util.List;

import com.dremio.common.utils.SqlUtils;
import com.dremio.service.namespace.file.proto.AvroFileConfig;
import com.dremio.service.namespace.file.proto.DeltalakeFileConfig;
import com.dremio.service.namespace.file.proto.ExcelFileConfig;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.HttpLogFileConfig;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.dremio.service.namespace.file.proto.UnknownFileConfig;
import com.dremio.service.namespace.file.proto.XlsFileConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import io.protostuff.ByteString;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;

/**
 * Format settings for a file along with the data and transformations.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  @Type(value = TextFileConfig.class, name = "Text"),
  @Type(value = HttpLogFileConfig.class, name = "HttpLog"),
  @Type(value = JsonFileConfig.class, name = "JSON"),
  @Type(value = AvroFileConfig.class, name = "Avro"),
  @Type(value = ParquetFileConfig.class, name = "Parquet"),
  @Type(value = IcebergFileConfig.class, name = "Iceberg"),
  @Type(value = ExcelFileConfig.class, name = "Excel"),
  @Type(value = XlsFileConfig.class, name = "XLS"),
  @Type(value = DeltalakeFileConfig.class, name = "Delta"),
  @Type(value = UnknownFileConfig.class, name = "Unknown")
})
public abstract class FileFormat {

  @JsonIgnore
  private final LinkedBuffer buffer = LinkedBuffer.allocate(512);

  private static final BiMap<String, FileType> extensionToType = new ImmutableBiMap.Builder<String, FileType>()
    .put("csv", FileType.CSV)
    .put("tsv", FileType.TSV)
    .put("txt", FileType.TEXT)
    .put("psv", FileType.PSV)
    .put("json", FileType.JSON)
    .put("avro", FileType.AVRO)
    .put("parquet", FileType.PARQUET)
    .put("iceberg", FileType.ICEBERG)
    .put("delta", FileType.DELTA)
    .put("xlsx", FileType.EXCEL)
    .put("xls", FileType.XLS)
    .put("unknown", FileType.UNKNOWN)
    .put("dremarrow1", FileType.ARROW)
    .put("log", FileType.HTTP_LOG)
    .build();

  private String name;
  private String owner;
  private List<String> fullPath;
  private long ctime;
  private String version;
  private boolean isFolder = false;
  private String location;

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing object.", e);
    }
  }

  private static String singleChar(String in) {
    return in.substring(0, 1);
  }

  public String toTableOptions() throws IllegalArgumentException {
    final StringBuilder stringBuilder = new StringBuilder();
    switch (getFileType()) {
      case TEXT:
      case CSV:
      case TSV:
      case PSV:
        final TextFileConfig textFileConfig = (TextFileConfig)this;
        stringBuilder.append("type => 'text', ");
        stringBuilder.append(format("fieldDelimiter => %s, ", SqlUtils.stringLiteral(textFileConfig.getFieldDelimiter())));
        stringBuilder.append(format("comment => %s, ", SqlUtils.stringLiteral(singleChar(textFileConfig.getComment()))));
        // escape is special word needs quotes around it.
        stringBuilder.append(format("%1$sescape%1$s => %2$s, ", SqlUtils.QUOTE, SqlUtils.stringLiteral(singleChar(textFileConfig.getEscape()))));
        stringBuilder.append(format("quote => %s, ", SqlUtils.stringLiteral(singleChar(textFileConfig.getQuote()))));
        stringBuilder.append(format("lineDelimiter => %s, ", SqlUtils.stringLiteral(textFileConfig.getLineDelimiter())));
        stringBuilder.append(format("extractHeader => %s, ", textFileConfig.getExtractHeader().toString()));
        stringBuilder.append(format("skipFirstLine => %s, ", textFileConfig.getSkipFirstLine().toString()));
        stringBuilder.append(format("autoGenerateColumnNames => %s, ", textFileConfig.getAutoGenerateColumnNames().toString()));
        stringBuilder.append(format("trimHeader => %s", textFileConfig.getTrimHeader().toString()));
        return stringBuilder.toString();

      case JSON:
        return "type => 'json'";

      case PARQUET:
        return "type => 'parquet'";

      case ICEBERG:
        return "type => 'iceberg'";

      case AVRO:
        return "type => 'avro'";

      case DELTA:
        return "type => 'delta'";

      case HTTP_LOG:
      case UNKNOWN:
        throw new UnsupportedOperationException("HTTP LOG and UNKNOWN file formats are not supported");

      case EXCEL: {
        final ExcelFileConfig excelFileConfig = (ExcelFileConfig) this;
        stringBuilder.append("type => 'excel', ");
        if (excelFileConfig.getSheetName() != null) {
          stringBuilder.append(format("sheet => %s, ", SqlUtils.stringLiteral(excelFileConfig.getSheetName())));
        }
        stringBuilder.append(format("extractHeader => %s, ", excelFileConfig.getExtractHeader().toString()));
        stringBuilder.append(format("hasMergedCells => %s, ", excelFileConfig.getHasMergedCells().toString()));
        stringBuilder.append(format("xls => false "));
        return stringBuilder.toString();
      }
      case XLS: {
        final XlsFileConfig xlsFileConfig = (XlsFileConfig) this;
        stringBuilder.append("type => 'excel', ");
        if (xlsFileConfig.getSheetName() != null) {
          stringBuilder.append(format("sheet => %s, ", SqlUtils.stringLiteral(xlsFileConfig.getSheetName())));
        }
        stringBuilder.append(format("extractHeader => %s, ", xlsFileConfig.getExtractHeader().toString()));
        stringBuilder.append(format("hasMergedCells => %s, ", xlsFileConfig.getHasMergedCells().toString()));
        stringBuilder.append(format("xls => true "));
        return stringBuilder.toString();
      }

      default:
        throw new IllegalArgumentException("Invalid file format type " + getFileType());
    }
  }

  public String getName() {
    return name;
  }

  public String getOwner() {
    return owner;
  }

  public boolean getIsFolder() {
    return isFolder;
  }

  public void setIsFolder(boolean isFolder) {
    this.isFolder = isFolder;
  }

  public String getLocation() {
    return location;
  }

  public List<String> getFullPath() {
    return fullPath;
  }

  public long getCtime() {
    return ctime;
  }

  public String getVersion() {
    return version;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public void setFullPath(List<String> fullPath) {
    this.fullPath = fullPath;
  }

  public void setCtime(long ctime) {
    this.ctime = ctime;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @JsonIgnore
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public FileConfig asFileConfig() {
    buffer.clear();
    FileConfig fc = new FileConfig();
    fc.setType(getFileType());
    fc.setName(name);
    fc.setOwner(owner);
    fc.setCtime(ctime);
    fc.setType(getFileType());
    fc.setTag(getVersion());
    fc.setLocation(location);
    byte[] bytes = ProtobufIOUtil.toByteArray(this, (Schema) getPrivateSchema(), buffer);
    fc.setExtendedConfig(ByteString.copyFrom(bytes));
    fc.setFullPathList(fullPath);
    return fc;
  }

  @JsonIgnore
  public FileType getFileType() {
    return FileFormatDefinitions.FILE_TYPES.get(this.getClass());
  }

  @JsonIgnore
  private Schema<?> getPrivateSchema() {
    return FileFormatDefinitions.SCHEMAS.get(this.getClass());
  }

  public static FileFormat getForFile(FileConfig fileConfig) {
    final FileFormat fileFormat = get(fileConfig);
    fileFormat.setIsFolder(false);
    return fileFormat;
  }

  public static FileFormat getForFolder(FileConfig fileConfig) {
    final FileFormat fileFormat = get(fileConfig);
    fileFormat.setIsFolder(true);
    return fileFormat;
  }

  private static FileFormat get(FileConfig fileConfig) {
    // TODO (Amit H) Remove after defining classes for tsv, csv, and psv
    FileType fileType = fileConfig.getType();
    if (fileType == FileType.CSV || fileType == FileType.TSV || fileType == FileType.PSV) {
      fileType = FileType.TEXT;
    }
    final Class<? extends FileFormat> fileFormatClass = FileFormatDefinitions.CLASS_TYPES.get(fileType);
    final Schema<FileFormat> schema = (Schema<FileFormat>) FileFormatDefinitions.SCHEMAS.get(fileFormatClass);

    final FileFormat fileFormat = schema.newMessage();
    if (fileConfig.getExtendedConfig() != null) {
      ProtobufIOUtil.mergeFrom(fileConfig.getExtendedConfig().toByteArray(), fileFormat, schema);
    }

    fileFormat.setCtime(fileConfig.getCtime());
    fileFormat.setName(fileConfig.getName());
    fileFormat.setOwner(fileConfig.getOwner());
    fileFormat.setFullPath(fileConfig.getFullPathList());
    fileFormat.setVersion(fileConfig.getTag());
    fileFormat.setLocation(fileConfig.getLocation());
    return fileFormat;
  }

  public static FileFormat getEmptyConfig(FileType type) {
    switch (type) {
      case TEXT:
      case CSV:
      case TSV:
      case PSV:
        return new TextFileConfig();
      case JSON:
        return new JsonFileConfig();
      case PARQUET:
        return new ParquetFileConfig();
      case ICEBERG:
        return new IcebergFileConfig();
      case AVRO:
        return new AvroFileConfig();
      case HTTP_LOG:
        return new HttpLogFileConfig();
      case EXCEL:
        return new ExcelFileConfig();
      case XLS:
        return new XlsFileConfig();
      case UNKNOWN:
        break;
      default:
        break;
    }
    return null;
  }

  public static String getExtension(FileType type) {
    final String extension = extensionToType.inverse().get(type);
    if (extension == null) {
      throw new IllegalArgumentException("file extension not found for file type " + type);
    }
    return extension;
  }

  public static FileType getFileFormatType(List<String> extensions) {
    if (extensions == null || extensions.isEmpty()) {
      return FileType.UNKNOWN;
    }
    final FileType fileType = extensionToType.get(extensions.get(0));
    return fileType == null? FileType.UNKNOWN: fileType;
  }
}
