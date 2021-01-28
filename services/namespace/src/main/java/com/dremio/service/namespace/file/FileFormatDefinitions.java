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

import java.util.Map;

import com.dremio.service.namespace.file.proto.ArrowFileConfig;
import com.dremio.service.namespace.file.proto.AvroFileConfig;
import com.dremio.service.namespace.file.proto.DeltalakeFileConfig;
import com.dremio.service.namespace.file.proto.ExcelFileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.HttpLogFileConfig;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.dremio.service.namespace.file.proto.UnknownFileConfig;
import com.dremio.service.namespace.file.proto.XlsFileConfig;
import com.google.common.collect.ImmutableMap;

import io.protostuff.Schema;

/**
 * FileFormats type/schema mappings.
 */
public class FileFormatDefinitions {
  static final ImmutableMap<Class<? extends FileFormat>, FileType> FILE_TYPES;
  static final ImmutableMap<FileType, Class<? extends FileFormat>> CLASS_TYPES;
  static final ImmutableMap<Class<? extends FileFormat>, Schema<? extends FileFormat>> SCHEMAS;

  static {
    ImmutableMap.Builder<Class<? extends FileFormat>, FileType> types = ImmutableMap.builder();
    ImmutableMap.Builder<Class<? extends FileFormat>, Schema<? extends FileFormat>> schemas = ImmutableMap.builder();

    types.put(TextFileConfig.class, FileType.TEXT);
    schemas.put(TextFileConfig.class, TextFileConfig.getSchema());

    types.put(HttpLogFileConfig.class, FileType.HTTP_LOG);
    schemas.put(HttpLogFileConfig.class, HttpLogFileConfig.getSchema());

    types.put(JsonFileConfig.class, FileType.JSON);
    schemas.put(JsonFileConfig.class, JsonFileConfig.getSchema());

    types.put(AvroFileConfig.class, FileType.AVRO);
    schemas.put(AvroFileConfig.class, AvroFileConfig.getSchema());

    types.put(ParquetFileConfig.class, FileType.PARQUET);
    schemas.put(ParquetFileConfig.class, ParquetFileConfig.getSchema());

    types.put(ExcelFileConfig.class, FileType.EXCEL);
    schemas.put(ExcelFileConfig.class, ExcelFileConfig.getSchema());

    types.put(XlsFileConfig.class, FileType.XLS);
    schemas.put(XlsFileConfig.class, XlsFileConfig.getSchema());

    types.put(ArrowFileConfig.class, FileType.ARROW);
    schemas.put(ArrowFileConfig.class, ArrowFileConfig.getSchema());

    types.put(UnknownFileConfig.class, FileType.UNKNOWN);
    schemas.put(UnknownFileConfig.class, UnknownFileConfig.getSchema());

    types.put(IcebergFileConfig.class, FileType.ICEBERG);
    schemas.put(IcebergFileConfig.class, IcebergFileConfig.getSchema());

    types.put(DeltalakeFileConfig.class, FileType.DELTA);
    schemas.put(DeltalakeFileConfig.class, DeltalakeFileConfig.getSchema());

    FILE_TYPES = types.build();
    SCHEMAS = schemas.build();

    ImmutableMap.Builder<FileType, Class<? extends FileFormat>> classes = ImmutableMap.builder();
    for (Map.Entry<Class<? extends FileFormat>, FileType> entry : FILE_TYPES.entrySet()) {
      classes.put(entry.getValue(), entry.getKey());
    }
    CLASS_TYPES = classes.build();
  }
}
