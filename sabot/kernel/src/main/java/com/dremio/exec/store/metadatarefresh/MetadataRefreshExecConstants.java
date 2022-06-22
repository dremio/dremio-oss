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
package com.dremio.exec.store.metadatarefresh;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordWriter;

public class MetadataRefreshExecConstants {

  public static final String METADATA_STORAGE_PLUGIN_NAME = "__metadata";

  public static class DirList {
    public static class OUTPUT_SCHEMA {
      public static String FILE_PATH = "filepath";
      public static String FILE_SIZE = "filesize";
      public static String MODIFICATION_TIME = "modificationtime";
      public static String PARTITION_INFO = "partitioninfo";

      public static BatchSchema BATCH_SCHEMA = BatchSchema.newBuilder()
        .addField(MajorTypeHelper.getFieldForNameAndMajorType(FILE_PATH, Types.optional(TypeProtos.MinorType.VARCHAR)))
        .addField(MajorTypeHelper.getFieldForNameAndMajorType(FILE_SIZE, Types.optional(TypeProtos.MinorType.BIGINT)))
        .addField(MajorTypeHelper.getFieldForNameAndMajorType(MODIFICATION_TIME, Types.optional(TypeProtos.MinorType.BIGINT)))
        .addField(MajorTypeHelper.getFieldForNameAndMajorType(PARTITION_INFO, Types.optional(TypeProtos.MinorType.VARBINARY)))
        .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
        .build();
    }
  }

  public static class FooterRead {
    public static class OUTPUT_SCHEMA {
      public static final String DATA_FILE = RecordWriter.ICEBERG_METADATA_COLUMN;
      public static final String OPERATION_TYPE = RecordWriter.OPERATION_TYPE_COLUMN;
      public static final String MODIFICATION_TIME = "modificationtime";
      public static final String FILE_SCHEMA = "fileschema";

      public static BatchSchema BATCH_SCHEMA = BatchSchema.newBuilder()
        .addField(MajorTypeHelper.getFieldForNameAndMajorType(DATA_FILE, Types.optional(TypeProtos.MinorType.VARBINARY)))
        .addField(MajorTypeHelper.getFieldForNameAndMajorType(OPERATION_TYPE, Types.optional(TypeProtos.MinorType.INT)))
        .addField(MajorTypeHelper.getFieldForNameAndMajorType(MODIFICATION_TIME, Types.optional(TypeProtos.MinorType.BIGINT)))
        .addField(MajorTypeHelper.getFieldForNameAndMajorType(FILE_SCHEMA, Types.optional(TypeProtos.MinorType.VARBINARY)))
        .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
        .build();
    }
  }

  public static class SchemaAgg {
    public static class INPUT_SCHEMA {
      public static final String SCHEMA = "fileschema";
    }
  }

  public static class PathGeneratingDataFileProcessor {
    public static class OUTPUT_SCHEMA {
      public static String DATAFILE_PATH = "datafilePath";
      public static String PARTITION_DATA_PATH = "existingpartitioninfo";

      public static BatchSchema BATCH_SCHEMA = BatchSchema.newBuilder()
        .addField(Field.nullable(DATAFILE_PATH, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()))
        .addField(Field.nullable(PARTITION_DATA_PATH, org.apache.arrow.vector.types.Types.MinorType.VARBINARY.getType()))
        .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
        .build();
    }
  }

  public static final String IS_DELETED_FILE = "isDeleted";
  public static final String PARTITION_INFO = DirList.OUTPUT_SCHEMA.PARTITION_INFO;

}
