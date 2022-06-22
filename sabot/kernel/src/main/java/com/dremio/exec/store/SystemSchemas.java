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
package com.dremio.exec.store;

import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableList;

public final class SystemSchemas {
  public static final String SPLIT_IDENTITY = RecordReader.SPLIT_IDENTITY;
  public static final String SPLIT_INFORMATION = RecordReader.SPLIT_INFORMATION;
  public static final String COL_IDS = RecordReader.COL_IDS;

  public static final BatchSchema SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA = RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA;

  public static final String DATAFILE_PATH = "datafilePath";
  public static final String DELETEFILE_PATH = "deleteFilePath";
  public static final String AGG_DELETEFILE_PATHS = "deleteFilePaths";
  public static final String FILE_SIZE = "fileSize";
  public static final String FILE_CONTENT = "fileContent";
  public static final String SEQUENCE_NUMBER = "sequenceNumber";
  public static final String PARTITION_SPEC_ID = "partitionSpecId";
  public static final String PARTITION_KEY = "partitionKey";
  public static final String PARTITION_INFO = "partitionInfo";

  public static final BatchSchema ICEBERG_MANIFEST_SCAN_SCHEMA = BatchSchema.newBuilder()
      .addField(Field.nullable(DATAFILE_PATH, Types.MinorType.VARCHAR.getType()))
      .addField(Field.nullable(FILE_SIZE, Types.MinorType.BIGINT.getType()))
      .addField(Field.nullable(SEQUENCE_NUMBER, Types.MinorType.BIGINT.getType()))
      .addField(Field.nullable(PARTITION_SPEC_ID, Types.MinorType.INT.getType()))
      .addField(Field.nullable(PARTITION_KEY, Types.MinorType.VARBINARY.getType()))
      .addField(Field.nullable(PARTITION_INFO, Types.MinorType.VARBINARY.getType()))
      .addField(Field.nullable(COL_IDS, Types.MinorType.VARBINARY.getType()))
      .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
      .build();

  public static final BatchSchema ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA = BatchSchema.newBuilder()
      .addField(Field.nullable(DELETEFILE_PATH, Types.MinorType.VARCHAR.getType()))
      .addField(Field.nullable(FILE_CONTENT, Types.MinorType.INT.getType()))
      .addField(Field.nullable(SEQUENCE_NUMBER, Types.MinorType.BIGINT.getType()))
      .addField(Field.nullable(PARTITION_SPEC_ID, Types.MinorType.INT.getType()))
      .addField(Field.nullable(PARTITION_KEY, Types.MinorType.VARBINARY.getType()))
      .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
      .build();

  public static final BatchSchema ICEBERG_DELETE_FILE_AGG_SCHEMA = BatchSchema.newBuilder()
      .addField(Field.nullable(DATAFILE_PATH, Types.MinorType.VARCHAR.getType()))
      .addField(Field.nullable(FILE_SIZE, Types.MinorType.BIGINT.getType()))
      .addField(Field.nullable(PARTITION_INFO, Types.MinorType.VARBINARY.getType()))
      .addField(Field.nullable(COL_IDS, Types.MinorType.VARBINARY.getType()))
      .addField(new Field(AGG_DELETEFILE_PATHS, FieldType.nullable(Types.MinorType.LIST.getType()),
          ImmutableList.of(Field.nullable(ListVector.DATA_VECTOR_NAME, Types.MinorType.VARCHAR.getType()))))
      .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
      .build();

  public static final BatchSchema ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA = SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA
      .addColumn(new Field(AGG_DELETEFILE_PATHS, FieldType.nullable(Types.MinorType.LIST.getType()),
          ImmutableList.of(Field.nullable(ListVector.DATA_VECTOR_NAME, Types.MinorType.VARCHAR.getType()))));
}
