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

import java.util.List;

import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.MetadataColumns;

import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public final class SystemSchemas {
  public static final String SPLIT_IDENTITY = RecordReader.SPLIT_IDENTITY;
  public static final String SPLIT_INFORMATION = RecordReader.SPLIT_INFORMATION;
  public static final String COL_IDS = RecordReader.COL_IDS;
  public static final String RECORDS = RecordWriter.RECORDS_COLUMN;

  public static final BatchSchema SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA = RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA;

  public static final String DATAFILE_PATH = "datafilePath";
  public static final String PATH = "path";
  public static final String DELETE_FILE = "deleteFile";
  public static final String DELETE_FILES = "deleteFiles";
  public static final String FILE_SIZE = "fileSize";
  public static final String FILE_CONTENT = "fileContent";
  public static final String RECORD_COUNT = "recordCount";
  public static final String SEQUENCE_NUMBER = "sequenceNumber";
  public static final String IMPLICIT_SEQUENCE_NUMBER = "$_dremio_$_sequence_no_$";
  public static final String PARTITION_SPEC_ID = "partitionSpecId";
  public static final String PARTITION_KEY = "partitionKey";
  public static final String PARTITION_INFO = "partitionInfo";
  public static final String EQUALITY_IDS = "equalityIds";
  public static final String ICEBERG_METADATA = "icebergMetadata";
  public static final String DELETE_FILE_PATH = MetadataColumns.DELETE_FILE_PATH.name();
  public static final String POS = MetadataColumns.DELETE_FILE_POS.name();
  public static final String METADATA_FILE_PATH = "metadataFilePath";
  public static final String MANIFEST_LIST_PATH = "manifestListPath";
  public static final String SNAPSHOT_ID = "snapshotId";
  public static final String FILE_PATH = "filePath";
  public static final String FILE_TYPE = "fileType";

  public static final Field ICEBERG_METADATA_FIELD = Field.nullable(ICEBERG_METADATA, Types.MinorType.VARBINARY.getType());
  public static final List<String> CARRY_FORWARD_FILE_PATH_TYPE_COLS = Lists.newArrayList(FILE_PATH, FILE_TYPE);

  public static final BatchSchema ICEBERG_MANIFEST_SCAN_SCHEMA = BatchSchema.newBuilder()
      .addField(Field.nullable(DATAFILE_PATH, Types.MinorType.VARCHAR.getType()))
      .addField(Field.nullable(FILE_SIZE, Types.MinorType.BIGINT.getType()))
      .addField(Field.nullable(SEQUENCE_NUMBER, Types.MinorType.BIGINT.getType()))
      .addField(Field.nullable(PARTITION_SPEC_ID, Types.MinorType.INT.getType()))
      .addField(Field.nullable(PARTITION_KEY, Types.MinorType.VARBINARY.getType()))
      .addField(Field.nullable(PARTITION_INFO, Types.MinorType.VARBINARY.getType()))
      .addField(Field.nullable(COL_IDS, Types.MinorType.VARBINARY.getType()))
      .addField(Field.nullable(FILE_CONTENT, Types.MinorType.VARCHAR.getType()))
      .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
      .build();

  /*
   * Delete file path, File size, Partition Info and Column IDs are being projected for delete manifest scans
   * to follow SPLIT_GEN schema and allow delete files to be read using DATA_FILE_SCAN table function.
   */
  public static final BatchSchema ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA = BatchSchema.newBuilder()
      .addField(buildDeleteFileStruct(DELETE_FILE))
      .addField(Field.nullable(DATAFILE_PATH, Types.MinorType.VARCHAR.getType()))
      .addField(Field.nullable(FILE_SIZE, Types.MinorType.BIGINT.getType()))
      .addField(Field.nullable(SEQUENCE_NUMBER, Types.MinorType.BIGINT.getType()))
      .addField(Field.nullable(PARTITION_SPEC_ID, Types.MinorType.INT.getType()))
      .addField(Field.nullable(PARTITION_KEY, Types.MinorType.VARBINARY.getType()))
      .addField(Field.nullable(PARTITION_INFO, Types.MinorType.VARBINARY.getType()))
      .addField(Field.nullable(COL_IDS, Types.MinorType.VARBINARY.getType()))
      .addField(Field.nullable(FILE_CONTENT, Types.MinorType.VARCHAR.getType()))
      .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
      .build();

  public static final BatchSchema ICEBERG_POS_DELETE_FILE_SCHEMA = BatchSchema.newBuilder()
    .addField(Field.nullable(DELETE_FILE_PATH, Types.MinorType.VARCHAR.getType()))
    .addField(Field.nullable(IMPLICIT_SEQUENCE_NUMBER, Types.MinorType.BIGINT.getType()))
    .addField(Field.nullable(POS, Types.MinorType.BIGINT.getType()))
    .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
    .build();

  public static final BatchSchema ICEBERG_DELETE_FILE_AGG_SCHEMA = BatchSchema.newBuilder()
      .addField(Field.nullable(DATAFILE_PATH, Types.MinorType.VARCHAR.getType()))
      .addField(Field.nullable(FILE_SIZE, Types.MinorType.BIGINT.getType()))
      .addField(Field.nullable(PARTITION_INFO, Types.MinorType.VARBINARY.getType()))
      .addField(Field.nullable(COL_IDS, Types.MinorType.VARBINARY.getType()))
      .addField(new Field(DELETE_FILES, FieldType.nullable(Types.MinorType.LIST.getType()),
          ImmutableList.of(buildDeleteFileStruct(ListVector.DATA_VECTOR_NAME))))
      .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
      .build();

  public static final BatchSchema ICEBERG_SNAPSHOTS_SCAN_SCHEMA = BatchSchema.newBuilder()
    .addField(Field.nullable(METADATA_FILE_PATH, Types.MinorType.VARCHAR.getType()))
    .addField(Field.nullable(SNAPSHOT_ID, Types.MinorType.BIGINT.getType()))
    .addField(Field.nullable(MANIFEST_LIST_PATH, Types.MinorType.VARCHAR.getType()))
    .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
    .build();

  public static final BatchSchema CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA = BatchSchema.newBuilder()
    .addField(Field.nullable(FILE_PATH, Types.MinorType.VARCHAR.getType()))
    .addField(Field.nullable(FILE_TYPE, Types.MinorType.VARCHAR.getType()))
    .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
    .build();

  public static final BatchSchema ICEBERG_ORPHAN_FILE_DELETE_SCHEMA = BatchSchema.newBuilder()
    .addField(Field.nullable(FILE_PATH, Types.MinorType.VARCHAR.getType()))
    .addField(Field.nullable(FILE_TYPE, Types.MinorType.VARCHAR.getType()))
    .addField(Field.nullable(RECORDS, Types.MinorType.BIGINT.getType()))
    .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
    .build();

  public static final BatchSchema ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA = SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA
      .addColumn(new Field(DELETE_FILES, FieldType.nullable(Types.MinorType.LIST.getType()),
          ImmutableList.of(buildDeleteFileStruct(ListVector.DATA_VECTOR_NAME))));

  public static Field buildDeleteFileStruct(String fieldName) {
    return new Field(fieldName, FieldType.nullable(Types.MinorType.STRUCT.getType()),
        ImmutableList.of(
            Field.nullable(PATH, Types.MinorType.VARCHAR.getType()),
            Field.nullable(FILE_CONTENT, Types.MinorType.INT.getType()),
            Field.nullable(RECORD_COUNT, Types.MinorType.BIGINT.getType()),
            new Field(EQUALITY_IDS, FieldType.nullable(Types.MinorType.LIST.getType()),
                ImmutableList.of(
                    Field.nullable(ListVector.DATA_VECTOR_NAME, Types.MinorType.INT.getType())))));
  }
}
