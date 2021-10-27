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
package com.dremio.exec.store.parquet;

import java.util.List;

import org.apache.iceberg.parquet.ParquetMessageTypeIDExtractor;
import org.apache.parquet.schema.MessageType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf.IcebergSchemaField;

/**
 * This class stores projected schema paths for the parquet scan operator
 * It delegates the call to appropriate column name resolver
 */

public class ParquetScanProjectedColumns {
  private final List<SchemaPath> projectedColumns;
  private final List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs;
  private final boolean isConvertedIcebergDataset;

  private ParquetScanProjectedColumns(List<SchemaPath> projectedColumns, List<IcebergSchemaField> icebergColumnIDs, boolean isConvertedIcebergDataset) {
    this.projectedColumns = projectedColumns;
    this.icebergColumnIDs = icebergColumnIDs;
    this.isConvertedIcebergDataset = isConvertedIcebergDataset;
  }

  public int size() {
    return projectedColumns.size();
  }

  public ParquetColumnResolver getColumnResolver(MessageType parquetSchema) {
    ParquetColumnResolver columnResolver;
    if (this.icebergColumnIDs == null || icebergColumnIDs.isEmpty()) {
      columnResolver = new ParquetColumnDefaultResolver(this.projectedColumns);
      return columnResolver;
    }
    if (!this.isConvertedIcebergDataset) {
      ParquetMessageTypeIDExtractor idExtractor = new ParquetMessageTypeIDExtractor();
      idExtractor.hasIds(parquetSchema);
      columnResolver = new ParquetColumnIcebergResolver(this.projectedColumns, this.icebergColumnIDs, idExtractor.getAliases());
    } else {
      columnResolver = new ParquetColumnDefaultResolver(this.projectedColumns);
    }
    return columnResolver;
  }

  private ParquetScanProjectedColumns(List<SchemaPath> projectedColumns) {
    this(projectedColumns, null, true);
  }

  public List<SchemaPath> getBatchSchemaProjectedColumns() {
    return this.projectedColumns;
  }

  public static ParquetScanProjectedColumns fromSchemaPaths(List<SchemaPath> projectedColumns) {
    return new ParquetScanProjectedColumns(projectedColumns);
  }

  public static ParquetScanProjectedColumns fromSchemaPathAndIcebergSchema(
    List<SchemaPath> projectedColumns, List<IcebergSchemaField> icebergColumnIDs, boolean isConvertedIcebergDataset) {
    return new ParquetScanProjectedColumns(projectedColumns, icebergColumnIDs, isConvertedIcebergDataset);
  }

  ParquetScanProjectedColumns cloneForSchemaPaths(List<SchemaPath> projectedColumns, boolean isConvertedIcebergDataset) {
    return new ParquetScanProjectedColumns(projectedColumns, this.icebergColumnIDs, isConvertedIcebergDataset);
  }
}
