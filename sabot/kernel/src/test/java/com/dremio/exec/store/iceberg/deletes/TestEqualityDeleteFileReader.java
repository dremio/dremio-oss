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
package com.dremio.exec.store.iceberg.deletes;

import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rs;
import static com.dremio.sabot.op.common.ht2.LBlockHashTable.ORDINAL_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.parquet.InputStreamProviderFactory;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.Generator;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.google.common.collect.ImmutableList;

public class TestEqualityDeleteFileReader extends BaseTestEqualityDeleteFilter {

  // contains 30 rows deleting product_ids [ 0 .. 29 ]
  private static final Path DELETE_FILE_1 =
      Path.of("/tmp/iceberg-test-tables/v2/products_with_eq_deletes/data/widget/eqdelete-widget-00.parquet");

  private static final List<IcebergProtobuf.IcebergSchemaField> PRODUCTS_ICEBERG_FIELDS = ImmutableList.of(
      IcebergProtobuf.IcebergSchemaField.newBuilder().setSchemaPath("product_id").setId(1).build(),
      IcebergProtobuf.IcebergSchemaField.newBuilder().setSchemaPath("name").setId(2).build(),
      IcebergProtobuf.IcebergSchemaField.newBuilder().setSchemaPath("category").setId(3).build(),
      IcebergProtobuf.IcebergSchemaField.newBuilder().setSchemaPath("color").setId(4).build(),
      IcebergProtobuf.IcebergSchemaField.newBuilder().setSchemaPath("created_date").setId(5).build(),
      IcebergProtobuf.IcebergSchemaField.newBuilder().setSchemaPath("weight").setId(6).build(),
      IcebergProtobuf.IcebergSchemaField.newBuilder().setSchemaPath("quantity").setId(7).build());

  private static final int DEFAULT_BATCH_SIZE = 12;

  private OperatorContextImpl context;
  private RowLevelDeleteFileReaderFactory factory;

  private static IcebergTestTables.Table table;

  @BeforeClass
  public static void initTables() {
    table = IcebergTestTables.PRODUCTS_WITH_EQ_DELETES.get();
  }

  @AfterClass
  public static void closeTables() throws Exception {
    table.close();
  }

  @Before
  public void beforeTest() throws Exception {
    context = testContext.getNewOperatorContext(getTestAllocator(), null, DEFAULT_BATCH_SIZE, null);
    testCloseables.add(context);
    FileSystem fs = HadoopFileSystem.get(Path.of("/"), new Configuration(), context.getStats());
    factory = new ParquetRowLevelDeleteFileReaderFactory(
        InputStreamProviderFactory.DEFAULT,
        ParquetReaderFactory.NONE,
        fs,
        null,
        IcebergTestTables.PRODUCTS_SCHEMA);
  }

  @Test
  public void testTableCreation() throws Exception {
    validate(DELETE_FILE_1, 30, ImmutableList.of(1), generateExpectedProductIds(0, 30));
  }

  private RecordSet generateExpectedProductIds(int start, int end) {
    RecordSet.Record[] records = new RecordSet.Record[end - start];
    for (int i = start; i < end; i++) {
      records[i - start] = r(i);
    }
    return rs(IcebergTestTables.PRODUCTS_SCHEMA.maskAndReorder(
        ImmutableList.of(SchemaPath.getSimplePath("product_id"))), records);
  }

  private void validate(Path deleteFilePath, long recordCount, List<Integer> equalityIds, RecordSet probeRs)
      throws Exception {
    int probeBatchSize = probeRs.getMaxBatchSize();

    List<Integer> probeOrdinals = new ArrayList<>();

    try (EqualityDeleteFileReader reader = createReader(deleteFilePath, recordCount, equalityIds);
        EqualityDeleteHashTable table = reader.buildHashTable();
        Generator probeGenerator = probeRs.toGenerator(getTestAllocator());
        ArrowBuf probeOrdinalBuf = getTestAllocator().buffer((long) probeBatchSize * ORDINAL_SIZE)) {

      assertThat(table.size()).isEqualTo(recordCount);

      VectorAccessible probeAccessible = probeGenerator.getOutput();
      PivotDef probePivot = createPivotDef(probeAccessible, table.getEqualityFields());

      int records;
      while ((records = probeGenerator.next(probeBatchSize)) > 0) {
        find(table, records, probePivot, probeOrdinalBuf);
        appendOrdinalsToList(probeOrdinalBuf, records, probeOrdinals);
      }
    }

    assertThat(probeOrdinals).allMatch(i -> i >= 0);
  }

  private EqualityDeleteFileReader createReader(Path deleteFilePath, long recordCount, List<Integer> equalityIds)
      throws Exception {
    EqualityDeleteFileReader reader = factory.createEqualityDeleteFileReader(context, deleteFilePath, recordCount,
        equalityIds, PRODUCTS_ICEBERG_FIELDS);
    reader.setup();
    return reader;
  }
}
