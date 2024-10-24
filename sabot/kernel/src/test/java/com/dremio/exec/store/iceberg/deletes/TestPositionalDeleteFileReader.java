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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.parquet.InputStreamProviderFactory;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPositionalDeleteFileReader extends BaseTestOperator {

  // Test file with 500 deletes spread across 2 data files.  Deleted positions in each data file are
  // DATA_FILE_0: [ 700 .. 999 ]
  // DATA_FILE_1: [ 0 .. 199 ]
  // DATA_FILE_2: []
  private static final Path DELETE_FILE_2 =
      Path.of(
          IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES_FULL_PATH
              + "/data/2021/delete-2021-02.parquet");

  // Test file with 1000 rows split across two row groups.  First row group has 552 rows, second has
  // 448.
  // order_id values range from 6000 - 6999
  private static final String DATA_FILE_0 =
      IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES_FULL_PATH
          + "/data/2021/2021-00.parquet";
  // Test file with 1000 rows split across two row groups.  First row group has 551 rows, second has
  // 449.
  // order_id values range from 7000 - 7999
  private static final String DATA_FILE_1 =
      IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES_FULL_PATH
          + "/data/2021/2021-01.parquet";
  // Test file with 1000 rows split across two row groups.  First row group has 552 rows, second has
  // 448.
  // order_id values range from 8000 - 8999
  private static final String DATA_FILE_2 =
      IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES_FULL_PATH
          + "/data/2021/2021-02.parquet";

  private static final int DEFAULT_BATCH_SIZE = 42;

  private OperatorContextImpl context;
  private RowLevelDeleteFileReaderFactory factory;

  private static IcebergTestTables.Table table;

  @BeforeClass
  public static void initTables() {
    table = IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
  }

  @AfterClass
  public static void closeTables() throws Exception {
    AutoCloseables.close(table);
  }

  @Before
  public void beforeTest() throws Exception {
    context = testContext.getNewOperatorContext(getTestAllocator(), null, DEFAULT_BATCH_SIZE, null);
    testCloseables.add(context);
    FileSystem fs = HadoopFileSystem.get(Path.of("/"), new Configuration(), context.getStats());
    factory =
        new ParquetRowLevelDeleteFileReaderFactory(
            InputStreamProviderFactory.DEFAULT,
            ParquetReaderFactory.NONE,
            fs,
            null,
            IcebergTestTables.V2_ORDERS_SCHEMA);
  }

  @Test
  public void testIterationOverSingleDataFile() throws Exception {
    List<Long> expected = Stream.iterate(0L, i -> i + 1).limit(200).collect(Collectors.toList());

    PositionalDeleteFileReader reader = createReader(DELETE_FILE_2, ImmutableList.of(DATA_FILE_1));

    validateIterator(reader, DATA_FILE_1, expected);
    assertThat(reader.refCount()).isEqualTo(0);
  }

  @Test
  public void testIterationOverDataFileNotInDeleteFile() throws Exception {
    PositionalDeleteFileReader reader = createReader(DELETE_FILE_2, ImmutableList.of(DATA_FILE_2));

    validateIterator(reader, DATA_FILE_2, ImmutableList.of());
    assertThat(reader.refCount()).isEqualTo(0);
  }

  @Test
  public void testIterationOverAllDataFiles() throws Exception {
    List<Long> expected0 = Stream.iterate(700L, i -> i + 1).limit(300).collect(Collectors.toList());
    List<Long> expected1 = Stream.iterate(0L, i -> i + 1).limit(200).collect(Collectors.toList());
    List<Long> expected2 = ImmutableList.of();

    PositionalDeleteFileReader reader =
        createReader(DELETE_FILE_2, ImmutableList.of(DATA_FILE_0, DATA_FILE_1, DATA_FILE_2));

    validateIterator(reader, DATA_FILE_0, expected0);
    validateIterator(reader, DATA_FILE_1, expected1);
    validateIterator(reader, DATA_FILE_2, expected2);
    assertThat(reader.refCount()).isEqualTo(0);
  }

  @Test
  public void testMultipleActiveIteratorsFails() throws Exception {
    PositionalDeleteFileReader reader =
        createReader(DELETE_FILE_2, ImmutableList.of(DATA_FILE_0, DATA_FILE_1, DATA_FILE_2));
    try (PositionalDeleteIterator iterator = reader.createIteratorForDataFile(DATA_FILE_0)) {
      assertThatThrownBy(() -> reader.createIteratorForDataFile(DATA_FILE_1))
          .isInstanceOf(IllegalStateException.class);
    }
  }

  @Test
  public void testOutOfOrderDataFileIterationFails() throws Exception {
    PositionalDeleteFileReader reader =
        createReader(DELETE_FILE_2, ImmutableList.of(DATA_FILE_0, DATA_FILE_1, DATA_FILE_2));
    reader.createIteratorForDataFile(DATA_FILE_1).close();
    assertThatThrownBy(() -> reader.createIteratorForDataFile(DATA_FILE_0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testRepeatedCloseOnIteratorFails() throws Exception {
    PositionalDeleteFileReader reader =
        createReader(DELETE_FILE_2, ImmutableList.of(DATA_FILE_0, DATA_FILE_1, DATA_FILE_2));
    PositionalDeleteIterator iterator = reader.createIteratorForDataFile(DATA_FILE_1);
    iterator.close();
    assertThatThrownBy(iterator::close).isInstanceOf(IllegalStateException.class);
  }

  private PositionalDeleteFileReader createReader(Path deleteFilePath, List<String> dataFiles)
      throws Exception {
    PositionalDeleteFileReader reader =
        factory.createPositionalDeleteFileReader(context, deleteFilePath, dataFiles);
    testCloseables.add(reader);
    reader.setup();
    assertThat(reader.refCount()).isEqualTo(dataFiles.size());
    return reader;
  }

  private void validateIterator(
      PositionalDeleteFileReader reader, String dataFile, List<Long> expected) throws Exception {
    try (PositionalDeleteIterator iterator = reader.createIteratorForDataFile(dataFile)) {
      assertThat(iterator).toIterable().containsExactlyElementsOf(expected);
    }
  }
}
