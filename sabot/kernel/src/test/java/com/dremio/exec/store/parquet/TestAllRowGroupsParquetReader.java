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

import static com.dremio.exec.store.iceberg.deletes.PositionalDeleteFileReader.FILE_PATH_COLUMN;
import static com.dremio.exec.store.iceberg.deletes.PositionalDeleteFileReader.POS_COLUMN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.TestTools;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.iceberg.deletes.ParquetDeleteFileFilterCreator;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.google.common.collect.ImmutableList;

public class TestAllRowGroupsParquetReader extends BaseTestOperator {

  private OperatorContextImpl context;
  private FileSystem fs;
  private SampleMutator mutator;

  // Test file with 900 deletes spread equally across 3 different data files.  Deleted positions in each data file are
  // [ 0, 1, 2, 10, 11, 12, 20, 21, 22, ..., 990, 991, 992 ]
  private static final Path MULTI_ROWGROUP_DELETE_FILE = Path.of(TestTools.getWorkingPath() +
    "/src/test/resources/iceberg/v2/multi_rowgroup_orders_with_deletes/data/2021/delete-2021-00.parquet");

  private static final ParquetReaderOptions DEFAULT_READER_OPTIONS = ParquetReaderOptions.builder()
    .enablePrefetching(false)
    .setNumSplitsToPrefetch(0)
    .build();
  private static final ParquetReaderOptions PREFETCH_READER_OPTIONS = ParquetReaderOptions.builder()
    .enablePrefetching(true)
    .setNumSplitsToPrefetch(1)
    .build();

  private static final ParquetScanProjectedColumns PROJECTED_COLUMNS = ParquetScanProjectedColumns.fromSchemaPaths(
    ImmutableList.of(SchemaPath.getSimplePath(FILE_PATH_COLUMN), SchemaPath.getSimplePath(POS_COLUMN)));

  private static final List<String> EXPECTED_FILE_PATHS = generateExpectedFilePaths();
  private static final List<Long> EXPECTED_POSITIONS = generateExpectedPositions();

  @Before
  public void before() throws Exception {
    TableFunctionPOP pop = new TableFunctionPOP(PROPS, null, null);
    context = testContext.getNewOperatorContext(getTestAllocator(), pop, 4000, null);
    testCloseables.add(context);
    fs = HadoopFileSystem.getLocal(new Configuration());
    mutator = new SampleMutator(getTestAllocator());
    testCloseables.add(mutator);
  }

  @Test
  public void testSimpleRead() throws Exception {
    AllRowGroupsParquetReader reader = new AllRowGroupsParquetReader(
      context,
      MULTI_ROWGROUP_DELETE_FILE,
      null,
      fs,
      InputStreamProviderFactory.DEFAULT,
      ParquetReaderFactory.NONE,
      IcebergTestTables.V2_ORDERS_DELETE_FILE_SCHEMA,
      PROJECTED_COLUMNS,
      ParquetFilters.NONE,
      DEFAULT_READER_OPTIONS);
    testCloseables.add(reader);

    validateResults(reader, 0, 900);
  }

  @Test
  public void testFilteredRead() throws Exception {
    AllRowGroupsParquetReader reader = new AllRowGroupsParquetReader(
      context,
      MULTI_ROWGROUP_DELETE_FILE,
      null,
      fs,
      InputStreamProviderFactory.DEFAULT,
      ParquetReaderFactory.NONE,
      IcebergTestTables.V2_ORDERS_DELETE_FILE_SCHEMA,
      PROJECTED_COLUMNS,
      new ParquetFilters(ParquetDeleteFileFilterCreator.DEFAULT.createFilePathFilter(
          "/tmp/iceberg-test-tables/v2/multi_rowgroup_orders_with_deletes/data/2021/2021-02.parquet",
          "/tmp/iceberg-test-tables/v2/multi_rowgroup_orders_with_deletes/data/2021/2021-02.parquet")),
      DEFAULT_READER_OPTIONS);
    testCloseables.add(reader);

    validateResults(reader, 600, 900);
  }

  @Test
  public void testReadWithPrefetch() throws Exception {

    InputStreamProviderFactory factorySpy = spy(InputStreamProviderFactory.DEFAULT);

    AllRowGroupsParquetReader reader = new AllRowGroupsParquetReader(
      context,
      MULTI_ROWGROUP_DELETE_FILE,
      null,
      fs,
      factorySpy,
      ParquetReaderFactory.NONE,
      IcebergTestTables.V2_ORDERS_DELETE_FILE_SCHEMA,
      PROJECTED_COLUMNS,
      ParquetFilters.NONE,
      PREFETCH_READER_OPTIONS);
    testCloseables.add(reader);

    for (Field field : IcebergTestTables.V2_ORDERS_DELETE_FILE_SCHEMA.getFields()) {
      mutator.addField(field, TypeHelper.getValueVectorClass(field));
    }
    mutator.getContainer().buildSchema();
    mutator.getAndResetSchemaChanged();

    reader.setup(mutator);
    reader.allocate(mutator.getFieldVectorMap());

    // verify if prefetching happens prior to requesting the 1st row
    verifyRowGroupPrefetched(factorySpy, 0);
    verifyRowGroupPrefetched(factorySpy, 1);
    verifyRowGroupNotPrefetched(factorySpy, 2);

    // advance to row group 1 and see if row group 2 is prefetched
    int totalRecords = 0;
    while (totalRecords < 500) {
      int records = reader.next();
      if (records == 0) {
        break;
      }
      totalRecords += records;
    }

    verifyRowGroupPrefetched(factorySpy, 2);
  }

  @Test
  public void testPathWithScheme() throws Exception {
    // This may look a bit odd, but it simulates what happens with the S3 filesystem and serves to test the scenario...
    // The constructor should normalize the path to be a container-specific relative path.  This test takes advantage of
    // the fact that the filesystem doesn't check whether it supports a specific scheme before normalizing it, since
    // this test passes a path with s3:// scheme but the filesystem instance is a local Hadoop filesystem.
    Path path = Path.of("s3:/" + MULTI_ROWGROUP_DELETE_FILE);
    AllRowGroupsParquetReader reader = new AllRowGroupsParquetReader(
        context,
        path,
        null,
        fs,
        InputStreamProviderFactory.DEFAULT,
        ParquetReaderFactory.NONE,
        IcebergTestTables.V2_ORDERS_DELETE_FILE_SCHEMA,
        PROJECTED_COLUMNS,
        ParquetFilters.NONE,
        DEFAULT_READER_OPTIONS);
    testCloseables.add(reader);

    validateResults(reader, 0, 900);
  }

  @Test
  public void testDatasetKeyPassedToInputStreamProviderFactory() throws Exception {
    InputStreamProviderFactory factorySpy = spy(InputStreamProviderFactory.DEFAULT);

    List<String> datasetKey = ImmutableList.of("my", "table");
    AllRowGroupsParquetReader reader = new AllRowGroupsParquetReader(
        context,
        MULTI_ROWGROUP_DELETE_FILE,
        datasetKey,
        fs,
        factorySpy,
        ParquetReaderFactory.NONE,
        IcebergTestTables.V2_ORDERS_DELETE_FILE_SCHEMA,
        PROJECTED_COLUMNS,
        ParquetFilters.NONE,
        PREFETCH_READER_OPTIONS);
    testCloseables.add(reader);

    for (Field field : IcebergTestTables.V2_ORDERS_DELETE_FILE_SCHEMA.getFields()) {
      mutator.addField(field, TypeHelper.getValueVectorClass(field));
    }
    mutator.getContainer().buildSchema();
    mutator.getAndResetSchemaChanged();

    reader.setup(mutator);
    reader.allocate(mutator.getFieldVectorMap());

    verifyDatasetKey(factorySpy, datasetKey);
  }

  private void verifyRowGroupPrefetched(InputStreamProviderFactory spy, int rowGroupIndex) throws Exception {
    verify(spy).create(
      eq(fs),
      eq(context),
      eq(MULTI_ROWGROUP_DELETE_FILE),
      anyLong(),
      anyLong(),
      eq(PROJECTED_COLUMNS),
      any(),
      any(),
      argThat(arg -> arg.apply(null) == rowGroupIndex),
      eq(false),
      eq(null),
      anyLong(),
      eq(false),
      eq(true), eq(ParquetFilters.NONE), eq(ParquetFilterCreator.DEFAULT), eq(InputStreamProviderFactory.DEFAULT_NON_PARTITION_COLUMN_RF));
  }

  private void verifyRowGroupNotPrefetched(InputStreamProviderFactory spy, int rowGroupIndex) throws Exception {
    verify(spy, never()).create(
      eq(fs),
      eq(context),
      eq(MULTI_ROWGROUP_DELETE_FILE),
      anyLong(),
      anyLong(),
      eq(PROJECTED_COLUMNS),
      any(),
      any(),
      argThat(arg -> arg.apply(null) == rowGroupIndex),
      eq(false),
      eq(null),
      anyLong(),
      eq(false),
      eq(true), eq(ParquetFilters.NONE), eq(ParquetFilterCreator.DEFAULT), eq(InputStreamProviderFactory.DEFAULT_NON_PARTITION_COLUMN_RF));
  }

  private void verifyDatasetKey(InputStreamProviderFactory spy, List<String> dataset) throws Exception {
    verify(spy, atLeastOnce()).create(
        eq(fs),
        eq(context),
        eq(MULTI_ROWGROUP_DELETE_FILE),
        anyLong(),
        anyLong(),
        eq(PROJECTED_COLUMNS),
        any(),
        any(),
        any(),
        eq(false),
        eq(dataset),
        anyLong(),
        eq(false),
        eq(true), eq(ParquetFilters.NONE), eq(ParquetFilterCreator.DEFAULT), eq(InputStreamProviderFactory.DEFAULT_NON_PARTITION_COLUMN_RF));
  }

  private void validateResults(AllRowGroupsParquetReader reader, int expectedStartRow, int expectedEndRow)
    throws Exception {

    for (Field field : IcebergTestTables.V2_ORDERS_DELETE_FILE_SCHEMA.getFields()) {
      mutator.addField(field, TypeHelper.getValueVectorClass(field));
    }
    mutator.getContainer().buildSchema();
    mutator.getAndResetSchemaChanged();

    reader.setup(mutator);
    reader.allocate(mutator.getFieldVectorMap());

    VarCharVector filePathVector = (VarCharVector) mutator.getVector(FILE_PATH_COLUMN);
    BigIntVector posVector = (BigIntVector) mutator.getVector(POS_COLUMN);

    List<String> actualFilePaths = new ArrayList<>();
    List<Long> actualPositions = new ArrayList<>();

    int records;
    while ((records = reader.next()) != 0) {
      for (int i = 0; i < records; i++) {
        actualFilePaths.add(filePathVector.getObject(i).toString());
        actualPositions.add(posVector.get(i));
      }
    }

    assertThat(actualFilePaths).isEqualTo(EXPECTED_FILE_PATHS.subList(expectedStartRow, expectedEndRow));
    assertThat(actualPositions).isEqualTo(EXPECTED_POSITIONS.subList(expectedStartRow, expectedEndRow));
  }

  private static List<String> generateExpectedFilePaths() {
    List<String> expectedFilePaths = ImmutableList.of(
      "/tmp/iceberg-test-tables/v2/multi_rowgroup_orders_with_deletes/data/2021/2021-00.parquet",
      "/tmp/iceberg-test-tables/v2/multi_rowgroup_orders_with_deletes/data/2021/2021-01.parquet",
      "/tmp/iceberg-test-tables/v2/multi_rowgroup_orders_with_deletes/data/2021/2021-02.parquet"
    );

    return Stream.iterate(0, i -> i + 1)
      .limit(900)
      .map(i -> expectedFilePaths.get(i / 300))
      .collect(Collectors.toList());
  }

  private static List<Long> generateExpectedPositions() {
    return Stream.iterate(0, i -> i + 1)
      .limit(900)
      .map(i -> (((i % 300) / 3) * 10L) + (i % 3))
      .collect(Collectors.toList());
  }

}
