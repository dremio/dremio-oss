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
package com.dremio.exec.store.parquet2;

import static com.dremio.common.arrow.DremioArrowSchema.DREMIO_ARROW_SCHEMA;
import static com.dremio.common.arrow.DremioArrowSchema.DREMIO_ARROW_SCHEMA_2_1;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.net.URL;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.arrow.DremioArrowSchema;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.parquet.SingletonParquetFooterCache;
import com.dremio.io.file.Path;

public class TestParquetReader extends BaseTestQuery {
  final static String WORKING_PATH = TestTools.getWorkingPath();

  // enable decimal data type
  @BeforeClass
  public static void enableDecimalDataType() throws Exception {
    test(String.format("alter system set \"%s\" = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @AfterClass
  public static void disableDecimalDataType() throws Exception {
    test(String.format("alter system set \"%s\" = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  private void testColumn(String columnName) throws Exception {
    testNoResult("alter session set \"store.parquet.use_new_reader\" = true");

    BigDecimal result = new BigDecimal("1.20000000");

    testBuilder()
      .ordered()
      .sqlQuery("select %s from cp.\"parquet2/decimal28_38.parquet\"", columnName)
      .baselineColumns(columnName)
      .baselineValues(result)
      .go();

    testNoResult("alter session set \"store.parquet.use_new_reader\" = false");
  }

  @Test
  public void testRequiredDecimal28() throws Exception {
    testColumn("d28_req");
  }

  @Test
  public void testRequiredDecimal38() throws Exception {
    testColumn("d38_req");
  }

  @Test
  public void testOptionalDecimal28() throws Exception {
    testColumn("d28_opt");
  }

  @Test
  public void testOptionalDecimal38() throws Exception {
    testColumn("d38_opt");
  }

  @Test
  public void test4349() throws Exception {
    // start by creating a parquet file from the input csv file
    runSQL("CREATE TABLE dfs_test.\"4349\" AS SELECT columns[0] id, CAST(NULLIF(columns[1], '') AS DOUBLE) val FROM cp.\"parquet2/4349.csv.gz\"");

    // querying the parquet file should return the same results found in the csv file
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT * FROM dfs_test.\"4349\" WHERE id = 'b'")
      .sqlBaselineQuery("SELECT columns[0] id, CAST(NULLIF(columns[1], '') AS DOUBLE) val FROM cp.\"parquet2/4349.csv.gz\" WHERE columns[0] = 'b'")
      .go();
  }

  @Test
  public void testArrowSchema205InFooter() throws Exception {
    URL parquet205 = getClass().getResource("/dremio-region-205.parquet");
    Path filePath = Path.of(parquet205.toURI());
    ParquetMetadata parquetMetadata =
      SingletonParquetFooterCache.readFooter(localFs, filePath, ParquetMetadataConverter.NO_FILTER,
        ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getDefault().getNumVal());
    Map<String, String> metadata = parquetMetadata.getFileMetaData().getKeyValueMetaData();

    // should have DREMIO_ARROW_SCHEMA field, but no DREMIO_ARROW_SCHEMA_2_1
    assertTrue(metadata.containsKey(DREMIO_ARROW_SCHEMA));
    assertFalse(metadata.containsKey(DREMIO_ARROW_SCHEMA_2_1));

    Schema schema = DremioArrowSchema.fromMetaData(metadata);

    assertNotNull(schema);
  }

  @Test
  public void testArrowSchema210InFooter() throws Exception {
    URL parquet210 = getClass().getResource("/dremio-region-210.parquet");
    Path filePath210 = Path.of(parquet210.toURI());
    ParquetMetadata parquetMetadata210 =
      SingletonParquetFooterCache.readFooter(localFs, filePath210, ParquetMetadataConverter.NO_FILTER,
        ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getDefault().getNumVal());
    Map<String, String> metadata210 = parquetMetadata210.getFileMetaData().getKeyValueMetaData();

    // should not have DREMIO_ARROW_SCHEMA field, but should have DREMIO_ARROW_SCHEMA_2_1
    assertFalse(metadata210.containsKey(DREMIO_ARROW_SCHEMA));
    assertTrue(metadata210.containsKey(DREMIO_ARROW_SCHEMA_2_1));

    Schema schema210 = DremioArrowSchema.fromMetaData(metadata210);

    assertNotNull(schema210);
  }

  @Test
  public void testArrowSchemaOldInFooter() throws Exception {
    URL badparquet = getClass().getResource("/types.parquet");

    Path filePathBad = Path.of(badparquet.toURI());
    ParquetMetadata parquetMetadataBad =
      SingletonParquetFooterCache.readFooter(localFs, filePathBad, ParquetMetadataConverter.NO_FILTER,
        ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getDefault().getNumVal());
    Map<String, String> metadataBad = parquetMetadataBad.getFileMetaData().getKeyValueMetaData();

    // should have DREMIO_ARROW_SCHEMA field, but no DREMIO_ARROW_SCHEMA_2_1
    assertTrue(metadataBad.containsKey(DREMIO_ARROW_SCHEMA));
    assertFalse(metadataBad.containsKey(DREMIO_ARROW_SCHEMA_2_1));

    try {
      DremioArrowSchema.fromMetaData(metadataBad);
      fail("Should not be able to process arrow schema");
    } catch (Exception e) {
      // ok
    }
  }

  @Test
  public void testFilterOnNonExistentColumn() throws Exception {
    final String parquetFiles = WORKING_PATH + "/src/test/resources/parquet/nonexistingcols";
    testBuilder()
      .sqlQuery("SELECT * FROM dfs.\"" + parquetFiles + "\" where col1='bothvalscol1'")
      .ordered()
      .baselineColumns("col1", "col2")
      .baselineValues("bothvalscol1", "bothvalscol2")
      .go();

    testBuilder()
      .sqlQuery("SELECT col2 FROM dfs.\"" + parquetFiles + "\" where col1 is null")
      .ordered()
      .baselineColumns("col2")
      .baselineValues("singlevalcol2")
      .go();
  }

  @Test
  public void testAggregationFilterOnNonExistentColumn() throws Exception {
    final String parquetFiles = WORKING_PATH + "/src/test/resources/parquet/nonexistingcols";

    testBuilder()
      .sqlQuery("SELECT count(*) as cnt FROM dfs.\"" + parquetFiles + "\" where col1 = 'doesnotexist'")
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(0L)
      .go();

    testBuilder()
      .sqlQuery("SELECT count(*) as cnt FROM dfs.\"" + parquetFiles + "\"")
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(2L)
      .go();

    testBuilder()
      .sqlQuery("SELECT count(*) cnt FROM dfs.\"" + parquetFiles + "\" where col1='bothvalscol1'")
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(1L)
      .go();
  }
}
