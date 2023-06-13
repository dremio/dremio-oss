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
package com.dremio.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.PlanTestBase;
import com.dremio.common.AutoCloseables;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.util.GlobalDictionaryBuilder;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;

/**
 * tpch queries with global dictionaries
 */
public class TestTpchDistributedWithGlobalDictionaries extends PlanTestBase {

  @ClassRule
  public static TemporaryFolder folder = new TemporaryFolder();

  private static BufferAllocator testRootAllocator;
  private static BufferAllocator testAllocator;

  private static FileSystem fs;
  private static Path lineitem;
  private static Path customer;
  private static Path part;
  private static Path partsupp;
  private static Path region;
  private static Path nation;
  private static Path supplier;
  private static Path orders;

  @BeforeClass
  public static void setup() throws Exception {
    testRootAllocator = RootAllocatorFactory.newRoot(config);
    testAllocator = testRootAllocator.newChildAllocator("test-tpch-distrib", 0, testRootAllocator.getLimit());
    testNoResult("alter session set \"store.parquet.enable_dictionary_encoding_binary_type\"=true");
    final Configuration conf = new Configuration();
    final CompressionCodecFactory codec = CodecFactory.createDirectCodecFactory(conf, new ParquetDirectByteBufferAllocator(testAllocator), 0);
    try {
      fs = HadoopFileSystem.getLocal(conf);
      testNoResult("CREATE TABLE dfs_test.tpch_lineitem_gd AS SELECT * FROM cp.\"tpch/lineitem.parquet\"");
      testNoResult("CREATE TABLE dfs_test.tpch_customer_gd AS SELECT * FROM cp.\"tpch/customer.parquet\"");
      testNoResult("CREATE TABLE dfs_test.tpch_part_gd AS SELECT * FROM cp.\"tpch/part.parquet\"");
      testNoResult("CREATE TABLE dfs_test.tpch_partsupp_gd AS SELECT * FROM cp.\"tpch/partsupp.parquet\"");
      testNoResult("CREATE TABLE dfs_test.tpch_region_gd AS SELECT * FROM cp.\"tpch/region.parquet\"");
      testNoResult("CREATE TABLE dfs_test.tpch_nation_gd AS SELECT * FROM cp.\"tpch/nation.parquet\"");
      testNoResult("CREATE TABLE dfs_test.tpch_supplier_gd AS SELECT * FROM cp.\"tpch/supplier.parquet\"");
      testNoResult("CREATE TABLE dfs_test.tpch_orders_gd AS SELECT * FROM cp.\"tpch/orders.parquet\"");

      lineitem = Path.of(getDfsTestTmpSchemaLocation() + "/tpch_lineitem_gd");
      customer = Path.of(getDfsTestTmpSchemaLocation() + "/tpch_customer_gd");
      part = Path.of(getDfsTestTmpSchemaLocation() + "/tpch_part_gd");
      partsupp = Path.of(getDfsTestTmpSchemaLocation() + "/tpch_partsupp_gd");
      region = Path.of(getDfsTestTmpSchemaLocation() + "/tpch_region_gd");
      nation = Path.of(getDfsTestTmpSchemaLocation() + "/tpch_nation_gd");
      supplier = Path.of(getDfsTestTmpSchemaLocation() + "/tpch_supplier_gd");
      orders = Path.of(getDfsTestTmpSchemaLocation() + "/tpch_orders_gd");

      GlobalDictionaryBuilder.createGlobalDictionaries(codec, fs, lineitem, testAllocator);
      GlobalDictionaryBuilder.createGlobalDictionaries(codec, fs, customer, testAllocator);
      GlobalDictionaryBuilder.createGlobalDictionaries(codec, fs, part, testAllocator);
      GlobalDictionaryBuilder.createGlobalDictionaries(codec, fs, partsupp, testAllocator);
      GlobalDictionaryBuilder.createGlobalDictionaries(codec, fs, region, testAllocator);
      GlobalDictionaryBuilder.createGlobalDictionaries(codec, fs, nation, testAllocator);
      GlobalDictionaryBuilder.createGlobalDictionaries(codec, fs, supplier, testAllocator);
      GlobalDictionaryBuilder.createGlobalDictionaries(codec, fs, orders, testAllocator);
      disableGlobalDictionary();
    } finally {
      codec.release();
    }
  }

  @AfterClass
  public static void cleanup() throws Exception {
    testNoResult("alter session set \"store.parquet.enable_dictionary_encoding_binary_type\"=false");
    test("alter session set \"planner.slice_target\" = " + ExecConstants.SLICE_TARGET_DEFAULT);
    localFs.delete(lineitem, true);
    localFs.delete(customer, true);
    localFs.delete(part, true);
    localFs.delete(partsupp, true);
    localFs.delete(region, true);
    localFs.delete(nation, true);
    localFs.delete(supplier, true);
    localFs.delete(orders, true);
    enableGlobalDictionary();
    AutoCloseables.close(testAllocator, testRootAllocator);
  }

  private static void testDistributed(final String fileName, String tag) throws Exception {
    final String query = getFile(fileName);
    setSessionOption(ExecConstants.SLICE_TARGET_OPTION, "10");
    validateResults(query, tag);
  }

  @Ignore
  @Test
  public void testLineitem() throws Exception {
    validateResults("select * from dfs_test.tpch_lineitem_gd", "lineitem");
  }

  @Ignore
  @Test
  public void testCustomer() throws Exception{
    validateResults("select * from dfs_test.tpch_customer_gd", "customer");
  }

  @Ignore
  @Test
  public void testOrders() throws Exception {
    validateResults("select * from dfs_test.tpch_orders_gd", "orders");
  }

  @Ignore
  @Test
  public void testRegion() throws Exception{
    validateResults("select * from dfs_test.tpch_region_gd", "region");
  }

  @Ignore
  @Test
  public void testNation() throws Exception{
    validateResults("select * from dfs_test.tpch_nation_gd", "nation");
  }

  @Ignore
  @Test
  public void testPart() throws Exception{
    validateResults("select * from dfs_test.tpch_part_gd", "part");
  }

  @Ignore
  @Test
  public void testPartsupp() throws Exception{
    validateResults("select * from dfs_test.tpch_partsupp_gd", "partsupp");
  }

  @Ignore
  @Test
  public void testSupplier() throws Exception{
    validateResults("select * from dfs_test.tpch_supplier_gd", "supplier");
  }

  @Ignore
  @Test
  public void tpch01() throws Exception{
    // with global dictionary enabled there is no filter in the query and no condition in the parquet scan!
    testDistributed("queries/tpch_gd/01.sql", "tpch01");
  }

  @Test
  @Ignore // DRILL-512
  public void tpch02() throws Exception{
    testDistributed("queries/tpch_gd/02.sql", "tpch02");
  }

  @Ignore
  @Test
  public void tpch03() throws Exception{
    testDistributed("queries/tpch_gd/03.sql", "tpch03");
  }

  @Ignore
  @Test
  public void tpch04() throws Exception{
    testDistributed("queries/tpch_gd/04.sql", "tpch04");
  }

  @Ignore
  @Test
  public void tpch05() throws Exception{
    testDistributed("queries/tpch_gd/05.sql", "tpch05");
  }

  @Ignore
  @Test
  public void tpch06() throws Exception{
    testDistributed("queries/tpch_gd/06.sql", "tpch06");
  }

  @Ignore
  @Test
  public void tpch07() throws Exception{
    testDistributed("queries/tpch_gd/07.sql", "tpch07");
  }

  @Ignore
  @Test
  public void tpch08() throws Exception{
    testDistributed("queries/tpch_gd/08.sql", "tpch08");
  }

  @Ignore
  @Test
  public void tpch09() throws Exception{
    testDistributed("queries/tpch_gd/09.sql", "tpch09");
  }

  @Ignore
  @Test
  public void tpch10() throws Exception{
    testDistributed("queries/tpch_gd/10.sql", "tpch10");
  }

  @Ignore
  @Test
  public void tpch11() throws Exception{
    testDistributed("queries/tpch_gd/11.sql", "tpch11");
  }

  @Ignore
  @Test
  public void tpch12() throws Exception{
    testDistributed("queries/tpch_gd/12.sql", "tpch12");
  }

  @Ignore
  @Test
  public void tpch13() throws Exception{
    testDistributed("queries/tpch_gd/13.sql", "tpch13");
  }

  @Ignore
  @Test
  public void tpch14() throws Exception{
    testDistributed("queries/tpch_gd/14.sql", "tpch14");
  }

  @Ignore
  @Test
  public void tpch16() throws Exception{
    testDistributed("queries/tpch_gd/16.sql", "tpch16");
  }

  @Ignore
  @Test
  public void tpch17() throws Exception{
    testDistributed("queries/tpch_gd/17.sql", "tpch17");
  }

  @Ignore
  @Test
  public void tpch18() throws Exception{
    testDistributed("queries/tpch_gd/18.sql", "tpch18");
  }

  @Test
  @Ignore // non-equality join
  public void tpch19() throws Exception{
    testDistributed("queries/tpch_gd/19.sql", "tpch19");
  }

  @Ignore
  @Test
  public void tpch19_1() throws Exception{
    testDistributed("queries/tpch_gd/19_1.sql", "tpch19_1");
  }

  @Ignore
  @Test
  public void tpch20() throws Exception{
    testDistributed("queries/tpch_gd/20.sql", "tpch20");
  }

  @Ignore
  @Test
  public void tpch21() throws Exception{
    testDistributed("queries/tpch_gd/21.sql", "tpch21");
  }

  @Test
  @Ignore // DRILL-518
  public void tpch22() throws Exception{
    testDistributed("queries/tpch_gd/22.sql", "tpch22");
  }
}
