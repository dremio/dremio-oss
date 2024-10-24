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
package com.dremio.exec.store.iceberg;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.deletes.PositionalDeleteFileReader;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.util.TestUtilities;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Paths;
import java.util.function.Supplier;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Helper class for preparing Iceberg tables for tests. */
public final class IcebergTestTables {

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(IcebergTestTables.class);

  private IcebergTestTables() {
    // utility class
  }

  // TODO: refactor to have a "PregenIcebergTableDefinition" class knowing all the involved paths
  // upfront and having a "copyToKnownFolder" method returning an AutoCloseable "PregenIcebergTable"
  // object.

  public static final String ICEBERG_TEST_TABLES_ROOT_PATH =
      TestUtilities.ICEBERG_TEST_TABLES_ROOT_PATH;

  public static final Supplier<Table> NATION =
      () -> getTable("iceberg/nation", "dfs_hadoop", "/tmp/iceberg");
  public static final Supplier<Table> PARTITIONED_NATION =
      () -> getTable("iceberg/partitionednation", "dfs_hadoop", "/tmp/iceberg");

  public static final Supplier<Table> INCONSISTENT_PARTITIONS =
      () ->
          getTable(
              "iceberg/internal_metadata_inconsistent_partitions", "dfs_hadoop", "/tmp/iceberg");

  public static final String V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES_FULL_PATH =
      ICEBERG_TEST_TABLES_ROOT_PATH + "/v2/multi_rowgroup_orders_with_deletes";
  public static final Supplier<Table> V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES =
      () ->
          getTable(
              "iceberg/v2/multi_rowgroup_orders_with_deletes",
              "dfs_static_test_hadoop",
              ICEBERG_TEST_TABLES_ROOT_PATH,
              "/v2/multi_rowgroup_orders_with_deletes");

  private static final String V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES_LONG_SUBFOLDER =
      "/v2/longDir"
          + Strings.repeat("0", 100)
          + "/longDir_1_"
          + Strings.repeat("0", 100)
          + "/longDir_2_"
          + Strings.repeat("0", 100)
          + "/multi_rowgroup_orders_with_deletes";
  public static final String V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES_LONG_FULL_PATH =
      ICEBERG_TEST_TABLES_ROOT_PATH + V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES_LONG_SUBFOLDER;
  public static final Supplier<Table> V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES_LONG_PATH =
      () ->
          getTable(
              "iceberg" + V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES_LONG_SUBFOLDER,
              "dfs_static_test_hadoop",
              ICEBERG_TEST_TABLES_ROOT_PATH,
              V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES_LONG_SUBFOLDER);

  public static final Supplier<Table> MERGE_ON_READ_TARGET =
      () ->
          getTable(
              "iceberg/v2/Merge_On_Read_Target_Unpartitioned",
              "dfs_static_test_hadoop",
              ICEBERG_TEST_TABLES_ROOT_PATH,
              "/v2/Merge_On_Read_Target_Unpartitioned");

  public static final Supplier<Table> MERGE_ON_READ_TARGET_PARTITIONED =
      () ->
          getTable(
              "iceberg/v2/Merge_On_Read_Target_Partitioned",
              "dfs_static_test_hadoop",
              ICEBERG_TEST_TABLES_ROOT_PATH,
              "/v2/Merge_On_Read_Target_Partitioned");

  public static final Supplier<Table> MERGE_ON_READ_SOURCE =
      () ->
          getTable(
              "iceberg/v2/Merge_On_Read_Source_Partitioned",
              "dfs_static_test_hadoop",
              ICEBERG_TEST_TABLES_ROOT_PATH,
              "/v2/Merge_On_Read_Source_Partitioned");

  public static final String V2_ORDERS_FULL_PATH = ICEBERG_TEST_TABLES_ROOT_PATH + "/v2/orders";
  public static final Supplier<Table> V2_ORDERS =
      () ->
          getTable(
              "iceberg/v2/orders",
              "dfs_static_test_hadoop",
              ICEBERG_TEST_TABLES_ROOT_PATH,
              "/v2/orders");
  public static final String PRODUCTS_WITH_EQ_DELETES_FULL_PATH =
      ICEBERG_TEST_TABLES_ROOT_PATH + "/v2/products_with_eq_deletes";
  public static final Supplier<Table> PRODUCTS_WITH_EQ_DELETES =
      () ->
          getTable(
              "iceberg/v2/products_with_eq_deletes",
              "dfs_static_test_hadoop",
              ICEBERG_TEST_TABLES_ROOT_PATH,
              "/v2/products_with_eq_deletes");

  public static final BatchSchema V2_ORDERS_SCHEMA =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable("order_id", new ArrowType.Int(32, true)),
              Field.nullable("order_year", new ArrowType.Int(32, true)),
              Field.nullable("order_date", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
              Field.nullable("product_name", new ArrowType.Utf8()),
              Field.nullable(
                  "amount", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));

  public static final BatchSchema PRODUCTS_SCHEMA =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable("product_id", Types.MinorType.INT.getType()),
              Field.nullable("name", Types.MinorType.VARCHAR.getType()),
              Field.nullable("category", Types.MinorType.VARCHAR.getType()),
              Field.nullable("color", Types.MinorType.VARCHAR.getType()),
              Field.nullable("created_date", Types.MinorType.DATEMILLI.getType()),
              Field.nullable("weight", Types.MinorType.FLOAT8.getType()),
              Field.nullable("quantity", Types.MinorType.INT.getType())));

  public static final BatchSchema V2_ORDERS_DELETE_FILE_SCHEMA = PositionalDeleteFileReader.SCHEMA;

  public static Table getTable(
      String resourcePath, String source, String sourceRoot, String location) {
    try {
      return new Table(resourcePath, source, sourceRoot, location);
    } catch (Throwable t) {
      throw new RuntimeException("getTable failed for: " + resourcePath, t);
    }
  }

  public static Table getTable(String resourcePath, String source, String location) {
    return getTable(resourcePath, source, "", location);
  }

  /**
   * copies a pre-existing iceberg table from "resourcePath" to the given location. this is needed
   * since iceberg files contain hardcoded paths, so they can only be used from the locations they
   * were created in. since multiple tests might be trying to use the same folder a filelock is
   * guarding against race conditions.
   */
  public static class Table implements AutoCloseable {

    private final FileSystem fs;
    private final String sourceName;
    private final String location;
    private final String tableName;
    private FolderLock folderLock;

    public Table(String resourcePath, String source, String sourceRoot, String location)
        throws Exception {
      this.sourceName = source;
      this.location = sourceRoot + location;
      if (!this.location.startsWith("/tmp/")) {
        throw new IllegalArgumentException(
            "Well-known iceberg table location must start with /tmp/ but was: " + this.location);
      }
      this.tableName = source + location.replace('/', '.');

      Configuration conf = new Configuration();
      conf.set("fs.default.name", "local");
      this.fs = FileSystem.get(conf);

      Path path = new Path(this.location);

      // parent needs to exist for FolderLock
      fs.mkdirs(path.getParent());

      java.nio.file.Path testRootPath = Paths.get(this.location);
      folderLock = new FolderLock(testRootPath);

      // when we have the lock, clean out old files
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
      fs.mkdirs(path);

      BaseTestQuery.copyFromJar(resourcePath, testRootPath);
    }

    public FileSystem getFileSystem() {
      return fs;
    }

    public String getSourceName() {
      return sourceName;
    }

    public String getLocation() {
      return location;
    }

    public String getTableName() {
      return tableName;
    }

    @Override
    public void close() throws Exception {
      if (location != null) {
        fs.delete(new Path(location), true);
      }
      // intentionally not closing "fs"
      folderLock.close();
    }

    public org.apache.iceberg.Table getIcebergTable(IcebergModel icebergModel) {
      return icebergModel.getIcebergTable(icebergModel.getTableIdentifier(getLocation()));
    }
  }

  private static class FolderLock implements AutoCloseable {

    private final java.nio.file.Path lockFilePath;
    private final FileOutputStream fileOutputStream;
    private final FileChannel fileChannel;
    private final FileLock fileLock;

    FolderLock(java.nio.file.Path folderToLock) throws IOException {
      lockFilePath = folderToLock.resolveSibling(folderToLock.getFileName() + ".folderLock");
      LOGGER.info("Acquiring FolderLock: {} (folder: {})", lockFilePath, folderToLock);
      try {
        fileOutputStream = new FileOutputStream(lockFilePath.toFile());
        fileChannel = fileOutputStream.getChannel();
        fileLock = acquireLockWithRetries(fileChannel);
        LOGGER.info("Acquired FolderLock: {}", lockFilePath);
      } catch (Exception e) {
        throw new IOException("Failed to acquire FolderLock: " + lockFilePath);
      }
    }

    private static FileLock acquireLockWithRetries(FileChannel fileChannel)
        throws InterruptedException, IOException {
      for (int i = 0; i < 50; i++) {
        FileLock lock = fileChannel.tryLock();
        if (lock != null) {
          return lock;
        }
        Thread.sleep(5_000);
      }
      throw new IOException("Failed to acquire lock after retries!");
    }

    @Override
    public void close() throws Exception {
      LOGGER.info("Releasing FolderLock: {}", lockFilePath);
      AutoCloseables.close(fileLock, fileChannel, fileOutputStream);
      LOGGER.info("Released FolderLock: {}", lockFilePath);
    }
  }
}
