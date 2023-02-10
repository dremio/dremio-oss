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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dremio.BaseTestQuery;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.deletes.PositionalDeleteFileReader;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

/**
 * Helper class for preparing Iceberg tables for tests.
 */
public class IcebergTestTables {

  public static Supplier<Table> NATION = () -> getTable("iceberg/nation", "dfs_hadoop", "/tmp/iceberg");
  public static Supplier<Table> PARTITIONED_NATION = () -> getTable("iceberg/partitionednation", "dfs_hadoop", "/tmp/iceberg");

  public static Supplier<Table> V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES = () ->
    getTable("iceberg/v2/multi_rowgroup_orders_with_deletes",
      "dfs_static_test_hadoop",
      "/tmp/iceberg-test-tables",
      "/v2/multi_rowgroup_orders_with_deletes");
  public static Supplier<Table> V2_ORDERS = () -> getTable("iceberg/v2/orders",
      "dfs_static_test_hadoop",
      "/tmp/iceberg-test-tables",
      "/v2/orders");
  public static Supplier<Table> PRODUCTS_WITH_EQ_DELETES = () -> getTable(
      "iceberg/v2/products_with_eq_deletes",
      "dfs_static_test_hadoop",
      "/tmp/iceberg-test-tables",
      "/v2/products_with_eq_deletes");

  public static BatchSchema V2_ORDERS_SCHEMA = new BatchSchema(ImmutableList.of(
    Field.nullable("order_id", new ArrowType.Int(32, true)),
    Field.nullable("order_year", new ArrowType.Int(32, true)),
    Field.nullable("order_date", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
    Field.nullable("product_name", new ArrowType.Utf8()),
    Field.nullable("amount", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));

  public static BatchSchema PRODUCTS_SCHEMA = new BatchSchema(ImmutableList.of(
      Field.nullable("product_id", Types.MinorType.INT.getType()),
      Field.nullable("name", Types.MinorType.VARCHAR.getType()),
      Field.nullable("category", Types.MinorType.VARCHAR.getType()),
      Field.nullable("color", Types.MinorType.VARCHAR.getType()),
      Field.nullable("created_date", Types.MinorType.DATEMILLI.getType()),
      Field.nullable("weight", Types.MinorType.FLOAT8.getType()),
      Field.nullable("quantity", Types.MinorType.INT.getType())));

  public static BatchSchema V2_ORDERS_DELETE_FILE_SCHEMA = PositionalDeleteFileReader.SCHEMA;

  private static Table getTable(String resourcePath, String source, String sourceRoot, String location) {
    try {
      return new Table(resourcePath, source, sourceRoot, location);
    } catch (Exception ex) {
      ex.printStackTrace();
      return null;
    }
  }

  private static Table getTable(String resourcePath, String source, String location) {
    return getTable(resourcePath, source, "", location);
  }

  public static class Table implements AutoCloseable {

    private final FileSystem fs;
    private final String location;
    private final String tableName;
    private AutoCloseable icebergOptionsScope;

    public Table(String resourcePath, String source, String sourceRoot, String location) throws Exception {
      Configuration conf = new Configuration();
      conf.set("fs.default.name", "local");
      this.fs = FileSystem.get(conf);
      this.location = sourceRoot + location;
      this.tableName = source + location.replace('/', '.');

      copyFromJar(resourcePath, sourceRoot + location);
    }

    public void enableIcebergSystemOptions() {
      this.icebergOptionsScope = BaseTestQuery.enableIcebergTables();
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
      if (icebergOptionsScope != null) {
        icebergOptionsScope.close();
      }
    }

    private void copyFromJar(String src, String testRoot) throws IOException, URISyntaxException {
      Path path = new Path(testRoot);
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
      fs.mkdirs(path);

      URI resource = Resources.getResource(src).toURI();
      // if resources are in a jar and not exploded on disk, we need to use a filesystem wrapper on top of the jar
      // to enumerate files in the table directory
      if (resource.getScheme().equals("jar")) {
        try (java.nio.file.FileSystem fileSystem = FileSystems.newFileSystem(resource, Collections.emptyMap())) {
          src = !src.startsWith("/") ?  "/" + src : src;
          java.nio.file.Path srcDir = fileSystem.getPath(src);
          try (Stream<java.nio.file.Path> stream = Files.walk(srcDir)) {
            stream.forEach(source -> {
              java.nio.file.Path dest = Paths.get(testRoot).resolve(Paths.get(srcDir.relativize(source).toString()));
              copy(source, dest);
            });
          }
        }
      } else {
        java.nio.file.Path srcDir = Paths.get(resource);
        try (Stream<java.nio.file.Path> stream = Files.walk(srcDir)) {
          stream.forEach(source -> copy(source, Paths.get(testRoot).resolve(srcDir.relativize(source))));
        }
      }
    }

    private void copy(java.nio.file.Path source, java.nio.file.Path dest) {
      try {
        Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }
}
