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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG;
import static org.apache.iceberg.Transactions.createTableTransaction;
import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LockManagers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;

public abstract class BaseIcebergTable extends BaseTestQuery {

  protected static File tableFolder;

  protected static TableOperations ops;
  protected static final String tableName = "icebergtable";

  @ClassRule
  public static final TemporaryFolder tempDir = new TemporaryFolder();

  @BeforeClass
  public static void createIcebergTable() throws Exception {
    setSystemOption(ENABLE_ICEBERG, "true");
    final Schema icebergSchema = new Schema(
      optional(1, "col1", Types.ListType.ofOptional(7, Types.IntegerType.get())),
      optional(2, "col2", Types.StructType.of(optional(8, "x", Types.IntegerType.get()),
        optional(9, "y", Types.LongType.get()))),
      optional(3, "col3", Types.ListType.ofOptional(10, Types.FloatType.get())),
      optional(4, "col4", Types.StructType.of(optional(11, "x", Types.FloatType.get()),
        optional(12, "y", Types.LongType.get()))),
      optional(5, "col5", Types.ListType.ofOptional(13,
        Types.DecimalType.of(5,3).asPrimitiveType())),
      optional(6, "col6", Types.StructType.of(optional(14, "x",
          Types.DecimalType.of(5,3).asPrimitiveType()),
        optional(15, "y", Types.LongType.get())))
    );

    final PartitionSpec spec = PartitionSpec.unpartitioned();

    final String icebergFolder = tempDir.newFolder().getAbsolutePath();
    tableFolder = new File(icebergFolder, tableName);
    tableFolder.mkdirs();

    final TableMetadata metadata = TableMetadata.newTableMetadata(icebergSchema, spec, tableFolder.toString(),
      Collections.emptyMap());
    ops = new TestHadoopTableOperations(new Path(tableFolder.toPath().toUri()),
      new Configuration());

    Transaction transaction = createTableTransaction(tableName, ops, metadata);
    transaction.commitTransaction();

    final String parquetFile1 = TestTools.getWorkingPath() + "/src/test/resources/iceberg/complex/file1.parquet";

    final DataFile df1 = DataFiles.builder(spec)
      .withPath(parquetFile1)
      .withFileSizeInBytes(3003)
      .withRecordCount(1)
      .build();
    transaction = Transactions.newTransaction(tableName, ops);
    transaction.newAppend().appendFile(df1).commit();
    transaction.commitTransaction();

    Thread.sleep(1001);

    final DataFile df2 = DataFiles.builder(spec)
      .withPath(parquetFile1)
      .withFileSizeInBytes(3003)
      .withRecordCount(1)
      .build();
    transaction = Transactions.newTransaction(tableName, ops);
    transaction.newAppend().appendFile(df2).commit();
    transaction.commitTransaction();

    runSQL("alter table dfs_hadoop." + "\"" + tableFolder.toPath() + "\"" + " refresh metadata");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    setSystemOption(ENABLE_ICEBERG, "false");
  }

  private static final class TestHadoopTableOperations extends HadoopTableOperations {
    public TestHadoopTableOperations(Path location, Configuration conf) {
      super(location, new HadoopFileIO(conf), conf, LockManagers.defaultLockManager());
    }
  }

}
