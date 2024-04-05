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

package com.dremio.exec.store.hive.deltalake;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class DeltaHiveInputFormat extends FileInputFormat<NullWritable, ArrayWritable> {
  static final String DELTA_STORAGE_HANDLER = "io.delta.hive.DeltaStorageHandler";
  static final String SPARK_SQL_SOURCES_PROVIDER = "spark.sql.sources.provider";
  static final String DELTA = "delta";
  static final String PATH = "path";

  public static boolean isDeltaTable(Table table) {
    return isDeltaByStorageHandler(table) || isDeltaBySparkFormat(table);
  }

  public static String getLocation(Table table) {
    if (isDeltaBySparkFormat(table)) {
      return table.getSd().getSerdeInfo().getParameters().get(PATH);
    }
    return table.getSd().getLocation();
  }

  private static boolean isDeltaByStorageHandler(Table table) {
    return DELTA_STORAGE_HANDLER.equalsIgnoreCase(table.getParameters().get(META_TABLE_STORAGE));
  }

  private static boolean isDeltaBySparkFormat(Table table) {
    return DELTA.equalsIgnoreCase(table.getParameters().get(SPARK_SQL_SOURCES_PROVIDER));
  }

  public DeltaHiveInputFormat() {
    super();
  }

  @Override
  public RecordReader<NullWritable, ArrayWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
    return null;
  }
}
