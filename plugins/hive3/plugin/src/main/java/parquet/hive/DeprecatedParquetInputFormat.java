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
package parquet.hive;

import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.io.ArrayWritable;

import org.apache.parquet.hadoop.ParquetInputFormat;

/**
 * Deprecated name of the parquet-hive input format. This class exists
 * simply to provide backwards compatibility with users who specified
 * this name in the Hive metastore. All users should now use
 * STORED AS PARQUET
 */

/**
 * CDH has support for tables with DeprecatedParquetInputFormat, and it should be in parquet.hive package.
 * Creating this placeholder class so that Dremio can mark such Hive tables as native parquet format tables.
 * Actual read happens using Dremio parquet readers
 */
@Deprecated
public class DeprecatedParquetInputFormat extends MapredParquetInputFormat {

  public DeprecatedParquetInputFormat() {
    super();
  }

  public DeprecatedParquetInputFormat(final ParquetInputFormat<ArrayWritable> realInputFormat) {
    super();
  }
}
