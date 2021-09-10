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
package com.dremio.exec.store.iceberg.hadoop;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopTableOperations;

import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Hadoop based iceberg table operations
 */
class IcebergHadoopTableOperations extends HadoopTableOperations {

  public IcebergHadoopTableOperations(Path location, Configuration conf, FileSystem fs, OperatorContext context, List<String> dataset) {
    super(location, new DremioFileIO(fs, context, dataset, null, null, conf), conf);
  }

}
