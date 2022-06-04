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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.util.LockManagers;

import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Hadoop based iceberg table operations
 */
public class IcebergHadoopTableOperations extends HadoopTableOperations {
  private final MutablePlugin plugin;

  public IcebergHadoopTableOperations(Path location, Configuration conf, FileSystem fs, OperatorContext context, MutablePlugin plugin) {
    super(location, new DremioFileIO(fs, context, null, null, null, conf, plugin), conf,
      LockManagers.defaultLockManager());
    this.plugin = plugin;
  }

  @Override
  protected org.apache.hadoop.fs.FileSystem getFileSystem(Path path, Configuration hadoopConf) {
    return plugin.getHadoopFsSupplier(path.toString(), hadoopConf).get();
  }
}
