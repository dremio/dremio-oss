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

package com.dremio.exec.store.hive.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * FileIO implementation for reading iceberg table metadata.
 */
public class HiveFileIO implements FileIO {
  private final Configuration hiveConf;

  public HiveFileIO(Configuration hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public InputFile newInputFile(String s) {
    return new HiveInputFile(s, hiveConf);
  }

  @Override
  public OutputFile newOutputFile(String s) {
    return null;
  }

  @Override
  public void deleteFile(String s) {

  }
}
