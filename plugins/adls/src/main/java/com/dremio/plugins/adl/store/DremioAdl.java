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
package com.dremio.plugins.adl.store;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

/**
 * AbstractFileSystem implementation for DremioAdlFileSystem.
 */
public class DremioAdl extends DelegateToFileSystem {
  DremioAdl(URI theUri, Configuration conf) throws IOException, URISyntaxException {
    super(theUri, createDataLakeFileSystem(conf), conf, DremioAdlFileSystem.SCHEME, false);
  }

  private static DremioAdlFileSystem createDataLakeFileSystem(Configuration conf) {
    DremioAdlFileSystem fs = new DremioAdlFileSystem();
    fs.setConf(conf);
    return fs;
  }
}
