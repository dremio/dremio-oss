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
package com.dremio.exec.store.hive;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

/**
 * PF4J plugin entry point class for Hive.
 */
public class HivePf4jPlugin extends Plugin {

  static {
    try (ContextClassLoaderSwapper contextClassLoaderSwapper = ContextClassLoaderSwapper.newInstance()) {
      try {
        final Configuration conf = new Configuration();
        // Forcibly load FileSystems within the plugin classloader.
        // This is to guarantee that the when the plugin reads the set of available plugins from the ServiceLoader
        // the first time, it does so within the plugin's ClassLoader (using the context class loader).
        // This static initializer is the first block of code the plugin class loader runs as its PF4J's entry point.
        FileSystem.getFileSystemClass("hdfs", conf);

        // Also force compression codecs to be loaded within the plugin classloader
        CompressionCodecFactory.getCodecClasses(conf);
      } catch (IOException ignore) {}
    }
  }

  public HivePf4jPlugin(PluginWrapper wrapper) {
    super(wrapper);
  }
}
