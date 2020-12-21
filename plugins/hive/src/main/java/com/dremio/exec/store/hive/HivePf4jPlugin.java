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
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

import com.dremio.common.util.Closeable;
import com.dremio.common.util.concurrent.ContextClassLoaderSwapper;
import com.dremio.exec.util.GuavaPatcher;

/**
 * PF4J plugin entry point class for Hive.
 */
public class HivePf4jPlugin extends Plugin {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(HivePf4jPlugin.class);

  static {
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      // Patching Guava for HBase 1.x compatibility
      try {
        GuavaPatcher.patchClassLoader(Thread.currentThread().getContextClassLoader());
      } catch (Exception ignored) {
        LOGGER.warn("Could not patch Guava for backward compatibility", ignored);
      }

      // Try to access UserGroupInformation in the context of the plugin classloader
      try {
        UserGroupInformation.getLoginUser();
      } catch (IOException ignored) {
        LOGGER.warn("Could not login into Hadoop", ignored);
      }

      final Configuration conf = new Configuration();

      try {
        // Forcibly load FileSystems within the plugin classloader.
        // This is to guarantee that the when the plugin reads the set of available plugins from the ServiceLoader
        // the first time, it does so within the plugin's ClassLoader (using the context class loader).
        // This static initializer is the first block of code the plugin class loader runs as its PF4J's entry point.
        FileSystem.getFileSystemClass("hdfs", conf);
      } catch (IOException ignore) {}

      try {
        // SecurityUtil needs to be force loaded within the Hive plugin
        Class.forName("org.apache.hadoop.security.SecurityUtil");
      } catch (ClassNotFoundException ignore) {}

      // Also force compression codecs to be loaded within the plugin classloader
      CompressionCodecFactory.getCodecClasses(conf);
    }
  }

  public HivePf4jPlugin(PluginWrapper wrapper) {
    super(wrapper);
  }

  /**
   * Current thread's class loader is replaced with the class loader
   * used to load hive plugin
   */
  public static Closeable swapClassLoader() {
    return ContextClassLoaderSwapper.swapClassLoader(HivePf4jPlugin.class);
  }

  public static void unregisterMetricMBean() {
    // DX-25305 - Call AwsSdkMetrics::unregisterMetricAdminMBean to free classloader references for Glue sources
    // Reflection is used to avoid addition of a dependency to AwsSdkMetrics
    try (Closeable ccls = swapClassLoader()) {
      Class<?> awsClass = HivePf4jPlugin.class.getClassLoader().loadClass("com.amazonaws.metrics.AwsSdkMetrics");
      Method method = awsClass.getMethod("unregisterMetricAdminMBean");
      method.invoke(null);
    } catch (Exception ex) {
      LOGGER.debug("Ignoring exception:", ex);
    }
  }
}
