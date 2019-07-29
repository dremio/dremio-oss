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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.orc.OrcConf;

import com.dremio.common.VM;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.store.StoragePlugin;
import com.google.common.base.Preconditions;

import io.protostuff.Tag;

/**
 * Base configuration for the Hive storage plugin
 */
public abstract class BaseHiveStoragePluginConfig<T extends ConnectionConf<T, P>, P extends StoragePlugin> extends ConnectionConf<T, P>{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseHiveStoragePluginConfig.class);
  private static final String DREMIO_SOURCE_CONFIGURATION_SOURCE = "Dremio source configuration";
  private static final String FS_S3_IMPL = "fs.s3.impl";
  private static final String FS_S3_IMPL_DEFAULT = "org.apache.hadoop.fs.s3a.S3AFileSystem";

  /*
   * Hostname where Hive metastore server is running
   */
  @Tag(1)
  @DisplayMetadata(label = "Hive Metastore Host")
  public String hostname;

  /*
   * Listening port of Hive metastore server
   */
  @Tag(2)
  @Min(1)
  @Max(65535)
  @DisplayMetadata(label = "Port")
  public int port = 9083;

  /*
   * Is kerberos authentication enabled on metastore services?
   */
  @Tag(3)
  @DisplayMetadata(label = "Enable SASL")
  public boolean enableSasl = false;

  /*
   * Kerberos principal name of metastore servers if kerberos authentication is enabled
   */
  @Tag(4)
  @DisplayMetadata(label = "Hive Kerberos Principal")
  public String kerberosPrincipal;

  /*
   * List of configuration properties.
   */
  @Tag(5)
  public List<Property> propertyList;

  public BaseHiveStoragePluginConfig() {
  }

  protected static HiveConf createHiveConf(BaseHiveStoragePluginConfig<?,?> config) {
    final HiveConf hiveConf = new HiveConf();

    final String metastoreURI = String.format("thrift://%s:%d", Preconditions.checkNotNull(config.hostname, "Hive hostname must be provided."), config.port);
    setConf(hiveConf, HiveConf.ConfVars.METASTOREURIS, metastoreURI);

    if (config.enableSasl) {
      setConf(hiveConf, HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, true);
      if (config.kerberosPrincipal != null) {
        setConf(hiveConf, HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, config.kerberosPrincipal);
      }
    }

    return hiveConf;
  }

  /**
   * Fills in a HiveConf instance with any user provided configuration parameters
   *
   * @param hiveConf - the conf to fill in
   * @param config - the user provided parameters
   * @return
   */
  protected static HiveConf addUserProperties(HiveConf hiveConf, BaseHiveStoragePluginConfig<?,?> config) {
    // Used to capture properties set by user
    final Set<String> userPropertyNames = new HashSet<>();
    if(config.propertyList != null) {
      for(Property prop : config.propertyList) {
        userPropertyNames.add(prop.name);
        setConf(hiveConf, prop.name, prop.value);
        if(logger.isTraceEnabled()){
          logger.trace("HiveConfig Override {}={}", prop.name, prop.value);
        }
      }
    }

    // Check if zero-copy has been set by user
    boolean zeroCopySetByUser = userPropertyNames.contains(OrcConf.USE_ZEROCOPY.getAttribute())
        || userPropertyNames.contains(HiveConf.ConfVars.HIVE_ORC_ZEROCOPY.varname);
    // Configure zero-copy for ORC reader
    if (!zeroCopySetByUser) {
      if (VM.isWindowsHost() || VM.isMacOSHost()) {
        logger.debug("MacOS or Windows host detected. Not automatically enabling ORC zero-copy feature");
      } else {
        String fs = hiveConf.get(FileSystem.FS_DEFAULT_NAME_KEY);
        // Equivalent to a case-insensitive startsWith...
        if (fs.regionMatches(true, 0, "maprfs", 0, 6)) {
          // DX-12672: do not enable ORC zero-copy on MapRFS
          logger.debug("MapRFS detected. Not automatically enabling ORC zero-copy feature");
        } else {
          logger.debug("Linux host detected. Enabling ORC zero-copy feature");
          setConf(hiveConf, HiveConf.ConfVars.HIVE_ORC_ZEROCOPY, true);
        }
      }
    } else {
      boolean useZeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(hiveConf);
      if (useZeroCopy) {
        logger.warn("ORC zero-copy feature has been manually enabled. This is not recommended.");
      } else {
        logger.error("ORC zero-copy feature has been manually disabled. This is not recommended and might cause memory issues");
      }
    }

    // Check if ORC Footer cache has been configured by user
    boolean orcStripCacheSetByUser = userPropertyNames.contains(HiveConf.ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_SIZE.varname);
    if (orcStripCacheSetByUser) {
      logger.error("ORC stripe details cache has been manually configured. This is not recommended and might cause memory issues");
    } else {
      logger.debug("Disabling ORC stripe details cache.");
      setConf(hiveConf, HiveConf.ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_SIZE, 0);
    }

    // Check if fs.s3.impl has been set by user
    boolean fsS3ImplSetByUser = userPropertyNames.contains(FS_S3_IMPL);
    if (fsS3ImplSetByUser) {
      logger.warn(FS_S3_IMPL + " manually set. This is not recommended.");
    } else {
      logger.debug("Setting " + FS_S3_IMPL + " to " + FS_S3_IMPL_DEFAULT);
      setConf(hiveConf, FS_S3_IMPL, FS_S3_IMPL_DEFAULT);
    }

    return hiveConf;
  }

  protected static void setConf(HiveConf configuration, String name, String value) {
    configuration.set(name, value, DREMIO_SOURCE_CONFIGURATION_SOURCE);
  }

  protected static void setConf(HiveConf configuration, HiveConf.ConfVars var, String value) {
    setConf(configuration, var.varname, value);
  }

  protected static void setConf(HiveConf configuration, HiveConf.ConfVars var, int value) {
    setConf(configuration, var.varname, Integer.toString(value));
  }

  protected static void setConf(HiveConf configuration, HiveConf.ConfVars var, boolean value) {
    setConf(configuration, var.varname, Boolean.toString(value));
  }

}
