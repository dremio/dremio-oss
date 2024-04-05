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

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.deltalake.DeltaLakeFormatConfig;
import com.dremio.exec.store.deltalake.DeltaLakeFormatPlugin;
import com.dremio.exec.store.dfs.AsyncStreamConf;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.iceberg.IcebergFormatConfig;
import com.dremio.exec.store.iceberg.IcebergFormatPlugin;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/** Base class which all hive storage plugins extend */
public abstract class BaseHiveStoragePlugin implements SupportsIcebergRootPointer {

  // Advanced option to set default ctas format
  public static final String HIVE_DEFAULT_CTAS_FORMAT = "hive.default.ctas.format";

  private final SabotContext sabotContext;
  private final String name;

  protected BaseHiveStoragePlugin(SabotContext sabotContext, String pluginName) {
    this.sabotContext = sabotContext;
    this.name = pluginName;
  }

  protected String getName() {
    return name;
  }

  public SabotContext getSabotContext() {
    return sabotContext;
  }

  public FileSystem createFS(
      FileSystem fs, OperatorContext operatorContext, AsyncStreamConf cacheAndAsyncConf)
      throws IOException {
    return this.sabotContext
        .getFileSystemWrapper()
        .wrap(
            fs,
            this.getName(),
            cacheAndAsyncConf,
            operatorContext,
            cacheAndAsyncConf.isAsyncEnabled(),
            false);
  }

  protected final void runQuery(final String query, final String userName, final String queryType)
      throws Exception {
    sabotContext.getJobsRunner().get().runQueryAsJob(query, userName, queryType, null);
  }

  @Override
  public Configuration getFsConfCopy() {
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> property : getConfigProperties()) {
      conf.set(property.getKey(), property.getValue());
    }
    return conf;
  }

  public abstract String getConfEntry(String key, String defaultValue);

  @Override
  public boolean isMetadataValidityCheckRecentEnough(
      Long lastMetadataValidityCheckTime, Long currentTime, OptionManager optionManager) {
    final long metadataAggressiveExpiryTime =
        Long.parseLong(
                getConfEntry(
                    PlannerSettings.METADATA_EXPIRY_CHECK_INTERVAL_SECS.getOptionName(),
                    String.valueOf(
                        optionManager.getOption(
                            PlannerSettings.METADATA_EXPIRY_CHECK_INTERVAL_SECS))))
            * 1000;
    return (lastMetadataValidityCheckTime != null
        && (lastMetadataValidityCheckTime + metadataAggressiveExpiryTime
            >= currentTime)); // dataset metadata validity was checked too long ago (or never)
  }

  @Override
  public FormatPlugin getFormatPlugin(FormatPluginConfig formatConfig) {
    if (formatConfig instanceof IcebergFormatConfig) {
      IcebergFormatPlugin icebergFormatPlugin =
          new IcebergFormatPlugin(
              "iceberg", sabotContext, (IcebergFormatConfig) formatConfig, null);
      icebergFormatPlugin.initialize((IcebergFormatConfig) formatConfig, this);
      return icebergFormatPlugin;
    }
    if (formatConfig instanceof DeltaLakeFormatConfig) {
      return new DeltaLakeFormatPlugin(
          "delta", sabotContext, (DeltaLakeFormatConfig) formatConfig, null);
    }
    throw new UnsupportedOperationException(
        "Format plugins for non iceberg use cases are not supported.");
  }

  public abstract Iterable<Map.Entry<String, String>> getConfigProperties();

  public String getDefaultCtasFormat() {
    return getConfEntry(HIVE_DEFAULT_CTAS_FORMAT, null);
  }
}
