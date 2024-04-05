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
package com.dremio.exec.store.dfs;

import static com.dremio.exec.ExecConstants.ICEBERG_CATALOG_TYPE_KEY;
import static com.dremio.exec.ExecConstants.ICEBERG_NAMESPACE_KEY;
import static com.dremio.exec.ExecConstants.NESSIE_METADATA_NAMESPACE;

import com.dremio.common.FSConstants;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.io.file.DistStorageMetadataPathRewritingFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Provider;

/** FileSystemPlugin for internal iceberg metadata tables */
public class MetadataStoragePlugin extends MayBeDistFileSystemPlugin<MetadataStoragePluginConfig> {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MetadataStoragePlugin.class);

  public MetadataStoragePlugin(
      MetadataStoragePluginConfig config,
      SabotContext context,
      String name,
      Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
  }

  @Override
  protected List<Property> getProperties() {
    List<Property> props = new ArrayList<>(super.getProperties());
    props.add(
        new Property(
            FSConstants.FS_S3A_FILE_STATUS_CHECK,
            Boolean.toString(getConfig().isS3FileStatusCheckEnabled())));
    props.add(new Property(ICEBERG_CATALOG_TYPE_KEY, IcebergCatalogType.NESSIE.name()));
    props.add(
        new Property(
            ICEBERG_NAMESPACE_KEY,
            getContext().getOptionManager().getOption(NESSIE_METADATA_NAMESPACE)));
    if (getConfig().getProperties() != null) {
      props.addAll(getConfig().getProperties());
    }
    return props;
  }

  @Override
  public void dropTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions) {
    try {
      TableMutationOptions metadataPluginTableMutationOptions =
          TableMutationOptions.newBuilder()
              .from(tableMutationOptions)
              .setIsLayered(true)
              .setShouldDeleteCatalogEntry(true)
              .build();
      super.dropTable(tableSchemaPath, schemaConfig, metadataPluginTableMutationOptions);
    } catch (Exception e) {
      logger.debug("Couldn't delete internal iceberg metadata table", e);
    }
  }

  /**
   * Before calling super, the method checks if unlimited-splits distributed-storage
   * metadata-relocation is enabled.
   */
  @Override
  public FileSystem createFS(String userName, OperatorContext operatorContext, boolean metadata)
      throws IOException {
    if (getContext()
        .getOptionManager()
        .getOption(ExecConstants.ENABLE_UNLIMITED_SPLITS_DISTRIBUTED_STORAGE_RELOCATION)) {
      FileSystem f = super.createFS(userName, operatorContext, metadata);
      Path configPath = this.getConfig().getPath();
      return new DistStorageMetadataPathRewritingFileSystem(f, configPath);
    } else {
      return super.createFS(userName, operatorContext, metadata);
    }
  }
}
