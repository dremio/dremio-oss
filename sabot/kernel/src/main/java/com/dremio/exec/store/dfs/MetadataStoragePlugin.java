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

import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;

import com.dremio.common.FSConstants;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshUtils;

/**
 * FileSystemPlugin for internal iceberg metadata tables
 */
public class MetadataStoragePlugin extends MayBeDistFileSystemPlugin<MetadataStoragePluginConfig> {
    public MetadataStoragePlugin(MetadataStoragePluginConfig config, SabotContext context, String name, Provider<StoragePluginId> idProvider) {
        super(config, context, name, idProvider);
    }

    @Override
    protected List<Property> getProperties() {
        List<Property> props = new ArrayList<>(super.getProperties());
        props.add(new Property(FSConstants.FS_S3A_FILE_STATUS_CHECK, Boolean.toString(getConfig().isS3FileStatusCheckEnabled())));
        props.add(new Property(ICEBERG_CATALOG_TYPE_KEY, IcebergCatalogType.NESSIE.name()));
        props.add(new Property(ICEBERG_NAMESPACE_KEY,
                getContext().getOptionManager().getOption(NESSIE_METADATA_NAMESPACE)));
        if (getConfig().getProperties() != null) {
          props.addAll(getConfig().getProperties());
        }
        return props;
    }

  @Override
  protected boolean ctasToUseIceberg() {
    return MetadataRefreshUtils.unlimitedSplitsSupportEnabled(getContext().getOptionManager());
  }

}
