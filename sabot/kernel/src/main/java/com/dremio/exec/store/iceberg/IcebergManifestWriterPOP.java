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
package com.dremio.exec.store.iceberg;

import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.StoragePluginResolver;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Iceberg writer POP
 */
@JsonTypeName("iceberg-manifest-writer")
public class IcebergManifestWriterPOP extends AbstractWriter {
    private final String location;
    private final MutablePlugin plugin;

    @JsonCreator
    public IcebergManifestWriterPOP(
            @JsonProperty("props") OpProps props,
            @JsonProperty("child") PhysicalOperator child,
            @JsonProperty("location") String location,
            @JsonProperty("options") WriterOptions options,
            @JsonProperty("pluginId") StoragePluginId pluginId,
            @JacksonInject StoragePluginResolver storagePluginResolver
    ) {
        super(props, child, options);
        this.plugin = storagePluginResolver.getSource(pluginId);
        this.location = location;
    }

    public IcebergManifestWriterPOP(
            OpProps props,
            PhysicalOperator child,
            String location,
            WriterOptions options,
            MutablePlugin plugin) {
        super(props, child, options);
        this.plugin = plugin;
        this.location = location;
    }

    @JsonProperty("location")
    public String getLocation() {
        return location;
    }

    public StoragePluginId getPluginId() {
        return plugin.getId();
    }

    @Override
    protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
        return new IcebergManifestWriterPOP(props, child, location, options, plugin);
    }

    @Override
    public int getOperatorType() {
        return UserBitShared.CoreOperatorType.ICEBERG_MANIFEST_WRITER_VALUE;
    }

    @JsonIgnore
    public MutablePlugin getPlugin() {
        return plugin;
    }
}
