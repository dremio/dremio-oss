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
package com.dremio.dac.api;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.Host;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import io.protostuff.Tag;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import javax.inject.Provider;

/** Fake source for testing. */
@SourceType(value = "FAKESOURCE", label = "FakeSource")
public class FakeSource extends ConnectionConf<FakeSource, StoragePlugin> {
  /** Test fake enum */
  public enum TestEnum {
    @DisplayMetadata(label = "Enum #1")
    ENUM_1,
    @DisplayMetadata(label = "Enum #2")
    ENUM_2
  }

  @Tag(1)
  public String username;

  @Tag(2)
  @Secret
  public String password;

  @Tag(3)
  public int numeric = 4;

  @Tag(4)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Awesome!")
  public boolean isAwesome = true;

  @Tag(5)
  public List<String> valueList;

  @Tag(6)
  public List<Host> hostList;

  @Tag(7)
  public List<Property> propList;

  @Tag(8)
  public AuthenticationType authenticationType;

  @Tag(9)
  public Source invalidType;

  @Tag(10)
  public TestEnum enumType;

  /** Fake source no-op implementation */
  class FakeSourcePlugin implements StoragePlugin {
    private final FakeSource fakeSource;
    private final String name;

    public FakeSourcePlugin(FakeSource fakeSource, String name) {
      this.fakeSource = fakeSource;
      this.name = name;
    }

    @Override
    public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
      return false;
    }

    @Override
    public SourceState getState() {
      return SourceState.GOOD;
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
      return null;
    }

    @Override
    public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
      return null;
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
      return null;
    }

    @Override
    public void start() throws IOException {
      // password has to equal name
      if (!this.fakeSource.password.equals(this.name)) {
        throw new IOException("Invalid password!");
      }
    }

    @Override
    public void close() {}

    @Override
    public Optional<DatasetHandle> getDatasetHandle(
        EntityPath datasetPath, GetDatasetOption... options) {
      return Optional.empty();
    }

    @Override
    public DatasetMetadata getDatasetMetadata(
        DatasetHandle datasetHandle,
        PartitionChunkListing chunkListing,
        GetMetadataOption... options)
        throws ConnectorException {
      throw new ConnectorException("Invalid handle.");
    }

    @Override
    public PartitionChunkListing listPartitionChunks(
        DatasetHandle datasetHandle, ListPartitionChunkOption... options)
        throws ConnectorException {
      throw new ConnectorException("Invalid handle.");
    }

    @Override
    public boolean containerExists(EntityPath containerPath) {
      return false;
    }
  }

  @Override
  public StoragePlugin newPlugin(
      SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new FakeSourcePlugin(this, name);
  }
}
