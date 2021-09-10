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
package com.dremio.plugins.sysflight;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtils;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.ImmutableList;

import io.grpc.ManagedChannel;

/**
 * Plugin for System tables using Flight protocol
 */
public class SysFlightStoragePlugin implements StoragePlugin, SupportsListingDatasets {
  static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SysFlightStoragePlugin.class);

  private final SysFlightPluginConf conf;
  private final SabotContext context;
  private final String name;
  private final Boolean useConduit;
  private final BufferAllocator allocator;
  private FlightClient flightClient;
  private final Predicate<String> userPredicate;

  public SysFlightStoragePlugin(SysFlightPluginConf conf, SabotContext context, String name, Boolean useConduit, Predicate<String> userPredicate) {
    this.conf = conf;
    this.context = context;
    this.name = name;
    this.useConduit = useConduit;
    this.userPredicate = userPredicate;
    allocator = context.getAllocator().newChildAllocator(SysFlightStoragePlugin.class.getName(), 0, Long.MAX_VALUE);
  }

  public FlightClient getFlightClient() {
    if (flightClient == null) {
      synchronized (this) {
        if (flightClient == null) {
          NodeEndpoint ep;
          if (useConduit) {
            ep = context.getEndpoint();
          } else {
            ep = conf.endpoint;
          }
          final ManagedChannel channel = context.getConduitProvider().getOrCreateChannel(ep);
          flightClient = FlightGrpcUtils.createFlightClientWithSharedChannel(allocator, channel);
        }
      }
    }
    return flightClient;
  }
  SysFlightStoragePlugin(SysFlightPluginConf conf, SabotContext context, String name, Boolean useConduit) {
    this(conf, context, name, useConduit, s -> true);
  }

  public SabotContext getSabotContext() {
    return context;
  }

  public SysFlightPluginConf getPluginConf() {
    return conf;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {

    return this.userPredicate.test(user);
  }

  @Override
  public SourceState getState() {
    return SourceState.GOOD;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return SysFlightRulesFactory.class;
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public void start() { }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(flightClient, allocator);
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
    Iterable<FlightInfo> flightInfos = getFlightClient().listFlights(Criteria.ALL);

    return () -> StreamSupport.stream(flightInfos.spliterator(), false)
      .map(input -> {
        final EntityPath path = new EntityPath(ImmutableList.of(name, input.getDescriptor().getPath().get(0)));
        return getDataset(path).get();
      }).iterator();
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) {
    return (Optional<DatasetHandle>) (Object) getDataset(datasetPath);
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
    DatasetHandle datasetHandle,
    PartitionChunkListing chunkListing,
    GetMetadataOption... options
  ) {
    return datasetHandle.unwrap(SysFlightTable.class);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options) {
    return datasetHandle.unwrap(SysFlightTable.class);
  }

  @Override
  public boolean containerExists(EntityPath containerPath) {
    return false;
  }

  private Optional<SysFlightTable> getDataset(EntityPath entityPath) {
    final List<String> components = entityPath.getComponents();
    if(components.size() != 2) {
      return Optional.empty();
    }
    return Optional.of(new SysFlightTable(entityPath, getFlightClient()));
  }

  private static EntityPath canonicalize(EntityPath entityPath) {
    return new EntityPath(entityPath.getComponents().stream().map(String::toLowerCase).collect(Collectors.toList()));
  }
}
