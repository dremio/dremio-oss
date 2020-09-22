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

package com.dremio.exec.physical.config;

import java.util.Iterator;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

@JsonTypeName("writer-committer")
public class WriterCommitterPOP extends AbstractSingle {

  private final String tempLocation;
  private final String finalLocation;
  private final FileSystemPlugin<?> plugin;
  private final IcebergTableProps icebergTableProps;

  @JsonCreator
  public WriterCommitterPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("tempLocation") String tempLocation,
      @JsonProperty("finalLocation") String finalLocation,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("icebergTableProps") IcebergTableProps icebergTableProps,
      @JsonProperty("child") PhysicalOperator child,
      @JacksonInject CatalogService catalogService
      ) {
    super(props, child);
    this.tempLocation = tempLocation;
    this.finalLocation = finalLocation;
    this.icebergTableProps = icebergTableProps;
    this.plugin = Preconditions.checkNotNull(catalogService.<FileSystemPlugin<?>>getSource(pluginId));
  }

  public WriterCommitterPOP(
      OpProps props,
      String tempLocation,
      String finalLocation,
      IcebergTableProps icebergTableProps,
      PhysicalOperator child,
      FileSystemPlugin<?> plugin
      ) {
    super(props, child);
    this.tempLocation = tempLocation;
    this.finalLocation = finalLocation;
    this.plugin = Preconditions.checkNotNull(plugin);
    this.icebergTableProps = icebergTableProps;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
      return physicalVisitor.visitWriterCommiter(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new WriterCommitterPOP(props, tempLocation, finalLocation, icebergTableProps, child, plugin);
  }

  public String getTempLocation() {
    return tempLocation;
  }

  public String getFinalLocation() {
    return finalLocation;
  }

  public StoragePluginId getPluginId() {
    return plugin.getId();
  }

  @JsonIgnore
  public FileSystemPlugin<?> getPlugin(){
    return plugin;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
      return Iterators.singletonIterator(child);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.WRITER_COMMITTER_VALUE;
  }

  public IcebergTableProps getIcebergTableProps() {
    return icebergTableProps;
  }
}
