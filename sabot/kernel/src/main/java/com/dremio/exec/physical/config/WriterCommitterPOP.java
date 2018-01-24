/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.namespace.StoragePluginId;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

@JsonTypeName("writer-committer")
public class WriterCommitterPOP extends AbstractSingle {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WriterCommitterPOP.class);

  private final String tempLocation;
  private final String finalLocation;
  private final FileSystemPlugin plugin;

  @JsonCreator
  public WriterCommitterPOP(
          @JsonProperty("tempLocation") String tempLocation,
          @JsonProperty("finalLocation") String finalLocation,
          @JsonProperty("userName") String userName,
          @JsonProperty("pluginId") StoragePluginId pluginId,
          @JsonProperty("child") PhysicalOperator child,
          @JacksonInject StoragePluginRegistry engineRegistry
  ) throws ExecutionSetupException {
      super(child, userName);
      this.tempLocation = tempLocation;
      this.finalLocation = finalLocation;
      this.plugin = Preconditions.checkNotNull((FileSystemPlugin) engineRegistry.getPlugin(pluginId));
  }

  public WriterCommitterPOP(
      String tempLocation,
      String finalLocation,
      String userName,
      FileSystemPlugin plugin,
      PhysicalOperator child) {
    super(child, userName);
    this.plugin = Preconditions.checkNotNull(plugin);
    this.tempLocation = tempLocation;
    this.finalLocation = finalLocation;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
      return physicalVisitor.visitWriterCommiter(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new WriterCommitterPOP(tempLocation, finalLocation, getUserName(), plugin, child);
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
  public FileSystemPlugin getPlugin(){
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
}
