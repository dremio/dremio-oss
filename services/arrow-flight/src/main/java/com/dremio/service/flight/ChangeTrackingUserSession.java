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
package com.dremio.service.flight;

import java.util.List;
import java.util.Objects;

import org.apache.calcite.avatica.util.Quoting;

import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.exec.work.user.SubstitutionSettings;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.InboundImpersonationManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Tracks if an underlying UserSession was updated so that it can
 * be saved to the user session cache.
 */
public class ChangeTrackingUserSession extends UserSession {
  private final UserSession delegate;
  private boolean updated = false;

  public ChangeTrackingUserSession(UserSession delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean isSupportComplexTypes() {
    return delegate.isSupportComplexTypes();
  }

  @Override
  public OptionManager getOptions() {
    return delegate.getOptions();
  }

  @Override
  public void setSessionOptionManager(SessionOptionManager sessionOptionManager, OptionManager fallback) {
    delegate.setSessionOptionManager(sessionOptionManager, fallback);
  }

  @Override
  public SessionOptionManager getSessionOptionManager() {
    return delegate.getSessionOptionManager();
  }

  @Override
  public String getRoutingTag() {
    return delegate.getRoutingTag();
  }

  @Override
  public String getRoutingQueue() {
    return delegate.getRoutingQueue();
  }

  @Override
  public void setRoutingQueue(String queueName) {
    final String oldRoutingQueue = delegate.getRoutingQueue();
    delegate.setRoutingQueue(queueName);
    updated = !Objects.equals(oldRoutingQueue, delegate.getRoutingQueue());
  }

  @Override
  public String getRoutingEngine() {
    return delegate.getRoutingEngine();
  }

  @Override
  public void setRoutingEngine(String routingEngine) {
    final String oldRoutingEngine = delegate.getRoutingEngine();
    delegate.setRoutingEngine(routingEngine);
    updated = !Objects.equals(oldRoutingEngine, delegate.getRoutingEngine());
  }

  @Override
  public String getEngine() {
    return delegate.getEngine();
  }

  @Override
  public void setEngine(String routingEngine) {
    final String oldRoutingEngine = delegate.getEngine();
    delegate.setEngine(routingEngine);
    updated = !Objects.equals(oldRoutingEngine, delegate.getEngine());
  }

  @Override
  public UserBitShared.UserCredentials getCredentials() {
    return delegate.getCredentials();
  }

  @Override
  public UserBitShared.RpcEndpointInfos getClientInfos() {
    return delegate.getClientInfos();
  }

  @Override
  public UserProtos.RecordBatchFormat getRecordBatchFormat() {
    return delegate.getRecordBatchFormat();
  }

  @Override
  public boolean exposeInternalSources() {
    return delegate.exposeInternalSources();
  }

  @Override
  public boolean isTracingEnabled() {
    return delegate.isTracingEnabled();
  }

  @Override
  public SubstitutionSettings getSubstitutionSettings() {
    return delegate.getSubstitutionSettings();
  }

  @Override
  public String getCatalogName() {
    return delegate.getCatalogName();
  }

  @Override
  public boolean useLegacyCatalogName() {
    return delegate.useLegacyCatalogName();
  }

  @Override
  public int getMaxMetadataCount() {
    return delegate.getMaxMetadataCount();
  }

  @Override
  public boolean supportFullyQualifiedProjections() {
    return delegate.supportFullyQualifiedProjections();
  }

  @Override
  public void replaceUserCredentials(InboundImpersonationManager impersonationManager, UserBitShared.UserCredentials newCredentials) {
    delegate.replaceUserCredentials(impersonationManager, newCredentials);
    updated = true;
  }

  @Override
  public String getTargetUserName() {
    return delegate.getTargetUserName();
  }

  @Override
  public String getDefaultSchemaName() {
    return delegate.getDefaultSchemaName();
  }

  @Override
  public void incrementQueryCount() {
    delegate.incrementQueryCount();
  }

  @Override
  public Quoting getInitialQuoting() {
    return delegate.getInitialQuoting();
  }

  @Override
  public void setDefaultSchemaPath(List<String> newDefaultSchemaPath) {
    final NamespaceKey oldNamespaceKey = delegate.getDefaultSchemaPath();
    delegate.setDefaultSchemaPath(newDefaultSchemaPath);
    updated = !Objects.equals(oldNamespaceKey, delegate.getDefaultSchemaPath());
  }

  @Override
  public NamespaceKey getDefaultSchemaPath() {
    return delegate.getDefaultSchemaPath();
  }

  @Override
  public UserBitShared.QueryId getLastQueryId() {
    return delegate.getLastQueryId();
  }

  @Override
  public void setLastQueryId(UserBitShared.QueryId id) {
    final UserBitShared.QueryId oldQueryId = delegate.getLastQueryId();
    delegate.setLastQueryId(id);
    updated = !Objects.equals(oldQueryId, delegate.getLastQueryId());
  }

  @Override
  public VersionContext getSessionVersionForSource(String sourceName) {
    return delegate.getSessionVersionForSource(sourceName);
  }

  @Override
  public CaseInsensitiveMap<VersionContext> getSourceVersionMapping() {
    return delegate.getSourceVersionMapping();
  }

  @Override
  public void setSessionVersionForSource(String sourceName, VersionContext versionContext) {
    delegate.setSessionVersionForSource(sourceName, versionContext);
    updated = true;
  }

  public boolean isUpdated() {
    return updated;
  }
}
