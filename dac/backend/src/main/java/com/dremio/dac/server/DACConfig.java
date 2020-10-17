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
package com.dremio.dac.server;

import com.dremio.common.config.SabotConfig;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.DACDaemon.ClusterMode;

/**
 * Daemon configuration pojo facade. Look to replace with tscfg
 */
public final class DACConfig {

  public final String thisNode;
  public final boolean autoPort;
  public final boolean sendStackTraceToClient;
  public final boolean prettyPrintJSON;
  public final boolean verboseAccessLog;
  public final boolean allowTestApis;
  public final boolean serveUI;
  public final boolean webSSLEnabled;
  public final boolean prepopulate;
  public final boolean addDefaultUser;
  public final boolean allowNewerKVStore;

  // private due to checkstyle.
  private final ClusterMode clusterMode;
  public final int localPort;
  public final boolean inMemoryStorage;
  public final boolean isMaster;
  public final boolean isRemote;

  private final DremioConfig config;

  public DACConfig(DremioConfig config) {
    // default values
    this(
      config.getThisNode(),
      config.getBoolean(DremioConfig.DEBUG_AUTOPORT_BOOL),
      config.getBoolean(DremioConfig.DEBUG_ENABLED_BOOL),
      config.getBoolean(DremioConfig.DEBUG_ENABLED_BOOL),
      config.getBoolean(DremioConfig.DEBUG_ENABLED_BOOL),
      config.getBoolean(DremioConfig.DEBUG_ALLOW_TEST_APIS_BOOL),
      config.getBoolean(DremioConfig.WEB_ENABLED_BOOL),
      config.getBoolean(DremioConfig.WEB_SSL_PREFIX + DremioConfig.SSL_ENABLED),
      config.getBoolean(DremioConfig.DEBUG_PREPOPULATE_BOOL),
      config.getBoolean(DremioConfig.DEBUG_SINGLE_NODE_BOOL) ? ClusterMode.LOCAL : ClusterMode.DISTRIBUTED,
      config.getInt(DremioConfig.SERVER_PORT_INT),
      config.getBoolean(DremioConfig.DEBUG_USE_MEMORY_STRORAGE_BOOL),
      config.getBoolean(DremioConfig.ENABLE_COORDINATOR_BOOL) && config.getBoolean(DremioConfig.ENABLE_MASTER_BOOL),
      config.getBoolean(DremioConfig.DEBUG_FORCE_REMOTE_BOOL),
      config.getBoolean(DremioConfig.DEBUG_ADD_DEFAULT_USER),
      config.getBoolean(DremioConfig.DEBUG_ALLOW_NEWER_KVSTORE),
      config
     );
  }

  private DACConfig(
    String thisNode,
    boolean autoPort,
    boolean sendStackTraceToClient,
    boolean prettyPrintJSON,
    boolean verboseAccessLog,
    boolean allowTestApis,
    boolean serveUI,
    boolean webSSLEnabled,
    boolean prepopulate,
    ClusterMode clusterMode,
    int localPort,
    boolean inMemoryStorage,
    boolean isMaster,
    boolean forceRemote,
    boolean addDefaultUser,
    boolean allowNewerKVStore,
    DremioConfig config
  ) {
    super();
    this.thisNode = thisNode;
    this.autoPort = autoPort;
    this.sendStackTraceToClient = sendStackTraceToClient;
    this.prettyPrintJSON = prettyPrintJSON;
    this.verboseAccessLog = verboseAccessLog;
    this.allowTestApis = allowTestApis;
    this.serveUI = serveUI;
    this.webSSLEnabled = webSSLEnabled;
    this.prepopulate = prepopulate;
    this.clusterMode = clusterMode;
    this.inMemoryStorage = inMemoryStorage;
    this.localPort = localPort;
    this.config = config;
    this.isMaster = isMaster;
    this.isRemote = forceRemote;
    this.addDefaultUser = addDefaultUser;
    this.allowNewerKVStore = allowNewerKVStore;
  }

  public DACConfig with(String path, Object value){
    return new DACConfig(config.withValue(path, value));
  }

  public DACConfig debug(boolean debug) {
    return with(DremioConfig.DEBUG_ENABLED_BOOL, debug);
  }

  public DACConfig jobServerEnabled(boolean enabled) {
    return with(DremioConfig.JOBS_ENABLED_BOOL, enabled);
  }

  public DACConfig noOpClusterCoordinatorEnabled(boolean enabled) {
    return with(DremioConfig.NO_OP_CLUSTER_COORDINATOR_ENABLED, enabled);
  }

  public DACConfig autoPort(boolean autoPort) {
    return with(DremioConfig.DEBUG_AUTOPORT_BOOL, autoPort);
  }

  public int getHttpPort(){
    return config.getInt(DremioConfig.WEB_PORT_INT);
  }

  public DACConfig allowTestApis(boolean allowTestApis) {
    return with(DremioConfig.DEBUG_ALLOW_TEST_APIS_BOOL, allowTestApis);
  }

  public DACConfig addDefaultUser(boolean addDefaultUser) {
    return with(DremioConfig.DEBUG_ADD_DEFAULT_USER, addDefaultUser);
  }

  public DACConfig serveUI(boolean serveUI) {
    return with(DremioConfig.WEB_ENABLED_BOOL, serveUI);
  }

  public DACConfig webSSLEnabled(boolean webSSLEnabled) {
    return with(DremioConfig.WEB_SSL_PREFIX + DremioConfig.SSL_ENABLED, webSSLEnabled);
  }

  public DACConfig prepopulate(boolean prepopulate) {
    return with(DremioConfig.DEBUG_PREPOPULATE_BOOL, prepopulate);
  }

  public DACConfig writePath(String writePath) {
    return with(DremioConfig.LOCAL_WRITE_PATH_STRING, writePath);
  }

  public DACConfig distWritePath(String writePath) {
    return with(DremioConfig.DIST_WRITE_PATH_STRING, writePath);
  }

  public DACConfig clusterMode(ClusterMode clusterMode) {
    return with(DremioConfig.DEBUG_SINGLE_NODE_BOOL, clusterMode == ClusterMode.LOCAL);
  }

  public DACConfig localPort(int port) {
    return with(DremioConfig.SERVER_PORT_INT, port);
  }

  public DACConfig zk(String quorum) {
    return with(DremioConfig.ZOOKEEPER_QUORUM, quorum);
  }

  public DACConfig httpPort(int httpPort) {
    return with(DremioConfig.WEB_PORT_INT, httpPort);
  }

  public DACConfig inMemoryStorage(boolean inMemoryStorage) {
    return with(DremioConfig.DEBUG_USE_MEMORY_STRORAGE_BOOL, inMemoryStorage);
  }

  public DACConfig isMaster(boolean isMaster) {
    return with(DremioConfig.ENABLE_MASTER_BOOL, isMaster);
  }

  public DACConfig isRemote(boolean isRemote) {
    return with(DremioConfig.DEBUG_FORCE_REMOTE_BOOL, isRemote);
  }

  public boolean isAutoUpgrade() {
    return config.getBoolean(DremioConfig.AUTOUPGRADE);
  }

  public DACConfig autoUpgrade(boolean value) {
    return with(DremioConfig.AUTOUPGRADE, value);
  }

  public boolean webSSLEnabled() {
    return webSSLEnabled;
  }

  public ClusterMode getClusterMode() {
    return this.clusterMode;
  }

  public DremioConfig getConfig(){
    return config;
  }

  public static DACConfig newConfig() {
    return new DACConfig(DremioConfig.create());
  }

  public static DACConfig newDebugConfig(SabotConfig config) {
    return new DACConfig(
        DremioConfig.create(null, config)
        ).debug(true);
  }

  public boolean isMigrationEnabled() {
    return config.hasPath(DremioConfig.MIGRATION_ENABLED) && config.getBoolean(DremioConfig.MIGRATION_ENABLED);
  }
}
