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
package com.dremio.dac.daemon;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.dac.server.DACConfig;
import com.dremio.datastore.KVStoreProviderType;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.NoopKVStoreProvider;
import com.dremio.datastore.RemoteKVStoreProvider;
import com.dremio.datastore.TracingKVStoreProvider;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.services.fabric.api.FabricService;

import io.opentracing.Tracer;

/**
 * KVStoreProvider helper class.
 * Selects appropriate KVStoreProvider depending on the datastore type defined in the dremio.conf.
 * If default or TestClusterDB, determines if the provider should be Local or Remote.
 * Otherwise, matches the datastore.type to the KVStoreProvider implementation that defined it.
 */
public class KVStoreProviderHelper {
  private static final Logger logger = LoggerFactory.getLogger(KVStoreProviderHelper.class);
  public static final String KVSTORE_TYPE_PROPERTY_NAME = "dremio.kvstore.type";
  private static final String KVSTORE_HOSTNAME_PROPERTY_NAME = "dremio.kvstore.hostname";
  private static final String DEFAULT_DB = "default";
  private static final String TEST_CLUSTER_DB = "TestClusterDB";

  public static KVStoreProvider newKVStoreProvider(DACConfig dacConfig,
                                                   BootStrapContext bootstrap,
                                                   Provider<FabricService> fabricService,
                                                   Provider<NodeEndpoint> endPoint,
                                                   Tracer tracer) {

    return new TracingKVStoreProvider(internalKVStoreProvider(dacConfig, bootstrap, fabricService, endPoint), tracer);
  }

  private static KVStoreProvider internalKVStoreProvider(DACConfig dacConfig,
                                                               BootStrapContext bootstrap,
                                                               Provider<FabricService> fabricService,
                                                               Provider<NodeEndpoint> endPoint) {
    DremioConfig dremioConfig = dacConfig.getConfig();
    Map<String, Object> config = new HashMap<>();
    String thisNode = dremioConfig.getThisNode();

    // instantiate NoopKVStoreProvider on all non-coordinator nodes.
    boolean isCoordinator = dremioConfig.getBoolean(DremioConfig.ENABLE_COORDINATOR_BOOL);
    if (!isCoordinator) {
      return new NoopKVStoreProvider(bootstrap.getClasspathScan(), fabricService, endPoint, bootstrap.getAllocator(), config);
    }

    // Configure the default KVStore
    String datastoreType = System.getProperty(KVSTORE_TYPE_PROPERTY_NAME, DEFAULT_DB);
    config.put(DremioConfig.DEBUG_USE_MEMORY_STRORAGE_BOOL, dacConfig.inMemoryStorage);
    config.put(LocalKVStoreProvider.CONFIG_DISABLEOCC, "false");
    config.put(LocalKVStoreProvider.CONFIG_VALIDATEOCC, "true");
    config.put(LocalKVStoreProvider.CONFIG_TIMED, "true");
    config.put(LocalKVStoreProvider.CONFIG_BASEDIRECTORY, dremioConfig.getString(DremioConfig.DB_PATH_STRING));
    config.put(LocalKVStoreProvider.CONFIG_HOSTNAME, System.getProperty(KVSTORE_HOSTNAME_PROPERTY_NAME, thisNode));
    config.put(RemoteKVStoreProvider.HOSTNAME, thisNode);
    config.put(DremioConfig.REMOTE_DATASTORE_RPC_TIMEOUT_SECS, dremioConfig.getLong(DremioConfig.REMOTE_DATASTORE_RPC_TIMEOUT_SECS));

    // find the appropriate KVStoreProvider from path
    // first check for the default behavior (if services.datastore.type is set to "default")
    // if services.datastore.type is set, check ClassPath for associated KVStoreProvider type
    Class<? extends KVStoreProvider> cls = null;
    switch (datastoreType) {
      case DEFAULT_DB:
        config.put(LocalKVStoreProvider.CONFIG_HOSTNAME, thisNode);
       // fall through to TEST_CLUSTER_DB
      case TEST_CLUSTER_DB:
        boolean isMasterless = dremioConfig.isMasterlessEnabled();
        boolean isMaster = (!isMasterless && isCoordinator && dremioConfig.getBoolean(DremioConfig.ENABLE_MASTER_BOOL));
        boolean needsLocalKVStore = (isMasterless && thisNode.equals(config.get(LocalKVStoreProvider.CONFIG_HOSTNAME)));
        cls = (isMaster || needsLocalKVStore)? LocalKVStoreProvider.class : RemoteKVStoreProvider.class;
        break;

      default:
        final ScanResult results = ClassPathScanner.fromPrescan(dremioConfig.getSabotConfig());
        final Set<Class<? extends KVStoreProvider>> classes = results.getImplementations(KVStoreProvider.class);
        for (Class<? extends KVStoreProvider> it : classes) {
          try {
            KVStoreProviderType anno = it.getAnnotation(KVStoreProviderType.class);
            if (anno != null && anno.type().equals(datastoreType)) {
              cls = it;
              break;
            }
          } catch (Exception e) {
            logger.info(String.format("Unable to find KVStoreProviderType annotation in %s during search, skipping", cls.getName()));
            continue;
          }
        }
        break;
    }

    // not able to find a KVStoreProvider for the requested services.datastore.type
    if (cls == null) {
      throw new RuntimeException("Unable to find appropriate KVStoreProvider for " + datastoreType);
    }

    try {
      final Constructor<? extends KVStoreProvider> con = cls.getDeclaredConstructor(
        ScanResult.class,
        Provider.class,
        Provider.class,
        BufferAllocator.class,
        Map.class
      );

      return con.newInstance(bootstrap.getClasspathScan(),
                             fabricService,
                             endPoint,
                             bootstrap.getAllocator(),
                             config
      );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
