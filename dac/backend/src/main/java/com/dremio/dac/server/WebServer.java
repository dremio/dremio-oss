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

import java.security.KeyStore;

import javax.inject.Provider;

import com.dremio.dac.daemon.DremioBinder;
import com.dremio.dac.daemon.ServerHealthMonitor;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.Service;
import com.dremio.service.SingletonRegistry;
import com.google.common.annotations.VisibleForTesting;

import io.opentracing.Tracer;

/**
 * Dremio web server.
 */
public class WebServer implements Service {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WebServer.class);

  /**
   * Specific media types used by Dremio
   */
  public static final class MediaType extends javax.ws.rs.core.MediaType {
    public MediaType(String type, String subType) {
      super(type, subType);
    }

    /**
     * Qlik Sense app media type string
     */
    public static final String TEXT_PLAIN_QLIK_APP = "text/plain+qlik-app";

    /**
     * Qlik Sense app media type
     */
    public static final MediaType TEXT_PLAIN_QLIK_APP_TYPE = new MediaType("text", "plain+qlik-app");


    /**
     * Tableau TDS media type string
     */
    public static final String APPLICATION_TDS = "application/tds";
    /**
     * Tableau TDS media type
     */
    public static final MediaType APPLICATION_TDS_TYPE = new MediaType("application", "tds");

    /**
     * Tableau TDS media type string (for Native Drill Connector)
     */
    public static final String APPLICATION_TDS_DRILL = "application/tds+drill";

    /**
     * Tableau TDS media type (for Native Drill Connector)
     */
    public static final MediaType APPLICATION_TDS_DRILL_TYPE = new MediaType("application", "tds+drill");
  }

  /**
   * Dremio hostname to use for response (usually match the Host header field).
   */
  public static final String X_DREMIO_HOSTNAME = "x-dremio-hostname";

  /**
   * Dremio header that forces numbers to be returned as strings for job data
   *
   * That header is also hardcoded in {@link /dac/ui/src/utils/apiUtils/apiUtils.js} for client side
   */
  public static final String X_DREMIO_JOB_DATA_NUMBERS_AS_STRINGS = "x-dremio-job-data-number-format";
  public static final String X_DREMIO_JOB_DATA_NUMBERS_AS_STRINGS_SUPPORTED_VALUE = "number-as-string";

  // After being created in the start() method a reference to this
  // ResourceConfig for the rest server is maintained so that
  // resources from the SabotNode can be directly injected into it
  // after the start up completes
  private final SingletonRegistry registry;
  private final Provider<RestServerV2> restServerProvider;
  private final Provider<APIServer> apiServerProvider;
  private final DremioServer server;
  private final Provider<ServerHealthMonitor> serverHealthMonitor;
  private final DACConfig config;
  private final Provider<NodeEndpoint> endpointProvider;
  private final Provider<SabotContext> context;
  private final DremioBinder dremioBinder;
  private final Tracer tracer;
  private final String uiType;
  private final boolean isInternalUS;

  public WebServer(
      SingletonRegistry registry,
      DACConfig config,
      Provider<ServerHealthMonitor> serverHealthMonitor,
      Provider<NodeEndpoint> endpointProvider,
      Provider<SabotContext> context,
      Provider<RestServerV2> restServer,
      Provider<APIServer> apiServer,
      Provider<DremioServer> server,
      DremioBinder dremioBinder,
      Tracer tracer,
      String uiType,
      boolean isInternalUS) {
    this.config = config;
    this.endpointProvider = endpointProvider;
    this.registry = registry;
    this.serverHealthMonitor = serverHealthMonitor;
    this.context = context;
    this.restServerProvider = restServer;
    this.apiServerProvider = apiServer;
    this.dremioBinder = dremioBinder;
    this.tracer = tracer;
    this.uiType = uiType;
    this.isInternalUS = isInternalUS;
    this.server = server.get();
  }

  public AccessLogFilter getAccessLogFilter() {
    return server.getAccessLogFilter();
  }

  @Override
  public void start() throws Exception {
    server.startDremioServer(
      registry,
      config,
      serverHealthMonitor,
      endpointProvider,
      context,
      restServerProvider,
      apiServerProvider,
      dremioBinder,
      tracer,
      uiType,
      isInternalUS
    );
  }

  public int getPort() {
    return server.getPort();
  }

  /**
   * Get the trust store containing the self-signed certificate (not the private key).
   *
   * @return trust store, null if the cert is not generated by the server
   */
  @VisibleForTesting
  KeyStore getTrustStore() {
    return server.getTrustStore();
  }

  @Override
  public void close() throws Exception {
    server.close();
  }
}
