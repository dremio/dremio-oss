/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.net.URISyntaxException;
import java.security.KeyStore;
import java.util.EnumSet;

import javax.inject.Provider;
import javax.servlet.DispatcherType;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import com.dremio.dac.daemon.DremioBinder;
import com.dremio.dac.daemon.ServerHealthMonitor;
import com.dremio.dac.server.socket.SocketServlet;
import com.dremio.dac.server.tokens.TokenManager;
import com.dremio.dac.support.SupportService;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.Service;
import com.dremio.service.SingletonRegistry;
import com.dremio.service.jobs.JobsService;
import com.google.common.annotations.VisibleForTesting;

import ch.qos.logback.access.jetty.RequestLogImpl;

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

  // After being created in the start() method a reference to this
  // ResourceConfig for the rest server is maintained so that
  // resources from the SabotNode can be directly injected into it
  // after the start up completes
  private final SingletonRegistry registry;
  private final Provider<RestServerV2> restServerProvider;
  private final Provider<APIServer> apiServerProvider;
  private final Server embeddedJetty = new Server();
  private final Provider<ServerHealthMonitor> serverHealthMonitor;
  private final DACConfig config;
  private final Provider<NodeEndpoint> endpointProvider;
  private final Provider<SabotContext> context;
  private final String uiType;
  private final boolean isInternalUS;

  private KeyStore trustStore;
  private AccessLogFilter accessLogFilter;
  private int port = -1;

  public WebServer(
      SingletonRegistry registry,
      DACConfig config,
      Provider<ServerHealthMonitor> serverHealthMonitor,
      Provider<NodeEndpoint> endpointProvider,
      Provider<SabotContext> context,
      Provider<RestServerV2> restServer,
      Provider<APIServer> apiServer,
      String uiType,
      boolean isInternalUS) {
    this.config = config;
    this.endpointProvider = endpointProvider;
    this.registry = registry;
    this.serverHealthMonitor = serverHealthMonitor;
    this.context = context;
    this.restServerProvider = restServer;
    this.apiServerProvider = apiServer;
    this.uiType = uiType;
    this.isInternalUS = isInternalUS;
  }

  public AccessLogFilter getAccessLogFilter() {
    return accessLogFilter;
  }

  @Override
  public void start() throws Exception {
    final ServerConnector serverConnector;
    if (config.webSSLEnabled()) {
      Pair<ServerConnector, KeyStore> connectorTrustStorePair =
          new HttpsConnectorGenerator().createHttpsConnector(embeddedJetty, config.getConfig(),
              config.thisNode, endpointProvider.get().getAddress());
      serverConnector = connectorTrustStorePair.getLeft();
      trustStore = connectorTrustStorePair.getRight();
    } else {
      serverConnector = new ServerConnector(embeddedJetty, new HttpConnectionFactory(new HttpConfiguration()));
    }
    if (!config.autoPort) {
      serverConnector.setPort(config.getHttpPort());
    }

    embeddedJetty.addConnector(serverConnector);

    // root handler with request logging
    final RequestLogHandler rootHandler = new RequestLogHandler();
    embeddedJetty.setHandler(rootHandler);
    RequestLogImpl requestLogger = new RequestLogImpl();
    requestLogger.setResource("/logback-access.xml");
    rootHandler.setRequestLog(requestLogger);

    // servlet handler for everything (to manage path mapping)
    final ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContextHandler.setContextPath("/");
    rootHandler.setHandler(servletContextHandler);

    // error handler
    final ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setShowMessageInTitle(true);
    servletContextHandler.setErrorHandler(errorHandler);

    // gzip filter.
    servletContextHandler.addFilter(GzipFilter.class.getName(), "/*", EnumSet.of(DispatcherType.REQUEST));

    // add the font mime type.
    final MimeTypes mimeTypes = servletContextHandler.getMimeTypes();
    mimeTypes.addMimeMapping("woff2", "application/font-woff2; charset=utf-8");
    servletContextHandler.setMimeTypes(mimeTypes);

    // WebSocket API
    final SocketServlet servlet = new SocketServlet(registry.lookup(JobsService.class), registry.lookup(TokenManager.class));
    final ServletHolder wsHolder = new ServletHolder(servlet);
    wsHolder.setInitOrder(3);
    servletContextHandler.addServlet(wsHolder, "/apiv2/socket");

    // Rest API
    ResourceConfig restServer = restServerProvider.get();

    restServer.property(RestServerV2.ERROR_STACKTRACE_ENABLE, config.sendStackTraceToClient);
    restServer.property(RestServerV2.TEST_API_ENABLE, config.allowTestApis);
    restServer.property(RestServerV2.FIRST_TIME_API_ENABLE, isInternalUS);

    restServer.register(new DremioBinder(registry));

    final ServletHolder restHolder = new ServletHolder(new ServletContainer(restServer));
    restHolder.setInitOrder(2);
    servletContextHandler.addServlet(restHolder, "/apiv2/*");

    // Public API
    ResourceConfig apiServer = apiServerProvider.get();
    apiServer.register(new DremioBinder(registry));

    final ServletHolder apiHolder = new ServletHolder(new ServletContainer(apiServer));
    apiHolder.setInitOrder(3);
    servletContextHandler.addServlet(apiHolder, "/api/v3/*");

    if (config.verboseAccessLog) {
      accessLogFilter = new AccessLogFilter();
      servletContextHandler.addFilter(
          new FilterHolder(accessLogFilter),
          "/*",
          EnumSet.of(DispatcherType.REQUEST));
    }

    if (config.serveUI) {
      final String basePath = "rest/dremio_static/";
      final String markerPath = String.format("META-INF/%s.properties", uiType);
      final SupportService support = registry.lookup(SupportService.class);

      final ServletHolder fallbackServletHolder = new ServletHolder("fallback-servlet",
          new DremioServlet(config, serverHealthMonitor.get(), context.get().getOptionManager(), support));
      addStaticPath(fallbackServletHolder, basePath, markerPath);
      servletContextHandler.addServlet(fallbackServletHolder, "/*");

      // TODO DX-1556 - temporary static asset serving for showing Profiles
      final String baseStaticPath = "rest/static/";
      final String arrowDownResourceRelativePath = "rest/static/img/arrow-down-small.svg";
      ServletHolder restStaticHolder = new ServletHolder("static", DefaultServlet.class);
      // Get resource URL for legacy static assets, based on where some image is located
      addStaticPath(restStaticHolder, baseStaticPath, arrowDownResourceRelativePath);
      servletContextHandler.addServlet(restStaticHolder, "/static/*");
    }

    embeddedJetty.start();

    port = serverConnector.getLocalPort();
    logger.info("Started on {}://localhost:" + port, config.webSSLEnabled() ? "https" : "http");
  }

  private void addStaticPath(ServletHolder holder, String basePath, String relativeMarkerPathToResource) throws URISyntaxException {
    String path = Resource.newClassPathResource(relativeMarkerPathToResource).getURL().toString();
    final String fullBasePath = path.substring(0, path.length() - relativeMarkerPathToResource.length()) + basePath;
    holder.setInitParameter("dirAllowed", "false");
    holder.setInitParameter("pathInfoOnly", "true");
    holder.setInitParameter("resourceBase", fullBasePath);
  }

  public int getPort() {
    return port;
  }

  /**
   * Get the trust store containing the self-signed certificate (not the private key).
   *
   * @return trust store, null if the cert is not generated by the server
   */
  @VisibleForTesting
  KeyStore getTrustStore() {
    return trustStore;
  }

  @Override
  public void close() throws Exception {
    embeddedJetty.stop();
  }
}
