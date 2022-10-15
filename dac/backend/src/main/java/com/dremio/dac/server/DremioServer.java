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

import java.net.InetAddress;
import java.net.URISyntaxException;
import java.security.KeyStore;
import java.util.EnumSet;
import java.util.function.Consumer;

import javax.inject.Provider;
import javax.servlet.DispatcherType;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

import com.dremio.service.SingletonRegistry;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.telemetry.api.tracing.http.ServerTracingFilter;
import com.google.common.annotations.VisibleForTesting;

/**
 * Provides Dremio web server
 */
public class DremioServer {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioServer.class);

  private final Server embeddedJetty;
  private Provider<CredentialsService> credentialsServiceProvider;

  private KeyStore trustStore;
  private AccessLogFilter accessLogFilter;
  private int port = -1;
  private ServerConnector serverConnector;
  private ServletContextHandler servletContextHandler;
  private boolean serviceStarted = false;

  public DremioServer() {
    embeddedJetty = new Server();
  }

  public AccessLogFilter getAccessLogFilter() {
    return accessLogFilter;
  }

  public void startDremioServer(
    SingletonRegistry registry,
    DACConfig config,
    Provider<CredentialsService> credentialsServiceProvider,
    String uiType,
    Consumer<ServletContextHandler> servletRegistrer
  ) throws Exception {
    this.credentialsServiceProvider = credentialsServiceProvider;
    try {
      if (!embeddedJetty.isRunning()) {
        createConnector(config);
        addHandlers();
      }

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

        final ServletHolder fallbackServletHolder = new ServletHolder("fallback-servlet", registry.lookup(DremioServlet.class));
        addStaticPath(fallbackServletHolder, basePath, markerPath);
        servletContextHandler.addServlet(fallbackServletHolder, "/*");
      }

      if (servletRegistrer != null) {
        servletRegistrer.accept(servletContextHandler);
      }

      if (!embeddedJetty.isRunning()) {
        embeddedJetty.start();
      }

      setPortFromConnector();
      logger.info("Started on {}://localhost:" + port, config.webSSLEnabled() ? "https" : "http");

      serviceStarted = true;
    } catch (Exception ex) {
      throw new ServerErrorException(ex);
    }
  }

  protected void createConnector(DACConfig config) throws Exception {
    if (config.webSSLEnabled()) {
      Pair<ServerConnector, KeyStore> connectorTrustStorePair =
        new HttpsConnectorGenerator().createHttpsConnector(embeddedJetty, config.getConfig(),
          credentialsServiceProvider,
          config.thisNode, InetAddress.getLocalHost().getCanonicalHostName());
      serverConnector = connectorTrustStorePair.getLeft();
      trustStore = connectorTrustStorePair.getRight();
    } else {
      final HttpConfiguration configuration = new HttpConfiguration();
      configuration.setSendServerVersion(false);
      serverConnector = new ServerConnector(embeddedJetty, new HttpConnectionFactory(configuration));
    }
    if (!config.autoPort) {
      serverConnector.setPort(config.getHttpPort());
    }

    embeddedJetty.addConnector(serverConnector);
  }

  protected void addHandlers() {
    // root handler with request logging
    final RequestLogHandler rootHandler = new RequestLogHandler();
    embeddedJetty.insertHandler(rootHandler);
    RequestLogImpl_Jetty_Fix requestLogger = new RequestLogImpl_Jetty_Fix();
    requestLogger.setResource("/logback-access.xml");
    rootHandler.setRequestLog(requestLogger);

    // gzip handler.
    final GzipHandler gzipHandler = new GzipHandler();
    // gzip handler interferes with ChunkedOutput, so exclude the job download path
    gzipHandler.addExcludedPaths("/apiv2/job/*", "/api/v3/support-bundle/*");
    rootHandler.setHandler(gzipHandler);

    // servlet handler for everything (to manage path mapping)
    servletContextHandler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContextHandler.setContextPath("/");
    gzipHandler.setHandler(servletContextHandler);

    // error handler
    final ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setShowMessageInTitle(true);
    errorHandler.setShowServlet(false);
    embeddedJetty.setErrorHandler(errorHandler);
  }

  protected void addStaticPath(ServletHolder holder, String basePath, String relativeMarkerPathToResource) throws URISyntaxException {
    String path = Resource.newClassPathResource(relativeMarkerPathToResource).getURL().toString();
    final String fullBasePath = path.substring(0, path.length() - relativeMarkerPathToResource.length()) + basePath;
    holder.setInitParameter("dirAllowed", "false");
    holder.setInitParameter("pathInfoOnly", "true");
    holder.setInitParameter("resourceBase", fullBasePath);
  }

  public int getPort() {
    return port;
  }

  protected void setPortFromConnector() {
    port = serverConnector.getLocalPort();
  }

  protected ServletContextHandler getServletContextHandler() {
    return servletContextHandler;
  }

  protected boolean isServiceStarted() {
    return serviceStarted;
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

  public Server getJettyServer() {
    return embeddedJetty;
  }

  public void close() throws Exception {
    embeddedJetty.stop();
  }

  /**
   * Wrapper extension to tracing filter to enable registration of class instead of instance.
   * Registering an object prevents context injection. Hence registering a class.
   */
  public static class TracingFilter extends ServerTracingFilter {
    public TracingFilter() {
      super(true, "x-tracing-enabled");
    }
  }
}
