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
package com.dremio.plugins.elastic.util;

import java.io.File;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.apache.http.HttpVersion;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class to create a SSL Proxy
 *
 * <p>It's a separate class so that jetty-proxy dependency can be made optional
 */
public final class ProxyServerFactory {
  private static final Logger logger = LoggerFactory.getLogger(ProxyServerFactory.class);

  private ProxyServerFactory() {}

  public static Server of(String proxyTo, int port, File keystoreFile, String keystorePassword) {
    Server proxy = new Server();
    logger.info("Setting up HTTPS connector for web server");

    final SslContextFactory sslContextFactory = new SslContextFactory.Client();

    sslContextFactory.setKeyStorePath(keystoreFile.getAbsolutePath());
    sslContextFactory.setKeyStorePassword(keystorePassword);

    // SSL Connector
    final ServerConnector sslConnector =
        new ServerConnector(
            proxy,
            new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.toString()),
            new HttpConnectionFactory(new HttpConfiguration()));
    // regular http connector if one needs to inspect the wire. Requires tweaking the
    // ElasticsearchPlugin to use http
    // final ServerConnector sslConnector = new ServerConnector(embeddedJetty,
    //   new HttpConnectionFactory(new HttpConfiguration()));
    sslConnector.setPort(port);
    proxy.addConnector(sslConnector);

    // root handler with request logging
    final RequestLogHandler rootHandler = new RequestLogHandler();
    proxy.setHandler(rootHandler);

    final ServletContextHandler servletContextHandler =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContextHandler.setContextPath("/");
    rootHandler.setHandler(servletContextHandler);

    // error handler
    ProxyServlet.Transparent proxyServlet =
        new ProxyServlet.Transparent() {
          @Override
          public void service(ServletRequest req, ServletResponse res)
              throws ServletException, IOException {
            try {
              HttpServletRequest hr = (HttpServletRequest) req;
              logger.debug(
                  "incoming {} {}://{}:{} {}",
                  hr.getMethod(),
                  req.getScheme(),
                  req.getServerName(),
                  req.getServerPort(),
                  hr.getRequestURL());
              super.service(req, res);
            } catch (Exception e) {
              logger.error("can't proxy " + req, e);
              throw new RuntimeException(e);
            }
          }

          @Override
          protected String rewriteTarget(HttpServletRequest clientRequest) {
            final String serverName = clientRequest.getServerName();
            final int serverPort = clientRequest.getServerPort();
            final String query = clientRequest.getQueryString();

            String result = super.rewriteTarget(clientRequest);

            logger.debug(
                "Proxying {}://{}:{}{} to {}\n",
                clientRequest.getScheme(),
                serverName,
                serverPort,
                query != null ? '?' + query : "",
                result);

            return result;
          }
        };
    // Rest API
    final ServletHolder proxyHolder = new ServletHolder(proxyServlet);
    proxyHolder.setInitParameter("proxyTo", proxyTo);
    proxyHolder.setInitOrder(1);

    servletContextHandler.addServlet(proxyHolder, "/*");

    return proxy;
  }
}
