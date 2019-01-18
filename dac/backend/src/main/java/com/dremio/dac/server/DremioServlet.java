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

import java.io.IOException;

import javax.inject.Provider;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.servlet.DefaultServlet;

import com.dremio.dac.daemon.ServerHealthMonitor;
import com.dremio.dac.support.SupportService;
import com.dremio.options.OptionManager;

/**
 * Servlet that hosts static assets but mapped to specific extensions. If the static assets are not available, falls
 * back to the index servlet.
 */
public class DremioServlet implements Servlet {

  private final IndexServlet indexServlet;
  private final DefaultServlet staticResources = new DefaultServlet();

  private static final String[] EXTENSIONS = { ".jpg", ".js", ".png", ".woff2", ".ttf", ".svg", ".css", ".ico", ".js.map" };

  public DremioServlet(DACConfig config, Provider<ServerHealthMonitor> serverHealthMonitor, Provider<OptionManager> optionManager, Provider<SupportService> supportService) {
    this.indexServlet = new IndexServlet(config, serverHealthMonitor, optionManager, supportService);
  }

  @Override
  public void init(ServletConfig config) throws ServletException {
    indexServlet.init(config);
    staticResources.init(config);
  }

  @Override
  public ServletConfig getServletConfig() {
    return indexServlet.getServletConfig();
  }

  @Override
  public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
    if(!(req instanceof HttpServletRequest)){
      indexServlet.service(req, res);
    }

    HttpServletRequest httpReq = (HttpServletRequest) req;
    String requestUri = httpReq.getRequestURI();

    for (String extension : EXTENSIONS) {
      if (requestUri.endsWith(extension)) {
        staticResources.service(httpReq, res);
        return;
      }
    }

    serveIndex(req, res);
  }

  protected void serveIndex(ServletRequest req, ServletResponse res) throws ServletException, IOException {
    indexServlet.service(req, res);
  }

  @Override
  public String getServletInfo() {
    return "";
  }

  @Override
  public void destroy() {
    indexServlet.destroy();
    staticResources.destroy();
  }
}
