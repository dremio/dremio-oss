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
package com.dremio.dac.server;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.eclipse.jetty.util.resource.Resource;

import com.dremio.dac.daemon.ServerHealthMonitor;
import com.dremio.dac.server.ServerData.ClientSettings;
import com.dremio.dac.support.SupportService;
import com.dremio.exec.server.options.OptionManager;
import com.google.common.collect.ImmutableMap;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

/**
 * Simple servlet to always return the index.html page.
 * <p/>
 * As the application is written completely in javascript the web server returns
 * only data from the REST API, and static assets. To allow for page reloads,
 * the index page needs to be returned for any URL. See {@link WebServer} for
 * where this servlet is configured to resolve to anything that does not resolve
 * to a REST API or static asset. The correct page will be shown to the user
 * based on client side routing and page loading.
 */
class IndexServlet implements Servlet {
  private final Configuration templateCfg;
  private final ServerHealthMonitor serverHealthMonitor;
  private final OptionManager options;
  private final DacConfig config;

  private ServletConfig servletConfig;

  public IndexServlet(DacConfig config, ServerHealthMonitor serverHealthMonitor, OptionManager options) {
    this.config = config;
    this.templateCfg = new Configuration(Configuration.VERSION_2_3_23);
    this.serverHealthMonitor = serverHealthMonitor;
    this.options = options;
  }

  @Override
  public void init(ServletConfig servletConfig) throws ServletException {
    this.servletConfig = servletConfig;

    //templateCfg.setClassForTemplateLoading(getClass(), "/");
    Resource baseResource;
    try {
      baseResource = Resource.newResource(servletConfig.getInitParameter("resourceBase"));
    } catch (MalformedURLException e) {
      throw new ServletException(e);
    }
    templateCfg.setTemplateLoader(new ResourceTemplateLoader(baseResource));
    templateCfg.setDefaultEncoding("UTF-8");

    // Sets how errors will appear.
    // During web page *development* TemplateExceptionHandler.HTML_DEBUG_HANDLER
    // is better.
    // cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    templateCfg.setTemplateExceptionHandler(TemplateExceptionHandler.HTML_DEBUG_HANDLER);
  }

  @Override
  public ServletConfig getServletConfig() {
    return servletConfig;
  }

  @Override
  public void service(ServletRequest servletRequest, ServletResponse response) throws ServletException, IOException {
    ClientSettings settings = new ClientSettings(options.getOption(SupportService.SUPPORT_EMAIL_ADDR), options.getOption(SupportService.SUPPORT_EMAIL_SUBJECT),
      options.getOption(SupportService.OUTSIDE_COMMUNICATION_DISABLED));
    String enviornment = config.allowTestApis ? "DEVELOPMENT" : "PRODUCTION";
    final ServerData indexConfig = new ServerData(enviornment, serverHealthMonitor, config.getConfig(), settings);

    Template tmp = templateCfg.getTemplate("/index.html");

    response.setContentType("text/html; charset=utf-8");
    OutputStreamWriter outputWriter = new OutputStreamWriter(response.getOutputStream());
    try {
      tmp.process(ImmutableMap.of("dremio", indexConfig), outputWriter);
      outputWriter.flush();
      outputWriter.close();
    } catch (TemplateException e) {
      throw new IOException("Error rendering index.html template", e);
    }
  }

  @Override
  public String getServletInfo() {
    return null;
  }

  @Override
  public void destroy() {
  }
}
