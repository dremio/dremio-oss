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

import java.io.IOException;

import javax.inject.Provider;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.servlet.DefaultServlet;

import com.dremio.common.util.DremioEdition;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.ServerHealthMonitor;
import com.dremio.dac.server.models.AnalyzeTools;
import com.dremio.dac.server.models.ServerData;
import com.dremio.dac.service.datasets.DatasetDownloadManager;
import com.dremio.dac.support.QueryLogBundleService;
import com.dremio.dac.support.SupportService;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.options.OptionManager;
import com.dremio.service.reflection.ReflectionOptions;

/**
 * Servlet that hosts static assets but mapped to specific extensions. If the static assets are not available, falls
 * back to the index servlet.
 */
public class DremioServlet implements Servlet {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioServlet.class);

  private final IndexServlet indexServlet;
  private final DefaultServlet staticResources = new DefaultServlet();
  private final DremioConfig config;
  private final Provider<ServerHealthMonitor> healthMonitor;
  private final Provider<OptionManager> optionManager;
  private final Provider<SupportService> supportService;

  private static final String[] EXTENSIONS = { ".jpg", ".js", ".png", ".woff2", ".ttf", ".svg", ".css", ".ico", ".js.map" };

  public DremioServlet(DremioConfig config, Provider<ServerHealthMonitor> healthMonitor,
    Provider<OptionManager> optionManager, Provider<SupportService> supportService) {
    this.config = config;
    this.healthMonitor = healthMonitor;
    this.optionManager = optionManager;
    this.supportService = supportService;
    this.indexServlet = new IndexServlet(this::getDataForClient);
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

  private ServerData getDataForClient() {
    return this.getDataBuilder().build();
  }

  protected ServerData.Builder getDataBuilder() {
    final OptionManager options = optionManager.get();

    return ServerData.newBuilder()
      .setServerEnvironment(config.getBoolean(DremioConfig.DEBUG_ALLOW_TEST_APIS_BOOL) ? "DEVELOPMENT" : "PRODUCTION")
      .setServerStatus(healthMonitor.get().getStatus().toString())
      .setIntercomAppId(config.getString(DremioConfig.WEB_UI_SERVICE_CONFIG + ".intercom.appid"))
      .setShouldEnableBugFiling(config.getBoolean(DremioConfig.DEBUG_OPTIONS + ".bug.filing.enabled"))
      .setShouldEnableRSOD(config.getBoolean(DremioConfig.DEBUG_OPTIONS + ".rsod.enabled"))
      .setSupportEmailTo(options.getOption(SupportService.SUPPORT_EMAIL_ADDR))
      .setSupportEmailSubjectForJobs(options.getOption(SupportService.SUPPORT_EMAIL_SUBJECT))
      .setOutsideCommunicationDisabled(options.getOption(SupportService.OUTSIDE_COMMUNICATION_DISABLED))
      .setSubhourAccelerationPoliciesEnabled(options.getOption(ReflectionOptions.ENABLE_SUBHOUR_POLICIES))
      .setLowerProvisioningSettingsEnabled(options.getOption(UIOptions.ALLOW_LOWER_PROVISIONING_SETTINGS))
      .setAllowFileUploads(options.getOption(UIOptions.ALLOW_FILE_UPLOADS))
      .setAllowSpaceManagement(options.getOption(UIOptions.ALLOW_SPACE_MANAGEMENT))
      .setTdsMimeType(options.getOption(UIOptions.TABLEAU_TDS_MIMETYPE))
      .setWhiteLabelUrl(options.getOption(UIOptions.WHITE_LABEL_URL))
      .setClusterId(supportService.get().getClusterId().getIdentity())
      .setEdition(DremioEdition.getAsString())
      .setAnalyzeTools(AnalyzeTools.from(options))
      .setCrossSourceDisabled(options.getOption(CatalogOptions.DISABLE_CROSS_SOURCE_SELECT))
      .setQueryBundleUsersEnabled(options.getOption(QueryLogBundleService.USERS_BUNDLE_DOWNLOAD))
      .setDownloadRecordsLimit(options.getOption(DatasetDownloadManager.DOWNLOAD_RECORDS_LIMIT));
  }

  protected Provider<SupportService> getSupportService() {
    return supportService;
  }

  protected DremioConfig getConfig() {
    return config;
  }

  protected Provider<OptionManager> getOptionManager() {
    return optionManager;
  }
}
