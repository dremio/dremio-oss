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

import com.dremio.dac.daemon.DACDaemon;
import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.google.common.base.Preconditions;
import java.util.function.Function;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.eclipse.jetty.http.HttpHeader;

public class DACHttpClient extends BaseClientUtils {
  private final Client client;
  private final String webRootPath;
  private final WebTarget webRoot;
  private final WebTarget livenessRoot;
  private UserLoginSession userLoginSession;

  public DACHttpClient(Client client, DACDaemon dacDaemon) {
    this.client = Preconditions.checkNotNull(client);
    Preconditions.checkNotNull(dacDaemon);

    webRootPath = "http://localhost:" + dacDaemon.getWebServer().getPort();
    webRoot = client.target(webRootPath);
    livenessRoot =
        client.target("http://localhost:" + dacDaemon.getLivenessService().getLivenessPort());
  }

  public WebTarget getAPIv2() {
    return webRoot.path("apiv2");
  }

  public WebTarget getAPIRoot() {
    return webRoot.path("api");
  }

  public WebTarget getAPIv3() {
    return getAPIRoot().path("v3");
  }

  public WebTarget getNessieProxy() {
    return webRoot.path("nessie-proxy");
  }

  public WebTarget getScimAPIv2() {
    return webRoot.path("scim/v2");
  }

  public WebTarget getOAuthApi() {
    return webRoot.path("oauth");
  }

  public WebTarget getCatalogApi() {
    return getAPIv3().path("catalog");
  }

  public WebTarget getMetricsEndpoint() {
    return livenessRoot.path("metrics");
  }

  public WebTarget getUserApiV2(final String username) {
    return getAPIv2().path("user/" + username);
  }

  public <T extends UserLoginSession> T login(
      String userName, String password, Class<T> userLoginSessionClazz) {
    UserLogin userLogin = new UserLogin(userName, password);
    T loginSession =
        expectSuccess(
            getAPIv2().path("/login").request(JSON).buildPost(Entity.json(userLogin)),
            userLoginSessionClazz);
    userLoginSession = loginSession;
    return loginSession;
  }

  public UserLoginSession getUserLoginSession() {
    return Preconditions.checkNotNull(userLoginSession);
  }

  public String getAuthHeaderName() {
    return HttpHeader.AUTHORIZATION.toString();
  }

  public String getAuthHeaderValue() {
    return "_dremio" + getUserLoginSession().getToken();
  }

  public Invocation.Builder getBuilder(WebTarget webTarget) {
    return webTarget
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(getAuthHeaderName(), getAuthHeaderValue());
  }

  public Invocation.Builder request(Function<DACHttpClient, WebTarget> targetFunction) {
    return getBuilder(targetFunction.apply(this));
  }

  public Invocation.Builder request(String relativeToRootUrl) {
    if (!relativeToRootUrl.startsWith("/")) {
      relativeToRootUrl = "/" + relativeToRootUrl;
    }
    return getBuilder(client.target(webRootPath + relativeToRootUrl));
  }

  public DACDatasetApi getDatasetApi() {
    return new DACDatasetApi(this);
  }
}
