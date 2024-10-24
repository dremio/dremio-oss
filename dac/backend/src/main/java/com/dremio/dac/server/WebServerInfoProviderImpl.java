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

import com.dremio.common.server.WebServerInfoProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.StringValidator;
import com.google.common.base.Strings;
import java.net.MalformedURLException;
import java.net.URL;
import javax.inject.Provider;

@Options
public class WebServerInfoProviderImpl implements WebServerInfoProvider {
  public static final StringValidator WEB_SERVER_BASE_URI =
      new StringValidator("auth.oauth.issuer-url", "");

  private final Provider<String> clusterId;
  private final Provider<OptionManager> optionManager;
  private final String fallbackHostname;

  public WebServerInfoProviderImpl(
      Provider<String> clusterId, Provider<OptionManager> optionManager, String fallbackHostname) {
    this.clusterId = clusterId;
    this.optionManager = optionManager;
    this.fallbackHostname = fallbackHostname;
  }

  @Override
  public String getClusterId() {
    return clusterId.get();
  }

  @Override
  public URL getBaseURL() {
    final String override = optionManager.get().getOption(WEB_SERVER_BASE_URI);

    try {
      return new URL(!Strings.isNullOrEmpty(override) ? override : fallbackHostname);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(
          String.format(
              "Value of '%s' option must be a valid URL", WEB_SERVER_BASE_URI.getOptionName()),
          e);
    }
  }
}
