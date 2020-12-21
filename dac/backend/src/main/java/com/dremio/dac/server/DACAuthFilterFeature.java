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

import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;

/**
 * DAC Feature to add authentication filter
 */
public class DACAuthFilterFeature implements Feature {
  @Override
  public boolean configure(FeatureContext context) {
    final Configuration configuration = context.getConfiguration();

    Boolean disabled = PropertyHelper.getProperty(configuration, RestServerV2.DAC_AUTH_FILTER_DISABLE);
    // Default is not disabled
    if (disabled != null && disabled) {
      return false;
    }

    context.register(DACAuthFilter.class);
    context.register(DACAuthorizationObfuscationFilter.class);
    if (!configuration.isRegistered(RolesAllowedDynamicFeature.class)) {
      context.register(RolesAllowedDynamicFeature.class);
    }
    return true;
  }
}
