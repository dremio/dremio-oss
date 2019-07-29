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

import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

import com.dremio.dac.annotations.Bootstrap;
import com.dremio.dac.resource.BootstrapResource;
import com.dremio.dac.server.test.NoUserTestFilter;

/**
 * DAC Feature for first time API
 */
public class FirstTimeFeature implements Feature {

  @Override
  public boolean configure(FeatureContext context) {
    Configuration configuration = context.getConfiguration();
    Boolean enabled = PropertyHelper.getProperty(configuration, RestServerV2.FIRST_TIME_API_ENABLE);
    Boolean testApiEnabled = PropertyHelper.getProperty(configuration, RestServerV2.TEST_API_ENABLE);

    // Default is not enabled
    if (enabled == null || !enabled) {
      return false;
    }

    boolean allowTestApis = testApiEnabled != null && testApiEnabled;

    // this is handled separately from firstTimeApi because we may enable the api only on master
    // but still register the filer on all nodes
    context.register(BootstrapResource.class);

    // Registering a dynamic feature to only add filter to resources NOT annotated with
    // @Bootstrap
    final Class<? extends ContainerRequestFilter> filter = allowTestApis ? NoUserTestFilter.class : NoUserFilter.class;
    context.register(new DynamicFeature() {
      @Override
      public void configure(ResourceInfo resourceInfo, FeatureContext context) {
        if (resourceInfo.getResourceClass().isAnnotationPresent(Bootstrap.class) ||
            resourceInfo.getResourceMethod().isAnnotationPresent(Bootstrap.class)) {
          return;
        }
        context.register(filter);
      }
    });


    return true;
  }
}
