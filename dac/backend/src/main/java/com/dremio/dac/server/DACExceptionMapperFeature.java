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

/**
 * JAX-WS Feature for DAC error messages
 */
public class DACExceptionMapperFeature implements Feature {

  public DACExceptionMapperFeature() {
  }

  @Override
  public boolean configure(FeatureContext context) {
    Configuration configuration = context.getConfiguration();
    Boolean property = PropertyHelper.getProperty(configuration, RestServerV2.ERROR_STACKTRACE_ENABLE);

    // Default is false
    boolean includeStackTraces = property != null && property;

    context.register(new UserExceptionMapper(includeStackTraces));
    context.register(new GenericExceptionMapper(includeStackTraces));

    return true;
  }
}
