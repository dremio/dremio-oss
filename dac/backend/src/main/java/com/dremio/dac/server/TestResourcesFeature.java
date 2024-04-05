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

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.annotations.RestResourceUsedForTesting;
import com.dremio.exec.server.BootStrapContext;
import javax.inject.Inject;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

/** Feature to include test resources */
public class TestResourcesFeature implements Feature {
  private final ScanResult scanResult;

  @Inject
  public TestResourcesFeature(BootStrapContext context) {
    this.scanResult = context.getClasspathScan();
  }

  @Override
  public boolean configure(FeatureContext context) {
    Configuration configuration = context.getConfiguration();
    Boolean enabled = PropertyHelper.getProperty(configuration, RestServerV2.TEST_API_ENABLE);

    // Default is not enabled
    if (enabled == null || !enabled) {
      return false;
    }

    for (Class<?> resource : scanResult.getAnnotatedClasses(RestResourceUsedForTesting.class)) {
      context.register(resource);
    }

    return true;
  }
}
