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
package com.dremio.service.accelerator;

/**
 * Async task to rebuild refresh graph
 */
public final class RebuildRefreshGraph extends AsyncTask {
  private final AccelerationService accelerationService;

  private RebuildRefreshGraph(final AccelerationService accelerationService) {
    this.accelerationService = accelerationService;
  }

  @Override
  protected void doRun() {
    accelerationService.buildRefreshDependencyGraph();
  }

  public static RebuildRefreshGraph of(final AccelerationService accelerationService) {
    return new RebuildRefreshGraph(accelerationService);
  }
}
