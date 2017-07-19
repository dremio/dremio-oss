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

import java.util.Map;

import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;

/**
 * A factory to create MaterializationPlanningTask
 * Useful to inject failures during testing.
 *
 */
public class MaterializationPlanningFactory {

    public MaterializationPlanningTask createTask(String storageName,
        JobsService jobsService,
        Layout layout,
        Map<String, Layout> layoutMap,
        NamespaceService namespaceService, Acceleration acceleration) {
      MaterializationPlanningTask task = new MaterializationPlanningTask(
          storageName,
          jobsService,
          layout,
          layoutMap,
          namespaceService,
          acceleration
          );
      return task;
    }

}
