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
package com.dremio.dac.model.job.acceleration;

import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.DatasetDetails;
import com.dremio.service.accelerator.proto.MaterializationDetails;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;

/**
 * Maps between ui and kvstore objects.
 */
public class UiMapper {

  static MaterializationDetailsUI toUI(MaterializationDetails details) {
    if (details == null) {
      return null;
    }
    return new MaterializationDetailsUI(details);
  }
  static DatasetDetailsUI toUI(DatasetDetails details) {
    if (details == null) {
      return null;
    }
    return new DatasetDetailsUI(details);
  }

  static AccelerationSettingsUI toUI(AccelerationSettings settings) {
    if (settings == null) {
      return null;
    }
    return new AccelerationSettingsUI(settings);
  }

  static ReflectionRelationshipUI toUI(ReflectionRelationship relationship) {
    if (relationship == null) {
      return null;
    }
    return new ReflectionRelationshipUI(relationship);
  }

  public static AccelerationDetailsUI toUI(AccelerationDetails accelerationDetails) {
    if (accelerationDetails == null) {
      return null;
    }
    return new AccelerationDetailsUI(accelerationDetails);
  }
}
