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
package com.dremio.dac.server.admin.profile;

import java.util.Map;

import com.dremio.common.utils.PathUtils;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

/**
 * Wrapper class for {@link AccelerationDetails}
 */
public class AccelerationWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelerationWrapper.class);

  private final AccelerationDetails accelerationDetails;
  private final Map<String, ReflectionRelationship> relationshipMap;

  AccelerationWrapper(AccelerationDetails details) {
    this.accelerationDetails = details;
    relationshipMap = computeRelationships(details);
  }

  private static Map<String, ReflectionRelationship> computeRelationships(AccelerationDetails details) {
    return FluentIterable.from(details.getReflectionRelationshipsList())
      .uniqueIndex(new Function<ReflectionRelationship, String>() {
        @Override
        public String apply(ReflectionRelationship input) {
          return input.getReflection().getId().getId();
        }
      });
  }

  public String getReflectionDatasetPath(String layoutId) {
    try {
      return PathUtils.constructFullPath(relationshipMap.get(layoutId).getDataset().getPathList());
    }catch (Exception e) {
      logger.warn("failed to get reflection dataset path", e);
      return "";
    }
  }

  public boolean hasRelationship(String layoutId) {
    return relationshipMap.containsKey(layoutId);
  }
}
