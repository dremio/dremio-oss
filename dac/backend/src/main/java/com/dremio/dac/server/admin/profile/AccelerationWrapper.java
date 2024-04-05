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
package com.dremio.dac.server.admin.profile;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.model.job.acceleration.ReflectionExplanationUI;
import com.dremio.dac.model.job.acceleration.UiMapper;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Wrapper class for {@link AccelerationDetails} */
public class AccelerationWrapper {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AccelerationWrapper.class);

  private final AccelerationDetails accelerationDetails;
  private final Map<String, ReflectionRelationship> relationshipMap;

  public AccelerationWrapper(AccelerationDetails details) {
    this.accelerationDetails = details;
    relationshipMap = computeRelationships(details);
  }

  public String getReflectionDatasetPath(String layoutId) {
    try {
      return PathUtils.constructFullPath(relationshipMap.get(layoutId).getDataset().getPathList());
    } catch (Exception e) {
      logger.warn("failed to get reflection dataset path", e);
      return "";
    }
  }

  public String getReflectionDatasetVersion(String layoutId) {
    try {
      String unparsedJson = relationshipMap.get(layoutId).getDataset().getId();
      ObjectMapper mapper = new ObjectMapper();
      JsonNode parsedJson = mapper.readTree(unparsedJson);
      JsonNode versionContext = parsedJson.get("versionContext");
      return String.format(
          " [%s %s] ",
          versionContext.get("type").textValue(), versionContext.get("value").textValue());
    } catch (Exception e) {
      return " ";
    }
  }

  public Long getRefreshChainStartTime(String layoutId) {
    return relationshipMap.get(layoutId).getMaterialization().getRefreshChainStartTime();
  }

  public List<ReflectionExplanationUI> getHintsForLayoutId(String layoutId) {
    ReflectionRelationship relationship = relationshipMap.getOrDefault(layoutId, null);
    if (null == relationship || null == relationship.getReflectionExplanationList()) {
      return Collections.emptyList();
    }
    return relationship.getReflectionExplanationList().stream()
        .filter(Objects::nonNull)
        .map(UiMapper::toUI)
        .collect(Collectors.toList());
  }

  public boolean isHintHiddenforLayoutId(String layoutId) {
    ReflectionRelationship relationship = relationshipMap.getOrDefault(layoutId, null);
    if (null == relationship) {
      return false;
    }
    return relationship.getHideHint();
  }

  public boolean hasRelationship(String layoutId) {
    return relationshipMap.containsKey(layoutId);
  }

  public List<String> getErrors() {
    return accelerationDetails.getErrorList();
  }

  public boolean hasErrors() {
    final List<String> errors = accelerationDetails.getErrorList();
    return errors != null && !errors.isEmpty();
  }

  private static Map<String, ReflectionRelationship> computeRelationships(
      AccelerationDetails details) {
    if (details.getReflectionRelationshipsList() == null) {
      return ImmutableMap.of();
    }
    return FluentIterable.from(details.getReflectionRelationshipsList())
        .uniqueIndex(input -> input.getReflection().getId().getId());
  }
}
