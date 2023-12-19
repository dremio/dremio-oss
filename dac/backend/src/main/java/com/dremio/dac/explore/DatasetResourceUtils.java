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
package com.dremio.dac.explore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.catalog.model.VersionContext;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.VersionContextType;
import com.google.common.collect.ImmutableMap;

/**
 * Utility classes for DatasetResource
 */
public final class DatasetResourceUtils {

  private DatasetResourceUtils() {
    // utils class
  }

  public static Map<String, VersionContext> createSourceVersionMapping(final Map<String, VersionContextReq> references) {

    final Map<String, VersionContext> sourceVersionMapping = new HashMap<>();
    if (references != null) {
      for (Map.Entry<String, VersionContextReq> entry: references.entrySet()) {
        VersionContextReq.VersionContextType versionContextType = entry.getValue().getType();
        switch (versionContextType) {
          case BRANCH:
            sourceVersionMapping.put(entry.getKey(), VersionContext.ofBranch(entry.getValue().getValue()));
            break;
          case TAG:
            sourceVersionMapping.put(entry.getKey(), VersionContext.ofTag(entry.getValue().getValue()));
            break;
          case COMMIT:
            sourceVersionMapping.put(entry.getKey(), VersionContext.ofCommit(entry.getValue().getValue()));
            break;
          default:
            throw new IllegalArgumentException("Unrecognized versionContextType: " + versionContextType);
        }
      }
    }

    return ImmutableMap.copyOf(sourceVersionMapping);
  }

  public static List<SourceVersionReference> createSourceVersionReferenceList(Map<String, VersionContextReq> references) {
    List<SourceVersionReference> sourceVersionReferenceList = new ArrayList<>();
    if (references != null) {
      for (Map.Entry<String, VersionContextReq> entry: references.entrySet()) {
        VersionContextReq versionContextReq = entry.getValue();
        VersionContextReq.VersionContextType versionContextType = versionContextReq.getType();
        String sourceName= entry.getKey();
        switch (versionContextType) {
          case BRANCH:
            com.dremio.dac.proto.model.dataset.VersionContext versionContextBranch =
              new com.dremio.dac.proto.model.dataset.VersionContext(VersionContextType.BRANCH, versionContextReq.getValue());
            sourceVersionReferenceList.add(new SourceVersionReference(sourceName, versionContextBranch));
            break;
          case TAG:
            com.dremio.dac.proto.model.dataset.VersionContext versionContextTag =
              new com.dremio.dac.proto.model.dataset.VersionContext(VersionContextType.TAG, versionContextReq.getValue());
            sourceVersionReferenceList.add(new SourceVersionReference(sourceName, versionContextTag));
            break;
          case COMMIT:
            com.dremio.dac.proto.model.dataset.VersionContext versionContextCommit =
              new com.dremio.dac.proto.model.dataset.VersionContext(VersionContextType.COMMIT, versionContextReq.getValue());
            sourceVersionReferenceList.add(new SourceVersionReference(sourceName, versionContextCommit));
            break;
          default:
            throw new IllegalArgumentException("Unrecognized versionContextType: " + versionContextType);
        }
      }
    }

    return sourceVersionReferenceList;
  }

  public static Map<String, VersionContextReq> createSourceVersionMapping(
    String sourceName,
    String refType,
    String refValue) {

    VersionContextReq versionContextReq = VersionContextReq.tryParse(refType, refValue);

    if (versionContextReq != null) {
      return ImmutableMap.<String, VersionContextReq>builder()
        .put(sourceName, versionContextReq)
        .build();
    } else {
      return ImmutableMap.of();
    }
  }
}
