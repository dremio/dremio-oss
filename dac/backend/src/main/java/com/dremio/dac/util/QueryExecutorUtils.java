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
package com.dremio.dac.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.exec.catalog.VersionContext;

/**
 * Util class for QueryExecutor
 */
public class QueryExecutorUtils {

  public static Map<String, VersionContext> createSourceVersionMapping(List<SourceVersionReference> referenceList) {
    Map<String, VersionContext> versionContextMap = new HashMap<>();
    if (referenceList != null) {
      for (SourceVersionReference sourceVersionReference : referenceList) {
        String sourceName = sourceVersionReference.getSourceName();
        com.dremio.dac.proto.model.dataset.VersionContext versionContext = sourceVersionReference.getReference();
        VersionContext context;
        switch (versionContext.getType()) {
          case BRANCH:
            context = VersionContext.ofBranch(versionContext.getValue());
            break;
          case TAG:
            context = VersionContext.ofTag(versionContext.getValue());
            break;
          case COMMIT:
            context = VersionContext.ofBareCommit(versionContext.getValue());
            break;
          default:
            throw new IllegalArgumentException("Unrecognized versionContextType: " + versionContext.getType());
        }
        versionContextMap.put(sourceName, context);
      }
    }
    return versionContextMap;
  }
}
