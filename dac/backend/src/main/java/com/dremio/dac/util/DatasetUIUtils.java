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

import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.VersionContext;

/**
 * Util class for DatasetUI
 */
public class DatasetUIUtils {

  public static Map<String, VersionContextReq> createVersionContextMap(List<SourceVersionReference> referencesList) {
    final Map<String, VersionContextReq> versionContextReqMap = new HashMap<>();
    if (referencesList != null) {
      for (SourceVersionReference sourceVersionReference : referencesList) {
        String sourceName = sourceVersionReference.getSourceName();
        VersionContext versionContext = sourceVersionReference.getReference();
        VersionContextReq.VersionContextType versionContextType = null;
        switch (versionContext.getType()) {
          case BRANCH:
            versionContextType = VersionContextReq.VersionContextType.BRANCH;
            break;
          case TAG:
            versionContextType = VersionContextReq.VersionContextType.TAG;
            break;
          case COMMIT:
            versionContextType = VersionContextReq.VersionContextType.COMMIT;
            break;
          default:
            throw new IllegalArgumentException("Unrecognized versionContextType: " + versionContext.getType());
        }
        versionContextReqMap.put(sourceName, new VersionContextReq(versionContextType, versionContext.getValue()));
      }
    }

    return versionContextReqMap;
  }
}
