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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.service.jobs.JobsVersionContext;

/**
 * Utility class containing methods for Transform purposes
 */
public class TransformerUtils {

  /**
   * Accepts the reference list for transform api call and returns the corresponding map of source to version context
   *
   * @param referencesList list of references where each reference corresponds to source name and version context
   * @return
   */
  public static Map<String, JobsVersionContext> createSourceVersionMapping(List<TransformUpdateSQL.SourceVersionReference> referencesList) {
    Map<String, JobsVersionContext> sourceVersionMapping = new HashMap<>();

    if (referencesList != null) {
      for (TransformUpdateSQL.SourceVersionReference sourceVersionReference : referencesList) {
        String sourceName = sourceVersionReference.getSourceName();
        TransformUpdateSQL.VersionContext versionContext = sourceVersionReference.getReference();
        JobsVersionContext.VersionContextType jobsVersionContextType = null;
        if (versionContext.getType() == TransformUpdateSQL.VersionContextType.BRANCH) {
          jobsVersionContextType = JobsVersionContext.VersionContextType.BRANCH;
        } else if (versionContext.getType() == TransformUpdateSQL.VersionContextType.COMMIT) {
          jobsVersionContextType = JobsVersionContext.VersionContextType.BARE_COMMIT;
        } else if (versionContext.getType() == TransformUpdateSQL.VersionContextType.TAG) {
          jobsVersionContextType = JobsVersionContext.VersionContextType.TAG;
        }
        JobsVersionContext jobsVersionContext = new JobsVersionContext(jobsVersionContextType, versionContext.getValue());
        sourceVersionMapping.put(sourceName, jobsVersionContext);
      }
    }

    return sourceVersionMapping;
  }
}
