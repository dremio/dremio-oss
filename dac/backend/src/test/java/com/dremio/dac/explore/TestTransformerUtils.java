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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.service.jobs.JobsVersionContext;

public class TestTransformerUtils {

  @Test
  public void testCreateSourceVersionMapping() {
    List<TransformUpdateSQL.SourceVersionReference> referencesList = new ArrayList<>();
    TransformUpdateSQL.VersionContext versionContext1 =
      new TransformUpdateSQL.VersionContext(TransformUpdateSQL.VersionContextType.BRANCH, "branch");
    TransformUpdateSQL.VersionContext versionContext2 =
      new TransformUpdateSQL.VersionContext(TransformUpdateSQL.VersionContextType.TAG, "tag");
    TransformUpdateSQL.VersionContext versionContext3 =
      new TransformUpdateSQL.VersionContext(TransformUpdateSQL.VersionContextType.COMMIT, "d0628f078890fec234b98b873f9e1f3cd140988a");
    referencesList.add(new TransformUpdateSQL.SourceVersionReference("source1", versionContext1));
    referencesList.add(new TransformUpdateSQL.SourceVersionReference("source2", versionContext2));
    referencesList.add(new TransformUpdateSQL.SourceVersionReference("source3", versionContext3));

    Map<String, JobsVersionContext> sourceVersionMappingExpected = new HashMap<>();
    sourceVersionMappingExpected.put("source1", new JobsVersionContext(JobsVersionContext.VersionContextType.BRANCH, "branch"));
    sourceVersionMappingExpected.put("source2", new JobsVersionContext(JobsVersionContext.VersionContextType.TAG, "tag"));
    sourceVersionMappingExpected.put("source3", new JobsVersionContext(JobsVersionContext.VersionContextType.BARE_COMMIT,
      "d0628f078890fec234b98b873f9e1f3cd140988a"));

    assertThat(TransformerUtils.createSourceVersionMapping(referencesList)).usingRecursiveComparison().isEqualTo(sourceVersionMappingExpected);
  }
}

