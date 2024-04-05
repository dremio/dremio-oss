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
package com.dremio.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.VersionContext;
import com.dremio.dac.proto.model.dataset.VersionContextType;
import com.dremio.dac.util.DatasetUIUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Test class for DatasetUIUtils */
public class TestDatasetUIUtils {

  @Test
  public void testCreateVersionContextMap() {
    List<SourceVersionReference> sourceVersionReferenceList = new ArrayList<>();
    sourceVersionReferenceList.add(
        new SourceVersionReference(
            "source1", new VersionContext(VersionContextType.BRANCH, "branch")));
    sourceVersionReferenceList.add(
        new SourceVersionReference("source2", new VersionContext(VersionContextType.TAG, "tag")));
    sourceVersionReferenceList.add(
        new SourceVersionReference(
            "source3",
            new VersionContext(
                VersionContextType.COMMIT, "d0628f078890fec234b98b873f9e1f3cd140988a")));

    Map<String, VersionContextReq> expectedVersionContextMap = new HashMap<>();
    expectedVersionContextMap.put(
        "source1", new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, "branch"));
    expectedVersionContextMap.put(
        "source2", new VersionContextReq(VersionContextReq.VersionContextType.TAG, "tag"));
    expectedVersionContextMap.put(
        "source3",
        new VersionContextReq(
            VersionContextReq.VersionContextType.COMMIT,
            "d0628f078890fec234b98b873f9e1f3cd140988a"));

    assertThat(DatasetUIUtils.createVersionContextMap(sourceVersionReferenceList))
        .usingRecursiveComparison()
        .isEqualTo(expectedVersionContextMap);
  }
}
