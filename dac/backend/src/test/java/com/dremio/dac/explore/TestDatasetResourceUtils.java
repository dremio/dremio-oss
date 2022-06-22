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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.VersionContextType;
import com.dremio.exec.catalog.VersionContext;

public class TestDatasetResourceUtils {

  @Test
  public void testCreateSourceVersionMapping() {
    Map<String, VersionContextReq> references = new HashMap<>();
    references.put("source1", new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, "branch"));
    references.put("source2", new VersionContextReq(VersionContextReq.VersionContextType.TAG, "tag"));
    references.put("source3", new VersionContextReq(VersionContextReq.VersionContextType.COMMIT, "d0628f078890fec234b98b873f9e1f3cd140988a"));

    Map<String, VersionContext> sourceVersionMappingExpected = new HashMap<>();
    sourceVersionMappingExpected.put("source1", VersionContext.ofBranch("branch"));
    sourceVersionMappingExpected.put("source2", VersionContext.ofTag("tag"));
    sourceVersionMappingExpected.put("source3", VersionContext.ofBareCommit("d0628f078890fec234b98b873f9e1f3cd140988a"));

    assertThat(DatasetResourceUtils.createSourceVersionMapping(references)).usingRecursiveComparison().isEqualTo(sourceVersionMappingExpected);
  }

  @Test
  public void testCreateSourceVersionMappingForDatasetSummary() {
    Map<String, VersionContextReq> sourceVersionMappingExpected = new HashMap<>();
    sourceVersionMappingExpected.put("dataplane_source", new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, "branchValue"));
    Map<String, VersionContextReq> sourceVersionMappingActual = DatasetResourceUtils.createSourceVersionMapping("dataplane_source", "BRANCH", "branchValue");
    assertThat(sourceVersionMappingActual).usingRecursiveComparison().isEqualTo(sourceVersionMappingExpected);

    sourceVersionMappingExpected = new HashMap<>();
    sourceVersionMappingExpected.put("dataplane_source", new VersionContextReq(VersionContextReq.VersionContextType.TAG, "tagValue"));
    sourceVersionMappingActual = DatasetResourceUtils.createSourceVersionMapping("dataplane_source", "TAG", "tagValue");
    assertThat(sourceVersionMappingActual).usingRecursiveComparison().isEqualTo(sourceVersionMappingExpected);

    sourceVersionMappingExpected = new HashMap<>();
    sourceVersionMappingExpected.put("dataplane_source", new VersionContextReq(VersionContextReq.VersionContextType.COMMIT, "d0628f078890fec234b98b873f9e1f3cd140988a"));
    sourceVersionMappingActual = DatasetResourceUtils.createSourceVersionMapping("dataplane_source", "COMMIT", "d0628f078890fec234b98b873f9e1f3cd140988a");
    assertThat(sourceVersionMappingActual).usingRecursiveComparison().isEqualTo(sourceVersionMappingExpected);

    sourceVersionMappingActual = DatasetResourceUtils.createSourceVersionMapping("dataplane_source", "TAG", "");
    sourceVersionMappingExpected = new HashMap<>();
    assertThat(sourceVersionMappingActual).usingRecursiveComparison().isEqualTo(sourceVersionMappingExpected);

    sourceVersionMappingActual = DatasetResourceUtils.createSourceVersionMapping("dataplane_source", "", "");
    sourceVersionMappingExpected = new HashMap<>();
    assertThat(sourceVersionMappingActual).usingRecursiveComparison().isEqualTo(sourceVersionMappingExpected);

    sourceVersionMappingActual = DatasetResourceUtils.createSourceVersionMapping("dataplane_source", "", "value");
    sourceVersionMappingExpected = new HashMap<>();
    assertThat(sourceVersionMappingActual).usingRecursiveComparison().isEqualTo(sourceVersionMappingExpected);
  }

  @Test
  public void testCreateSourceVersionMappingForDatasetSummaryForIncorrectType() {
    assertThatThrownBy(() -> DatasetResourceUtils.createSourceVersionMapping("dataplane_source", "INVALID", "invalidValue"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("No enum constant");
  }

  @Test
  public void testCreateSourceVersionReferenceList() {
    List<SourceVersionReference> expectedReferenceArrayList = new ArrayList<>();
    expectedReferenceArrayList.add(new SourceVersionReference("source1",
      new com.dremio.dac.proto.model.dataset.VersionContext(VersionContextType.BRANCH, "branch")));
    expectedReferenceArrayList.add(new SourceVersionReference("source2",
      new com.dremio.dac.proto.model.dataset.VersionContext(VersionContextType.TAG, "tag")));
    expectedReferenceArrayList.add(new SourceVersionReference("source3",
      new com.dremio.dac.proto.model.dataset.VersionContext(VersionContextType.COMMIT, "d0628f078890fec234b98b873f9e1f3cd140988a")));

    Map<String, VersionContextReq> versionContextReqMap = new HashMap<>();
    versionContextReqMap.put("source1", new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, "branch"));
    versionContextReqMap.put("source2", new VersionContextReq(VersionContextReq.VersionContextType.TAG, "tag"));
    versionContextReqMap.put("source3", new VersionContextReq(VersionContextReq.VersionContextType.COMMIT, "d0628f078890fec234b98b873f9e1f3cd140988a"));
    assertThat(DatasetResourceUtils.createSourceVersionReferenceList(versionContextReqMap)).hasSameElementsAs(expectedReferenceArrayList);
  }
}
