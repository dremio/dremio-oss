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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.VersionContext;
import com.dremio.dac.proto.model.dataset.VersionContextType;

/**
 * Tests for QueryExecutorUtils class
 */
public class TestQueryExecutorUtils {

  @Test
  public void testCreateSourceVersionMapping() {
    List<SourceVersionReference> referencesList = new ArrayList<>();
    VersionContext versionContext1 = new VersionContext(VersionContextType.BRANCH, "branch");
    VersionContext versionContext2 = new VersionContext(VersionContextType.TAG, "tag");
    VersionContext versionContext3 = new VersionContext(VersionContextType.COMMIT, "d0628f078890fec234b98b873f9e1f3cd140988a");
    referencesList.add(new SourceVersionReference("source1", versionContext1));
    referencesList.add(new SourceVersionReference("source2", versionContext2));
    referencesList.add(new SourceVersionReference("source3", versionContext3));

    Map<String, com.dremio.catalog.model.VersionContext> sourceVersionMappingExpected = new HashMap<>();
    sourceVersionMappingExpected.put("source1", com.dremio.catalog.model.VersionContext.ofBranch("branch"));
    sourceVersionMappingExpected.put("source2", com.dremio.catalog.model.VersionContext.ofTag("tag"));
    sourceVersionMappingExpected.put("source3", com.dremio.catalog.model.VersionContext.ofCommit("d0628f078890fec234b98b873f9e1f3cd140988a"));

    assertThat(QueryExecutorUtils.createSourceVersionMapping(referencesList)).usingRecursiveComparison().isEqualTo(sourceVersionMappingExpected);
  }
}
