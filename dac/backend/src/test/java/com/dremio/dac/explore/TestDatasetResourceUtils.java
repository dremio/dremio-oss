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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.catalog.model.VersionContext;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.VersionContextType;
import com.dremio.dac.service.errors.ClientErrorException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class TestDatasetResourceUtils {

  @Test
  public void testCreateSourceVersionMapping() {
    Map<String, VersionContextReq> references = new HashMap<>();
    references.put(
        "source1", new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, "branch"));
    references.put(
        "source2", new VersionContextReq(VersionContextReq.VersionContextType.TAG, "tag"));
    references.put(
        "source3",
        new VersionContextReq(
            VersionContextReq.VersionContextType.COMMIT,
            "d0628f078890fec234b98b873f9e1f3cd140988a"));

    Map<String, VersionContext> sourceVersionMappingExpected = new HashMap<>();
    sourceVersionMappingExpected.put("source1", VersionContext.ofBranch("branch"));
    sourceVersionMappingExpected.put("source2", VersionContext.ofTag("tag"));
    sourceVersionMappingExpected.put(
        "source3", VersionContext.ofCommit("d0628f078890fec234b98b873f9e1f3cd140988a"));

    assertThat(DatasetResourceUtils.createSourceVersionMapping(references))
        .usingRecursiveComparison()
        .isEqualTo(sourceVersionMappingExpected);
  }

  @Test
  public void testCreateSourceVersionMappingForDatasetSummary() {
    Map<String, VersionContextReq> sourceVersionMappingExpected = new HashMap<>();
    sourceVersionMappingExpected.put(
        "dataplane_source",
        new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, "branchValue"));
    Map<String, VersionContextReq> sourceVersionMappingActual =
        DatasetResourceUtils.createSourceVersionMapping(
            "dataplane_source", "BRANCH", "branchValue");
    assertThat(sourceVersionMappingActual)
        .usingRecursiveComparison()
        .isEqualTo(sourceVersionMappingExpected);

    sourceVersionMappingExpected = new HashMap<>();
    sourceVersionMappingExpected.put(
        "dataplane_source",
        new VersionContextReq(VersionContextReq.VersionContextType.TAG, "tagValue"));
    sourceVersionMappingActual =
        DatasetResourceUtils.createSourceVersionMapping("dataplane_source", "TAG", "tagValue");
    assertThat(sourceVersionMappingActual)
        .usingRecursiveComparison()
        .isEqualTo(sourceVersionMappingExpected);

    sourceVersionMappingExpected = new HashMap<>();
    sourceVersionMappingExpected.put(
        "dataplane_source",
        new VersionContextReq(
            VersionContextReq.VersionContextType.COMMIT,
            "d0628f078890fec234b98b873f9e1f3cd140988a"));
    sourceVersionMappingActual =
        DatasetResourceUtils.createSourceVersionMapping(
            "dataplane_source", "COMMIT", "d0628f078890fec234b98b873f9e1f3cd140988a");
    assertThat(sourceVersionMappingActual)
        .usingRecursiveComparison()
        .isEqualTo(sourceVersionMappingExpected);

    assertThatThrownBy(
            () -> DatasetResourceUtils.createSourceVersionMapping("dataplane_source", "TAG", ""))
        .isInstanceOf(ClientErrorException.class)
        .hasMessageContaining("Version value was null while type was specified");

    sourceVersionMappingActual =
        DatasetResourceUtils.createSourceVersionMapping("dataplane_source", "", "");
    sourceVersionMappingExpected = new HashMap<>();
    assertThat(sourceVersionMappingActual)
        .usingRecursiveComparison()
        .isEqualTo(sourceVersionMappingExpected);

    assertThatThrownBy(
            () -> DatasetResourceUtils.createSourceVersionMapping("dataplane_source", "", "value"))
        .isInstanceOf(ClientErrorException.class)
        .hasMessageContaining("Version type was null while value was specified");
  }

  @Test
  public void testCreateSourceVersionMappingForDatasetSummaryForIncorrectType() {
    assertThatThrownBy(
            () ->
                DatasetResourceUtils.createSourceVersionMapping(
                    "dataplane_source", "INVALID", "invalidValue"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No enum constant");
  }

  @Test
  public void testCreateSourceVersionReferenceList() {
    List<SourceVersionReference> expectedReferenceArrayList = new ArrayList<>();
    expectedReferenceArrayList.add(
        new SourceVersionReference(
            "source1",
            new com.dremio.dac.proto.model.dataset.VersionContext(
                VersionContextType.BRANCH, "branch")));
    expectedReferenceArrayList.add(
        new SourceVersionReference(
            "source2",
            new com.dremio.dac.proto.model.dataset.VersionContext(VersionContextType.TAG, "tag")));
    expectedReferenceArrayList.add(
        new SourceVersionReference(
            "source3",
            new com.dremio.dac.proto.model.dataset.VersionContext(
                VersionContextType.COMMIT, "d0628f078890fec234b98b873f9e1f3cd140988a")));

    Map<String, VersionContextReq> versionContextReqMap = new HashMap<>();
    versionContextReqMap.put(
        "source1", new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, "branch"));
    versionContextReqMap.put(
        "source2", new VersionContextReq(VersionContextReq.VersionContextType.TAG, "tag"));
    versionContextReqMap.put(
        "source3",
        new VersionContextReq(
            VersionContextReq.VersionContextType.COMMIT,
            "d0628f078890fec234b98b873f9e1f3cd140988a"));
    assertThat(DatasetResourceUtils.createSourceVersionReferenceList(versionContextReqMap))
        .hasSameElementsAs(expectedReferenceArrayList);
  }

  @Test
  public void testLongestPeriodBetweenRefreshes() throws ParseException {
    assertEquals(
        TimeUnit.DAYS.toMillis(7),
        DatasetResourceUtils.findLongestPeriodBetweenRefreshes("0 0 8 * * 1"));
    assertEquals(
        TimeUnit.DAYS.toMillis(6),
        DatasetResourceUtils.findLongestPeriodBetweenRefreshes("0 0 8 * * 6,7"));
    assertEquals(
        TimeUnit.DAYS.toMillis(5),
        DatasetResourceUtils.findLongestPeriodBetweenRefreshes("0 0 8 * * 1,6"));
    assertEquals(
        TimeUnit.DAYS.toMillis(4),
        DatasetResourceUtils.findLongestPeriodBetweenRefreshes("0 0 8 * * 4,7"));
    assertEquals(
        TimeUnit.DAYS.toMillis(3),
        DatasetResourceUtils.findLongestPeriodBetweenRefreshes("0 0 8 * * 1,3,5"));
    assertEquals(
        TimeUnit.DAYS.toMillis(2),
        DatasetResourceUtils.findLongestPeriodBetweenRefreshes("0 0 8 * * 2-7"));
    assertEquals(
        TimeUnit.DAYS.toMillis(1),
        DatasetResourceUtils.findLongestPeriodBetweenRefreshes("0 0 8 * * ?"));
  }

  @Test
  public void testValidateInputSchedule() {
    assertThatThrownBy(() -> DatasetResourceUtils.validateInputSchedule("0 0 10 * *"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("refreshSchedule must contain 6 fields");
    assertThatThrownBy(() -> DatasetResourceUtils.validateInputSchedule("10 0 10 * * ?"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("seconds field for refreshSchedule must be set to 0");
    assertThatThrownBy(() -> DatasetResourceUtils.validateInputSchedule("0 60 10 * * ?"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("minute field for refreshSchedule must be a value 0-59");
    assertThatThrownBy(() -> DatasetResourceUtils.validateInputSchedule("0 0 24 * * ?"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("hour field for refreshSchedule must be a value 0-23");
    assertThatThrownBy(() -> DatasetResourceUtils.validateInputSchedule("0 0 8 3 * ?"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("day of month field for refreshSchedule must be '?' or '*'");
    assertThatThrownBy(() -> DatasetResourceUtils.validateInputSchedule("0 0 8 * 7 ?"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("month field for refreshSchedule must be '?' or '*'");
  }

  @Test
  public void testvalidDaysOfWeek() {
    assertTrue("Wilcard '*' should be accepted", DatasetResourceUtils.validDaysOfWeek("*"));
    assertTrue("Wilcard '?' should be accepted", DatasetResourceUtils.validDaysOfWeek("?"));

    assertTrue(
        "Range 'WED-SAT' should be accepted", DatasetResourceUtils.validDaysOfWeek("WED-SAT"));
    assertThatThrownBy(() -> DatasetResourceUtils.validDaysOfWeek("SUN-TUE-FRI"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid range specified for days of week for refreshSchedule");
    assertThatThrownBy(() -> DatasetResourceUtils.validDaysOfWeek("1-8"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid value specified in days of week for refreshSchedule");
    assertThatThrownBy(() -> DatasetResourceUtils.validDaysOfWeek("SAT-WED"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "range for days of week of refreshSchedule must start low and end high");

    assertTrue("daysOfWeek '7,4,2' should be valid", DatasetResourceUtils.validDaysOfWeek("7,4,2"));
    assertThatThrownBy(() -> DatasetResourceUtils.validDaysOfWeek("1,2,8"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid value specified in days of week for refreshSchedule");
  }
}
