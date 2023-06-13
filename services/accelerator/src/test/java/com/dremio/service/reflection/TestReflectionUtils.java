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
package com.dremio.service.reflection;

import static com.dremio.service.reflection.ReflectionUtils.computeMetrics;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.reflection.proto.MaterializationMetrics;
import com.dremio.service.reflection.proto.PartitionDistributionStrategy;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionField;

/**
 * Tests for ReflectionUtils
 */
public class TestReflectionUtils {
  @Test
  public void testAreReflectionDetailsEqual() {
    ReflectionDetails detail1 = new ReflectionDetails();
    ReflectionDetails detail2 = new ReflectionDetails();

    Assert.assertTrue(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    // check null equal []
    detail1.setSortFieldList(Collections.<ReflectionField>emptyList());
    detail2.setSortFieldList(null);
    Assert.assertTrue(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Collections.<ReflectionField>emptyList());
    detail2.setSortFieldList(Collections.<ReflectionField>emptyList());
    Assert.assertTrue(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    Assert.assertTrue(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    // order matter for sortFieldList
    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test1"), new ReflectionField("test2")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test1"), new ReflectionField("test2")));
    Assert.assertTrue(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test1"), new ReflectionField("test2")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test2"), new ReflectionField("test1")));
    Assert.assertFalse(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setSortFieldList(Collections.<ReflectionField>emptyList());
    Assert.assertFalse(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test2")));
    Assert.assertFalse(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test"), new ReflectionField("test2")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test2")));
    Assert.assertFalse(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail1.setPartitionFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setPartitionFieldList(Arrays.asList(new ReflectionField("test")));
    Assert.assertTrue(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail1.setPartitionFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setPartitionFieldList(Arrays.asList(new ReflectionField("test2")));
    Assert.assertFalse(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    detail1 = new ReflectionDetails();
    detail2 = new ReflectionDetails();

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail1.setPartitionDistributionStrategy(PartitionDistributionStrategy.CONSOLIDATED);
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setPartitionDistributionStrategy(PartitionDistributionStrategy.CONSOLIDATED);
    Assert.assertTrue(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail1.setPartitionDistributionStrategy(PartitionDistributionStrategy.CONSOLIDATED);
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setPartitionDistributionStrategy(PartitionDistributionStrategy.STRIPED);
    Assert.assertFalse(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));

    detail1 = new ReflectionDetails();
    detail2 = new ReflectionDetails();

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail1.setPartitionDistributionStrategy(PartitionDistributionStrategy.STRIPED);
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    Assert.assertFalse(ReflectionUtils.areReflectionDetailsEqual(detail1, detail2));
  }
  @Test
  public void testComputeMetricsEmptyReturnedRecordsCount() {
    final JobDataFragment jobDataFragment = mock(JobDataFragment.class);
    when(jobDataFragment.getReturnedRowCount()).thenReturn(0);
    final JobInfo info = new JobInfo();
    final JobAttempt jobAttempt = new JobAttempt();
    jobAttempt.setInfo(info);
    try (final MockedStatic<JobDataClientUtils> jobDataClientUtilsMockedStatic = Mockito.mockStatic(JobDataClientUtils.class)) {
      jobDataClientUtilsMockedStatic.when(() -> JobDataClientUtils.getJobData(null,null,null,0,1000))
        .thenReturn(jobDataFragment);
      try(final MockedStatic<JobsProtoUtil> jobsProtoUtilMockedStatic = Mockito.mockStatic(JobsProtoUtil.class)) {
        jobsProtoUtilMockedStatic.when(() -> JobsProtoUtil.getLastAttempt(null))
          .thenReturn(jobAttempt);
        final MaterializationMetrics materializationMetrics = computeMetrics(null, null, null, null);
        Assert.assertEquals((Integer) 0, materializationMetrics.getNumFiles());      //then
        jobDataClientUtilsMockedStatic.verify(
          () -> JobDataClientUtils.getJobData(null, null, null, 0, 1000));
      }
    }
  }
}
