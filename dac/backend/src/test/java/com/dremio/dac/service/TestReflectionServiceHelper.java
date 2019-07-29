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
package com.dremio.dac.service;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.service.reflection.proto.PartitionDistributionStrategy;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionField;

/**
 * Tests for ReflectionServiceHelper
 */
public class TestReflectionServiceHelper {
  @Test
  public void testAreReflectionDetailsEqual() {
    ReflectionDetails detail1 = new ReflectionDetails();
    ReflectionDetails detail2 = new ReflectionDetails();

    Assert.assertTrue(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    // check null equal []
    detail1.setSortFieldList(Collections.<ReflectionField>emptyList());
    detail2.setSortFieldList(null);
    Assert.assertTrue(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Collections.<ReflectionField>emptyList());
    detail2.setSortFieldList(Collections.<ReflectionField>emptyList());
    Assert.assertTrue(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    Assert.assertTrue(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    // order should not matter
    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test1"), new ReflectionField("test2")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test2"), new ReflectionField("test1")));
    Assert.assertTrue(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setSortFieldList(Collections.<ReflectionField>emptyList());
    Assert.assertFalse(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test2")));
    Assert.assertFalse(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test"), new ReflectionField("test2")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test2")));
    Assert.assertFalse(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail1.setPartitionFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setPartitionFieldList(Arrays.asList(new ReflectionField("test")));
    Assert.assertTrue(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail1.setPartitionFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setPartitionFieldList(Arrays.asList(new ReflectionField("test2")));
    Assert.assertFalse(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    detail1 = new ReflectionDetails();
    detail2 = new ReflectionDetails();

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail1.setPartitionDistributionStrategy(PartitionDistributionStrategy.CONSOLIDATED);
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setPartitionDistributionStrategy(PartitionDistributionStrategy.CONSOLIDATED);
    Assert.assertTrue(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail1.setPartitionDistributionStrategy(PartitionDistributionStrategy.CONSOLIDATED);
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail2.setPartitionDistributionStrategy(PartitionDistributionStrategy.STRIPED);
    Assert.assertFalse(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));

    detail1 = new ReflectionDetails();
    detail2 = new ReflectionDetails();

    detail1.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    detail1.setPartitionDistributionStrategy(PartitionDistributionStrategy.STRIPED);
    detail2.setSortFieldList(Arrays.asList(new ReflectionField("test")));
    Assert.assertFalse(ReflectionServiceHelper.areReflectionDetailsEqual(detail1, detail2));
  }
}
