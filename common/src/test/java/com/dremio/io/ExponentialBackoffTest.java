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
package com.dremio.io;
import org.junit.Assert;
import org.junit.Test;
/**
 * Test class for {@code ExponentialBackoff}
 */
public class ExponentialBackoffTest {
  @Test
  public void getBackoffWaitTimeTest(){
    //ExponentialBackoff ExponentialBackoffMock = Mockito.mock(ExponentialBackoff.class);
    int baseMillis = 5;
    int maxMillis = 60000;
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(62,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(63,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(64,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(65,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(70,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(31,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(100,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(Math.min(maxMillis, baseMillis * (1 << 13)), ExponentialBackoff.getBackoffWaitTime(13,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(Math.min(maxMillis, baseMillis * (1 << 28)), ExponentialBackoff.getBackoffWaitTime(28,baseMillis,maxMillis), 0.0f);

    baseMillis = 1;
    maxMillis = 60000;
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(62,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(63,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(64,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(65,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(70,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(31,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(maxMillis, ExponentialBackoff.getBackoffWaitTime(100,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(Math.min(maxMillis, baseMillis * (1 << 13)), ExponentialBackoff.getBackoffWaitTime(13,baseMillis,maxMillis), 0.0f);
    Assert.assertEquals(Math.min(maxMillis, baseMillis * (1 << 28)), ExponentialBackoff.getBackoffWaitTime(28,baseMillis,maxMillis), 0.0f);
  }
}
