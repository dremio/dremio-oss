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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestJobUtil {

  @Test
  public void testGetConvertedBytes() {
    assertEquals("2 B", JobUtil.getConvertedBytes(2L));
    assertEquals("3 KB", JobUtil.getConvertedBytes(3L * 1024));
    assertEquals("4 MB", JobUtil.getConvertedBytes(4L * 1024 * 1024));
    assertEquals("5 GB", JobUtil.getConvertedBytes(5L * 1024 * 1024 * 1024));
    assertEquals("6144 GB", JobUtil.getConvertedBytes(6L * 1024 * 1024 * 1024 * 1024));
  }
}
