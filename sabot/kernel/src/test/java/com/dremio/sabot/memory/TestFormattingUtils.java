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
package com.dremio.sabot.memory;

import com.dremio.test.DremioTest;
import org.junit.Assert;
import org.junit.Test;

public class TestFormattingUtils extends DremioTest {
  @Test
  public void testReadableMemorySizeFormatting() {
    Assert.assertEquals("0 bytes", FormattingUtils.formatBytes(0));
    Assert.assertEquals("1 byte", FormattingUtils.formatBytes(1));
    Assert.assertEquals("500 bytes", FormattingUtils.formatBytes(500));
    Assert.assertEquals("5000 bytes (5 KiB)", FormattingUtils.formatBytes(5000));
    Assert.assertEquals("50000 bytes (49 KiB)", FormattingUtils.formatBytes(50000));
    Assert.assertEquals("500000 bytes (488 KiB)", FormattingUtils.formatBytes(500_000));
    Assert.assertEquals(
        "500000000000 bytes (465.66 GiB)", FormattingUtils.formatBytes(500_000_000_000L));
    Assert.assertEquals(
        "9223372036854775807 bytes (8388608.00 TiB)", FormattingUtils.formatBytes(Long.MAX_VALUE));
    Assert.assertEquals("-500000 bytes (-488 KiB)", FormattingUtils.formatBytes(-500_000));
    Assert.assertEquals("-50000 bytes (-49 KiB)", FormattingUtils.formatBytes(-50000));
    Assert.assertEquals("-5000 bytes (-5 KiB)", FormattingUtils.formatBytes(-5000));
    Assert.assertEquals("-500 bytes", FormattingUtils.formatBytes(-500));
    Assert.assertEquals("-1 byte", FormattingUtils.formatBytes(-1));
  }
}
