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
package com.dremio.dac.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.explore.bi.PowerBIMessageBodyGenerator;
import com.dremio.dac.explore.bi.QlikAppMessageBodyGenerator;
import com.dremio.dac.explore.bi.TableauMessageBodyGenerator;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for {@link RestServerV2}.
 */
public class TestRestServerV2ClassRegister {
  @Test
  public void testRestServerV2BIToolBodyMessageGeneratorRegister() {
    // Setup
    final ScanResult scanResult = new ScanResult(
      Collections.EMPTY_LIST,
      Collections.EMPTY_LIST,
      ImmutableList.of(RestResource.class.getName()),
      Collections.EMPTY_LIST,
      Collections.EMPTY_LIST);

    // Test
    final RestServerV2 server = new RestServerV2(scanResult);

    // Verify
    assertTrue(server.isRegistered(TableauMessageBodyGenerator.class));
    assertTrue(server.isRegistered(PowerBIMessageBodyGenerator.class));
    assertTrue(server.isRegistered(QlikAppMessageBodyGenerator.class));
    assertFalse((boolean) server.getProperty(TableauMessageBodyGenerator.CUSTOMIZATION_ENABLED));
  }
}
