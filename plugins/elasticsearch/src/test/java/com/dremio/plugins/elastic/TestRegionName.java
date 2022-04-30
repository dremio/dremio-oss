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
package com.dremio.plugins.elastic;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.dremio.test.DremioTest;

/**
 * Test ElasticsearchAuthentication.getRegionName
 */
public class TestRegionName extends DremioTest {

  @Test
  public void testGetRegionName() {
    String endpoint = "vpc-dremio-es63-test.us-west-2.es.amazonaws.com";
    String regionName = ElasticsearchAuthentication.getRegionName("", endpoint);
    assertEquals("us-west-2", regionName);

    regionName = ElasticsearchAuthentication.getRegionName( "us-west-1", endpoint);
    assertEquals("us-west-1", regionName);
  }

  @Test
  public void testWrongRegionNameInEndpoint() {
    final String endpoint = "vpc-dremio-es63-test.us-west-.es.amazonaws.com";
    // We don't want to assert the exception message as it is thrown by a third party library.
    assertThatThrownBy(() -> ElasticsearchAuthentication.getRegionName("", endpoint))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testWrongRegionName() {
    final String endpoint = "vpc-dremio-es63-test.us-west-2.es.amazonaws.com";
    // We don't want to assert the exception message as it is thrown by a third party library.
    assertThatThrownBy(() -> ElasticsearchAuthentication.getRegionName("us-west", endpoint))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testNoRegionName() {
    final String endpoint = "es.amazonaws.com";
    assertThatThrownBy(() -> ElasticsearchAuthentication.getRegionName("", endpoint))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Failure creating Amazon Elasticsearch Service connection. " +
        "You must provide hostname like *.[region name].es.amazonaws.com");
  }
}
