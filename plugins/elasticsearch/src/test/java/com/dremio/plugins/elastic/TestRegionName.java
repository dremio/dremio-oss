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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.DremioTest;
import com.dremio.test.UserExceptionMatcher;

/**
 * Test ElasticsearchAuthentication.getRegionName
 */
public class TestRegionName extends DremioTest {

  @Test
  public void testGetRegionName() {
    String endpoint = "vpc-dremio-es63-test.us-west-2.es.amazonaws.com";
    String regionName = ElasticsearchAuthentication.getRegionName("", endpoint);
    assertEquals("us-west-2", regionName);

    regionName = ElasticsearchAuthentication.getRegionName("us-west-1", endpoint);
    assertEquals("us-west-1", regionName);
  }

  @Test
  public void testWrongRegionNameInEndpoint() {
    String endpoint = "vpc-dremio-es63-test.us-west-.es.amazonaws.com";
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION,
      "Failure creating Amazon Elasticsearch Service connection. The region is incorrect. You can change region in Advanced Options"));
    ElasticsearchAuthentication.getRegionName("", endpoint);
  }

  @Test
  public void testWrongRegionName() {
    String endpoint = "vpc-dremio-es63-test.us-west-2.es.amazonaws.com";
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION,
      "Failure creating Amazon Elasticsearch Service connection. The region is incorrect. You can change region in Advanced Options"));
    ElasticsearchAuthentication.getRegionName("us-west", endpoint);
  }

  @Test
  public void testNoRegionName() {
    String endpoint = "es.amazonaws.com";
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION,
      "Failure creating Amazon Elasticsearch Service connection. You must provide hostname like *.[region name].es.amazonaws.com"));
    ElasticsearchAuthentication.getRegionName("", endpoint);
  }
}
