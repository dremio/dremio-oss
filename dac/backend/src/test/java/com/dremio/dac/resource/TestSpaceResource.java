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
package com.dremio.dac.resource;

import static com.dremio.dac.server.FamilyExpectation.CLIENT_ERROR;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static org.junit.Assert.assertEquals;

import javax.ws.rs.client.Entity;

import org.junit.Test;

import com.dremio.dac.model.spaces.Space;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.GenericErrorMessage;

/**
 * Tests for APIs in {@link SpaceResource}
 */
public class TestSpaceResource extends BaseTestServer {

  @Test
  public void testCreateUpdateDeleteSpace() throws Exception {
    doc("create space");
    expectSuccess(getBuilder(getAPIv2().path("space/s1")).buildPut(Entity.json(new Space(null, "s1", null, null, null, 0, null))), Space.class);

    doc("get space");
    Space s1 = expectSuccess(getBuilder(getAPIv2().path("space/s1")).buildGet(), Space.class);

    doc("update space - add some description");
    Space newS1 = new Space(s1.getId(), s1.getName(), "I am s1", s1.getVersion(), null, 0, s1.getCtime());
    s1 = expectSuccess(getBuilder(getAPIv2().path("space/s1")).buildPut(Entity.json(newS1)), Space.class);

    assertEquals("s1", s1.getName());
    assertEquals("I am s1", s1.getDescription());

    doc("delete with bad version");
    long badVersion = 1234L;
    String expectedErrorMessage = String.format("Cannot delete space \"%s\", version provided \"%s\" is different from version found \"%s\"",
      s1.getName(), badVersion, s1.getVersion());
    final GenericErrorMessage errorDelete2 = expectStatus(CONFLICT,
      getBuilder(getAPIv2().path("space/s1").queryParam("version", badVersion)).buildDelete(),
      GenericErrorMessage.class);
    assertErrorMessage(errorDelete2, expectedErrorMessage);

    doc("delete space");
    expectSuccess(getBuilder(getAPIv2().path("space/s1").queryParam("version", s1.getVersion())).buildDelete(), Space.class);

    doc("try to fetch the space - expect a failure");
    expectError(CLIENT_ERROR, getBuilder(getAPIv2().path("space/s1")).buildGet(), GenericErrorMessage.class);
  }
}
