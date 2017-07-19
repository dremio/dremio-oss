/*
 * Copyright (C) 2017 Dremio Corporation
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

import static org.junit.Assert.assertEquals;

import javax.ws.rs.core.Response;

/**
 * Expectations based on the response status family
 */
public enum FamilyExpectation implements ResponseExpectation {
  SUCCESS(Response.Status.Family.SUCCESSFUL), CLIENT_ERROR(Response.Status.Family.CLIENT_ERROR), SERVER_ERROR(Response.Status.Family.SERVER_ERROR);

  private final Response.Status.Family f;

  private FamilyExpectation(Response.Status.Family f) {
    this.f = f;
  }

  @Override
  public void validate(Response response) {
    assertEquals(f, response.getStatusInfo().getFamily());
  }
}
