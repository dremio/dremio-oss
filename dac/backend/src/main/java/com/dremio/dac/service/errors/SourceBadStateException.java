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
package com.dremio.dac.service.errors;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.dremio.dac.server.GenericErrorMessage;

/**
 * Thrown when a source is in bad state or got permission issues.
 */
public class SourceBadStateException extends WebApplicationException {
  private static final long serialVersionUID = 1L;

  private static Response newResponse(Object entity) {
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
      .entity(entity)
      .type(APPLICATION_JSON_TYPE)
      .build();
  }

  public SourceBadStateException(String sourceName, String details, Exception e) {
    super(e, newResponse(new GenericErrorMessage(
        String.format("The source [%s] is in bad state. Please check the detailed error message.", sourceName),
        details, null)));
  }
}
