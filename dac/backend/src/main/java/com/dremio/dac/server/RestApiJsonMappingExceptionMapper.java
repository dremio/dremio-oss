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

import com.fasterxml.jackson.databind.JsonMappingException;
import java.util.Iterator;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

/**
 * The default {@code com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper} exposes internal
 * class and package names when an incoming request contains input that does not correctly map to
 * the internal request object for the requested path Example: the user supplied a value with the
 * wrong type for a property, so a JsonMappingException is thrown when we attempt to deserialize the
 * request body. This custom exception mapper generates responses that tell the user the path to the
 * invalid input in their JSON request without exposing internal details about the system.
 */
public class RestApiJsonMappingExceptionMapper implements ExceptionMapper<JsonMappingException> {
  @Override
  public Response toResponse(JsonMappingException exception) {
    if (exception.getPath().isEmpty()) {
      return errorResponse("Invalid value");
    }

    StringBuilder errorMessage = new StringBuilder("Invalid value found at: ");
    Iterator<JsonMappingException.Reference> iter = exception.getPath().iterator();

    if (iter.hasNext()) {
      errorMessage.append(referenceToString(iter.next()));
    }

    while (iter.hasNext()) {
      JsonMappingException.Reference ref = iter.next();
      errorMessage.append(referenceToString(ref, "."));
    }

    return errorResponse(errorMessage.toString());
  }

  private Response errorResponse(String errorMessage) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(new GenericErrorMessage(errorMessage))
        .type(MediaType.APPLICATION_JSON_TYPE)
        .build();
  }

  private String referenceToString(JsonMappingException.Reference ref) {
    return referenceToString(ref, "");
  }

  private String referenceToString(JsonMappingException.Reference ref, String fieldNamePrefix) {
    if (ref.getFieldName() != null) {
      return fieldNamePrefix + ref.getFieldName();
    } else if (ref.getIndex() != -1) {
      return String.format("[%s]", ref.getIndex());
    }

    return fieldNamePrefix + "UNKNOWN";
  }
}
