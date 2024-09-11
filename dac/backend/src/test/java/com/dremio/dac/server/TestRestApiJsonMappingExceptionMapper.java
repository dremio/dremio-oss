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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

public class TestRestApiJsonMappingExceptionMapper {
  @Test
  public void invalidTypeIdException() {
    RestApiJsonMappingExceptionMapper mapper = new RestApiJsonMappingExceptionMapper();

    String invalidValue = "anInvalidValue";
    InvalidTypeIdException invalidTypeIdException =
        new InvalidTypeIdException(null, "", null, invalidValue);

    Response response = mapper.toResponse(invalidTypeIdException);
    String errorMessage = ((GenericErrorMessage) response.getEntity()).getErrorMessage();
    assertEquals("An invalid value was found: " + invalidValue, errorMessage);
  }
}
