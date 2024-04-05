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

import static com.dremio.dac.server.FamilyExpectation.SUCCESS;

import com.dremio.common.perf.Timer;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

/** Utils for web client. */
public class BaseClientUtils {

  protected Response expectStatus(Response.StatusType status, Invocation i) {
    return expect(new StatusExpectation(status), i);
  }

  protected <T> T expectStatus(Response.StatusType status, Invocation i, GenericType<T> c) {
    return readEntity(expect(new StatusExpectation(status), i), c);
  }

  protected <T> T expectStatus(Response.StatusType status, Invocation i, Class<T> c) {
    return readEntity(expect(new StatusExpectation(status), i), new GenericType<T>(c));
  }

  protected Response expectSuccess(Invocation i) {
    return expect(SUCCESS, i);
  }

  protected <T> T expectSuccess(Invocation i, Class<T> c) {
    return expectSuccess(i, new GenericType<T>(c));
  }

  protected <T> T expectSuccess(Invocation i, GenericType<T> c) {
    return readEntity(expectSuccess(i), c);
  }

  protected <T> T expectError(FamilyExpectation error, Invocation i, Class<T> c) {
    return readEntity(expect(error, i), new GenericType<T>(c));
  }

  private <T> T readEntity(Response response, GenericType<T> c) {
    response.bufferEntity();
    try {
      return response.readEntity(c);
    } catch (ProcessingException e) {
      String body = response.readEntity(String.class);
      throw new RuntimeException("Error deserializing entity with " + c + " : " + body, e);
    }
  }

  protected Response expect(ResponseExpectation expectation, Invocation i) {
    try (Timer.TimedBlock b = Timer.time("request")) {
      Response response = i.invoke();
      response.bufferEntity();
      try {
        expectation.validate(response);
      } catch (AssertionError e) {
        // this will show the body of the response in the error message
        // if an error occurred it will show the server side error.
        // response.toString() does not show the content
        String body = response.readEntity(String.class);
        throw new AssertionError(
            String.format("%s\n%s\n%s", e.getMessage(), response.toString(), body), e);
      }
      return response;
    }
  }
}
