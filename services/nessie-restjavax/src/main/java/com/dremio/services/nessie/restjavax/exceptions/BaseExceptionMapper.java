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
package com.dremio.services.nessie.restjavax.exceptions;

import com.dremio.services.nessie.restjavax.common.RestCommon;
import java.util.function.Consumer;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.ext.ExceptionMapper;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieError;

/** Code shared between concrete exception-mapper implementations. */
public abstract class BaseExceptionMapper<T extends Throwable> implements ExceptionMapper<T> {

  @Context private HttpHeaders headers;

  protected Response buildBadRequestResponse(Exception e) {
    return buildExceptionResponse(ErrorCode.BAD_REQUEST, e.getMessage(), e);
  }

  protected Response buildExceptionResponse(ErrorCode errorCode, String message, Exception e) {
    return buildExceptionResponse(errorCode, message, e, h -> {});
  }

  protected Response buildExceptionResponse(
      ErrorCode errorCode, String message, Exception e, Consumer<ResponseBuilder> responseHandler) {

    Response.Status status = Response.Status.fromStatusCode(errorCode.httpStatus());
    if (status == null) {
      status = Response.Status.INTERNAL_SERVER_ERROR;
    }

    NessieError error =
        RestCommon.buildNessieError(
            message,
            status.getStatusCode(),
            status.getReasonPhrase(),
            errorCode,
            e,
            headers::getHeaderString);

    ResponseBuilder responseBuilder =
        Response.status(status).entity(error).type(MediaType.APPLICATION_JSON_TYPE);
    responseHandler.accept(responseBuilder);
    return responseBuilder.build();
  }
}
