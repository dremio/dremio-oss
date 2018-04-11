/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;

import java.util.List;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

/**
 * Map UserException to something the UI can understand.
 */
public class UserExceptionMapper implements ExceptionMapper<UserException> {

  public static final String SYSTEM_ERROR_MSG = "Unexpected error occurred";

  private static final String RESPONSE_STATUS = "RESPONSE_STATUS";

  private final boolean sendStackTrace;

  public UserExceptionMapper(boolean sendStackTrace) {
    super();
    this.sendStackTrace = sendStackTrace;
  }

  @Context
  private UriInfo uriInfo;

  @Context
  private Request request;

  @Override
  public Response toResponse(UserException exception) {
    final String errorMessage = exception.getErrorType() == ErrorType.SYSTEM ?
      SYSTEM_ERROR_MSG : exception.getOriginalMessage();
    return Response.status(getStatus(exception))
        .entity(new ErrorMessageWithContext(
            errorMessage,
            removeStatus(exception.getContextStrings()),
            sendStackTrace ? GenericErrorMessage.printStackTrace(exception): null
                ))
        .type(APPLICATION_JSON_TYPE)
        .build();
  }

  /**
   * An Error message that also has error context.
   */
  public static class ErrorMessageWithContext extends GenericErrorMessage {

    private final String[] context;

    @JsonCreator
    public ErrorMessageWithContext(
            @JsonProperty("errorMessage") String errorMessage,
            @JsonProperty("context") String[] context,
            @JsonProperty("stackTrace") String[] stackTrace) {
      super(errorMessage, "", stackTrace);
      this.context = context;
    }

    public String[] getContext() {
      return context;
    }

  }

  public static UserException.Builder withStatus(UserException.Builder builder, Response.Status status) {
    return builder.addContext(RESPONSE_STATUS+":"+status.toString());
  }

  private static Response.Status getStatus(UserException uex) {
    List<String> contextList = uex.getContextStrings();
    for (String context : contextList) {
      if (context.startsWith(RESPONSE_STATUS)) {
        try {
          String status = context.substring(RESPONSE_STATUS.length() + 1);
          return Response.Status.valueOf(status);
        } catch (Exception e) {
          // should not happen unless someone is passing an invalid status. In that case, ignore the context
        }
      }
    }
    if (uex.getErrorType() == ErrorType.CONCURRENT_MODIFICATION) {
      return CONFLICT;
    }
    return BAD_REQUEST;
  }

  private static String[] removeStatus(List<String> contextList) {
    List<String> updated = Lists.newArrayList();
    for (String context : contextList) {
      if (!context.startsWith(RESPONSE_STATUS)) {
        updated.add(context);
      }
    }
    return updated.toArray(new String[0]);
  }
}
