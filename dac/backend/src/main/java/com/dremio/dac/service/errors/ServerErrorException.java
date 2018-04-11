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
package com.dremio.dac.service.errors;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * An exception that maps to HTTP 500
 */
public class ServerErrorException extends WebApplicationException {
  public ServerErrorException(Throwable cause) {
    super(cause.getMessage(), cause, Response.Status.INTERNAL_SERVER_ERROR);
  }
}
