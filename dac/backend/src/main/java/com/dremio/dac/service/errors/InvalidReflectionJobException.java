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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;


/**
 * Exception to indicate
 */
public class InvalidReflectionJobException extends WebApplicationException {
  public InvalidReflectionJobException(String jobId, String reflectionId) {
    super(String.format("Reflection [%s] is not processed by job [%s]", reflectionId, jobId), Response.Status.NOT_FOUND);
  }

  public InvalidReflectionJobException(Exception e) {
    super(String.format(e.getMessage(), Response.Status.NOT_FOUND));
  }
}
