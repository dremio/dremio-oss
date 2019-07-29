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
package com.dremio.resource.exception;

/**
 * Resource unavailable exception. This exception is treated as an internal cancellation, and not a query failure.
 */
public class ResourceUnavailableException extends ResourceAllocationException {

  private static final long serialVersionUID = 7630874419871525605L;

  public ResourceUnavailableException(String msg) {
    super(msg);
  }
}
