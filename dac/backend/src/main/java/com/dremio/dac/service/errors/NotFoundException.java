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
package com.dremio.dac.service.errors;

import com.dremio.dac.model.common.ResourcePath;

/**
 * Base exception for resources not found
 *
 */
public abstract class NotFoundException extends ServiceException {
  private static final long serialVersionUID = 1L;

  private final ResourcePath resourcePath;

  public NotFoundException(ResourcePath path, String description, Throwable error) {
    super(makeMessage(path, description, error));
    this.resourcePath = path;
  }

  public NotFoundException(ResourcePath path, String description) {
    super(makeMessage(path, description));
    this.resourcePath = path;
  }

  public ResourcePath getResourcePath() {
    return resourcePath;
  }

  private static String makeMessage(final ResourcePath path, final String resource) {
    return String.format("%s at %s not found", resource, path);
  }

  private static String makeMessage(final ResourcePath path, final String resource, final Throwable error) {
    return String.format("%s. Reason: %s", makeMessage(path, resource), error);
  }
}
