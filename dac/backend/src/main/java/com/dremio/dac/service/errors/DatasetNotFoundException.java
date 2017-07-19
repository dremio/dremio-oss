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

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetResourcePath;

/**
 * Thrown when a dataset was not found for a given path
 */
public class DatasetNotFoundException extends NotFoundException {
  private static final long serialVersionUID = 1L;

  private final DatasetPath path;

  public DatasetNotFoundException(DatasetPath path, Exception error) {
    this(path, "dataset", error);
  }

  public DatasetNotFoundException(final DatasetPath path) {
    this(path, "dataset");
  }

  public DatasetNotFoundException(DatasetPath path, String message) {
    super(new DatasetResourcePath(path), message);
    this.path = path;
  }

  public DatasetNotFoundException(DatasetPath path, String message, Exception error) {
    super(new DatasetResourcePath(path), message, error);
    this.path = path;
  }

  public DatasetPath getDatasetPath() {
    return path;
  }
}
