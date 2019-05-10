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
package com.dremio.connector.metadata;

/**
 * Dataset not found exception.
 */
public class DatasetNotFoundException extends EntityNotFoundException {

  private static final long serialVersionUID = -2673750427623932743L;

  private final EntityPath datasetPath;

  public DatasetNotFoundException(EntityPath datasetPath) {
    this.datasetPath = datasetPath;
  }

  public DatasetNotFoundException(String message, EntityPath datasetPath) {
    super(message);
    this.datasetPath = datasetPath;
  }

  public DatasetNotFoundException(String message, Throwable cause, EntityPath datasetPath) {
    super(message, cause);
    this.datasetPath = datasetPath;
  }

  public DatasetNotFoundException(Throwable cause, EntityPath datasetPath) {
    super(cause);
    this.datasetPath = datasetPath;
  }

  public EntityPath getDatasetPath() {
    return datasetPath;
  }
}
