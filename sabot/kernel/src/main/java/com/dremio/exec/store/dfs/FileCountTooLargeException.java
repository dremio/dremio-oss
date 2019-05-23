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
package com.dremio.exec.store.dfs;

import com.dremio.exec.catalog.DatasetMetadataTooLargeException;

/**
 * Thrown when getting metadata for a directory being promoted to a dataset has too many files.
 */
public class FileCountTooLargeException extends DatasetMetadataTooLargeException {

  public static final String MESSAGE = "Number of files in dataset '%s' contained %d files which exceeds the maximum number of files of %d";
  private static final long serialVersionUID = -5486531057168594562L;

  public FileCountTooLargeException(String datasetName, int actual, int limit) {
    super(String.format(MESSAGE, datasetName, actual, limit));
  }
}
