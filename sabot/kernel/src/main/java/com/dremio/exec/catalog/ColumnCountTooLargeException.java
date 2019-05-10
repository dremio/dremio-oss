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

package com.dremio.exec.catalog;

/**
 * Thrown when dataset metadata is too wide, when column count in metadata exceeds the limit
 * defined in CatalogOptions
 */
public class ColumnCountTooLargeException extends DatasetMetadataTooLargeException {
  private static final long serialVersionUID = 7765148243332604247L;

  public static final String MESSAGE = "Number of fields in dataset '%s' exceeded the maximum number of fields of %d";

  public ColumnCountTooLargeException(String datasetName, int limit) {
    super(String.format(MESSAGE, datasetName, limit));
  }

}
