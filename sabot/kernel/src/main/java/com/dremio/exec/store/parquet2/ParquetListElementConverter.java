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
package com.dremio.exec.store.parquet2;

/**
 * Interface to be used to handle LIST field values while reading parquet files
 */
public interface ParquetListElementConverter {
  /**
   *
   * @return true if a value was written between startElement and endElement
   */
  boolean hasWritten();

  /**
   * signals start of writing an element
   */
  void startElement();

  /**
   * signals end of an writing element
   */
  void endElement();

  /**
   * write null value into list
   */
  void writeNullListElement();
}
