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
package com.dremio.datastore;

import java.io.IOException;

/**
 * Administrative interface for kvstores.
 */
public abstract class KVAdmin {

  private static final char INDENT = '\t';

  /**
   * Reindex this store (if it is an index store).
   * @return Number of records indexed.
   * @throws IOException
   */
  @Deprecated
  public int reindex() throws IOException {
    return 0;
  }

  /**
   * Compact the keys/values within this store.
   * @throws IOException
   */
  @Deprecated
  public void compactKeyValues() throws IOException {
  }

  /**
   * Return a string representation of stats associated with this kvstore.
   * @return A stats string
   */
  public abstract String getStats();


  /**
   * Indent a value a given number of positions.
   * @param indent The number of indentations to do.
   * @param value The value to use as the basis for the string.
   * @return An indented string value.
   */
  static String indent(int indent, Object value) {
    String replacement = "";
    for(int i =0 ; i < indent; i++) {
      replacement += INDENT;
    }
    return value.toString().replaceAll("(?m)^", replacement);
  }
}
