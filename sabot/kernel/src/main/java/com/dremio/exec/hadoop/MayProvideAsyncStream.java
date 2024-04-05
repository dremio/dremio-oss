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
package com.dremio.exec.hadoop;

import com.dremio.io.AsyncByteReader;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.fs.Path;

public
/** An addon interface for Hadoop FileSystems that support an async reader stream. */
interface MayProvideAsyncStream {

  /**
   * Whether this FileSystem may support async reads.
   *
   * @return true if async reads are supported for the given file.
   */
  boolean supportsAsync();

  /**
   * For a given file key, get an AsyncByteReader.
   *
   * @param path path for which async reader is requested
   * @param options
   * @return async reader
   * @throws IOException if async reader cannot be instantiated
   */
  AsyncByteReader getAsyncByteReader(Path path, String version, Map<String, String> options)
      throws IOException;
}
