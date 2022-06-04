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
package com.dremio.sabot.exec.rpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Manager to create/delete input/output file streams.
 */
public interface FileStreamManager {
  /**
   * Identifier for the set of files (may or may not be in the same directory).
   *
   * @return id string
   */
  String getId();

  /**
   * Create an output stream for the given sequence number.
   * @param fileSeq sequence number
   * @return output stream
   * @throws IOException on io errors
   */
  OutputStream createOutputStream(int fileSeq) throws IOException;

  /**
   * Open an input stream for the given sequence number.
   * @param fileSeq sequence number
   * @return input stream
   * @throws IOException on io errors
   */
  InputStream getInputStream(int fileSeq) throws IOException;

  /**
   * Delete the file & reclaim space for the given sequence number.
   * @param fileSeq sequence number.
   * @throws IOException on io errors.
   */
  void delete(int fileSeq) throws IOException;

  /**
   * Delete all files.
   * @throws IOException on io errors.
   */
  void deleteAll() throws IOException;
}
