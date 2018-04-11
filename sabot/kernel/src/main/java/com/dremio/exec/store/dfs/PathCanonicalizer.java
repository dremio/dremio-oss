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

import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 * Interface designed mainly as a hack for PDFS.
 *
 * FileSystems implementing this interface might not allow direct write
 * but requires path to be canonicalized beforehand.
 * Canonicalized paths are guaranteed to be stable and to be usable
 * for both reads and writes.
 */
public interface PathCanonicalizer {

  /**
   * Rewrites the provided path so that the filesystem would allow for write
   * operations.
   *
   * The same path can also be used to read, and should be considered the canonical
   * version of the original path.
   *
   * @param p the original path
   * @return a path allowing for write operations (possibly the same path)
   * @throws IOException
   */
  Path canonicalizePath(Path p) throws IOException;
}
