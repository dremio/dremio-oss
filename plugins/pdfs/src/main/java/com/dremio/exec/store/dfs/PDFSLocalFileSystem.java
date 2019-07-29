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
package com.dremio.exec.store.dfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * A {@link RawLocalFileSystem} subclass supporting {@code org.apache.hadoop.fs.FileSystem#listStatusIterator(Path)}
 * method
 */
public final class PDFSLocalFileSystem extends RawLocalFileSystem {

  @Override
  public CloseableRemoteIterator<FileStatus> listStatusIterator(Path f) throws FileNotFoundException, IOException {
    File localf = pathToFile(f);
    java.nio.file.Path localp = localf.toPath();

    if (!localf.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist");
    }

    if (localf.isFile()) {
      return CloseableRemoteIterator.of(getFileStatus(f));
    }

    final Stream<FileStatus> statuses = Files.list(localp)
        .map(p -> {
          Path hp = new Path(f, new Path(null, null, p.toFile().getName()));
          try {
            return getFileStatus(hp);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });

    return new CloseableRemoteIterator<>(statuses);
  }
}
