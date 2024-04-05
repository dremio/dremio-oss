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
package com.dremio.sabot.op.sender;

import com.dremio.sabot.exec.rpc.FileStreamManager;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.dremio.sabot.op.sort.external.SpillManager.SpillFile;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Wrapper over SpillManager to deal with create/open/delete streams. */
public class FileStreamManagerImpl implements FileStreamManager {
  private static final Logger logger = LoggerFactory.getLogger(FileStreamManagerImpl.class);
  private final SpillManager spillManager;

  /*
   * The spillManager picks a random directory on each invocation of getSpillFile(). To ensure that
   * the reader and writer use the same file, we remember the file that was used by the writer in
   * this map.
   */
  private final Map<Integer, SpillManager.SpillFile> spillFileMap = new ConcurrentHashMap<>();

  FileStreamManagerImpl(SpillManager spillManager) {
    this.spillManager = spillManager;
  }

  @Override
  public String getId() {
    return spillManager.getId();
  }

  @Override
  public OutputStream createOutputStream(int fileSeq) throws IOException {
    Preconditions.checkState(
        spillFileMap.get(fileSeq) == null, "duplicate file with seq " + fileSeq);
    SpillFile spillFile = spillManager.getSpillFile(getFileName(fileSeq));
    spillFileMap.put(fileSeq, spillFile);
    return spillFile.create(false);
  }

  @Override
  public InputStream getInputStream(int fileSeq) throws IOException {
    SpillFile spillFile = spillFileMap.get(fileSeq);
    Preconditions.checkNotNull(
        spillFile, "reader tried to open file with seq " + fileSeq + " ahead of writer");
    return spillFile.open(false);
  }

  @Override
  public void delete(int fileSeq) throws IOException {
    try {
      SpillFile spillFile = spillFileMap.remove(fileSeq);
      if (spillFile != null) {
        // the close call deletes the file.
        spillFile.close();
      }
    } catch (Exception ex) {
      Throwables.throwIfInstanceOf(ex, IOException.class);
      throw new IOException(ex);
    }
  }

  @Override
  public void deleteAll() throws IOException {
    try {
      logger.debug("deleteAll spillFileMap");
      spillFileMap.clear();
      // the close call deletes all the relevant files.
      spillManager.close();
    } catch (Exception ex) {
      Throwables.throwIfInstanceOf(ex, IOException.class);
      throw new IOException(ex);
    }
  }

  private static String getFileName(int index) {
    return String.format("%08d.arrow", index);
  }
}
