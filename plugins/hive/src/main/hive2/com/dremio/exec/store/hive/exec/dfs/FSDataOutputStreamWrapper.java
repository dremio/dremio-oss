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
package com.dremio.exec.store.hive.exec.dfs;

import static com.dremio.exec.store.hive.exec.dfs.DremioHadoopFileSystemWrapper.propagateFSError;

import com.dremio.io.FSOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSError;

/** Wrapper around FSDataOutputStream to catch {@code FSError}. */
public class FSDataOutputStreamWrapper extends FSOutputStream {
  private final FSDataOutputStream underlyingOS;

  public FSDataOutputStreamWrapper(FSDataOutputStream os) throws IOException {
    underlyingOS = os;
  }

  @Override
  public void write(int b) throws IOException {
    try {
      underlyingOS.write(b);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    try {
      underlyingOS.write(b);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    try {
      underlyingOS.write(b, off, len);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void flush() throws IOException {
    try {
      underlyingOS.flush();
    } catch (FSError e) {
      propagateFSError(e);
    }
  }

  @Override
  public long getPosition() throws IOException {
    return underlyingOS.getPos();
  }

  @Override
  public void close() throws IOException {
    try {
      underlyingOS.close();
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }
}
