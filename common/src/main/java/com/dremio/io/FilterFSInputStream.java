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
package com.dremio.io;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FilterFSInputStream extends FSInputStream {
  private final FSInputStream in;

  public FilterFSInputStream(FSInputStream in) {
    this.in = in;
  }

  /**
   * @return
   * @throws IOException
   * @see java.io.InputStream#read()
   */
  @Override
  public int read() throws IOException {
    return in.read();
  }

  /**
   * @param b
   * @return
   * @throws IOException
   * @see java.io.InputStream#read(byte[])
   */
  @Override
  public int read(byte[] b) throws IOException {
    return in.read(b);
  }

  /**
   * @param b
   * @param off
   * @param len
   * @return
   * @throws IOException
   * @see java.io.InputStream#read(byte[], int, int)
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return in.read(b, off, len);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return in.read(dst);
  }

  @Override
  public long getPosition() throws IOException {
    return in.getPosition();
  }

  @Override
  public void setPosition(long position) throws IOException {
    in.setPosition(position);
  }

  /**
   * @param n
   * @return
   * @throws IOException
   * @see java.io.InputStream#skip(long)
   */
  @Override
  public long skip(long n) throws IOException {
    return in.skip(n);
  }

  /**
   * @return
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return in.toString();
  }

  /**
   * @return
   * @throws IOException
   * @see java.io.InputStream#available()
   */
  @Override
  public int available() throws IOException {
    return in.available();
  }

  /**
   * @throws IOException
   * @see java.io.InputStream#close()
   */
  @Override
  public void close() throws IOException {
    in.close();
  }

  /**
   * @param readlimit
   * @see java.io.InputStream#mark(int)
   */
  @Override
  public void mark(int readlimit) {
    in.mark(readlimit);
  }

  /**
   * @throws IOException
   * @see java.io.InputStream#reset()
   */
  @Override
  public void reset() throws IOException {
    in.reset();
  }

  /**
   * @return
   * @see java.io.InputStream#markSupported()
   */
  @Override
  public boolean markSupported() {
    return in.markSupported();
  }



}
