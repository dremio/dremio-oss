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

import com.google.common.base.Objects;

public class FilterFSOutputStream extends FSOutputStream {
  private final FSOutputStream out;

  public FilterFSOutputStream(FSOutputStream out) {
    this.out = out;
  }

  /**
   * @param b
   * @throws IOException
   * @see java.io.OutputStream#write(int)
   */
  @Override
  public void write(int b) throws IOException {
    out.write(b);
  }

  /**
   * @return
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return out.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FilterFSOutputStream)) {
      return false;
    }
    FilterFSOutputStream other = (FilterFSOutputStream) o;
    return Objects.equal(out, other.out);
  }

  /**
   * @param b
   * @throws IOException
   * @see java.io.OutputStream#write(byte[])
   */
  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
  }

  /**
   * @param b
   * @param off
   * @param len
   * @throws IOException
   * @see java.io.OutputStream#write(byte[], int, int)
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }

  /**
   * @throws IOException
   * @see java.io.OutputStream#flush()
   */
  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public long getPosition() throws IOException {
    return out.getPosition();
  }
  /**
   * @throws IOException
   * @see java.io.OutputStream#close()
   */
  @Override
  public void close() throws IOException {
    out.close();
  }


}
