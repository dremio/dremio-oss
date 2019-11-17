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
package com.dremio.exec.store.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.SeekableInputStream;

import com.dremio.io.FSInputStream;

public final class Streams {

  private Streams() {
  }

  public static SeekableInputStream wrap(FSInputStream in) throws IOException {
    return new DelegatingSeekableInputStream(in) {

      @Override
      public int read(ByteBuffer buf) throws IOException {
        return in.read(buf);
      }

      @Override
      public void readFully(ByteBuffer buf) throws IOException {
        while (buf.remaining() > 0) {
          in.read(buf);
        }
      }

      @Override
      public void seek(long newPos) throws IOException {
        in.setPosition(newPos);

      }

      @Override
      public long getPos() throws IOException {
        return in.getPosition();
      }
    };
  }

}
