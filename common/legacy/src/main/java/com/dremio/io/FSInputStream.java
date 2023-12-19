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
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Advanced {@code InputStream} used by {@code FileSystemWrapper}
 */
public abstract class FSInputStream extends InputStream {

  public FSInputStream() {
  }

  /**
   * Reads bytes from the stream into the provided buffer {@code dst}
   *
   * @param dst the buffer
   * @return number of bytes read, possibly 0, or -1 if reaching end of stream
   * @throws IOException
   */
  public abstract int read(ByteBuffer dst) throws IOException;

  /**
   * Seeks to the position and reads bytes from the stream into the provided buffer {@code dst}
   *
   * @param position the position
   * @param dst the buffer
   * @return number of bytes read, possibly 0, or -1 if reaching end of stream
   * @throws IOException
   */
  public abstract int read(long position, ByteBuffer dst) throws IOException;

  /**
   * Gets the stream position
   * @return the stream position
   * @throws IOException
   */
  public abstract long getPosition() throws IOException;

  /**
   * Sets the stream position
   * @param position the new stream position
   * @throws IOException
   */
  public abstract void setPosition(long position) throws IOException;

}
