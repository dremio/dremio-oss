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

package com.dremio.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

/**
 * Tests for {@link AutoCloseables}
 */
public class TestAutoCloseables {
  @Test
  public void testClose() {
    Resource r1 = new Resource();
    Resource r2 = new Resource();
    Resource r3 = new Resource();
    Resource r4 = new Resource();

    try {
      AutoCloseables.close(
        () -> r1.closeWithException(new IOException("R1 exception")),
        () -> r2.closeWithException(null),
        () -> r3.closeWithException(new RuntimeException("R3 exception")),
        () -> r4.closeWithException(null)
      );

      fail("Expected exception");
    } catch (Exception e) {
      assertEquals(IOException.class, e.getClass());
    }
    assertTrue(r1.isClosed() && r2.isClosed() && r3.isClosed() && r4.isClosed());
  }

  @Test(expected = IOException.class)
  public void testCloseWithExpectedException() throws IOException {
    Resource r1 = new Resource();
    Resource r2 = new Resource();
    AutoCloseables.close(IOException.class,
      () -> r1.closeWithException(new IOException("R1 exception")),
      () -> r2.closeWithException(new RuntimeException("R3 exception"))
    );
  }

  @Test(expected = RuntimeException.class)
  public void testCloseWithExpectedRuntimeException() throws IOException {
    Resource r1 = new Resource();
    Resource r2 = new Resource();
    AutoCloseables.close(IOException.class,
      () -> r1.closeWithException(new RuntimeException("R1 exception")),
      () -> r2.closeWithException(new IOException("R3 exception"))
    );
  }

  @Test(expected = IOException.class)
  public void testCloseWithExpectedWrappedException() throws IOException {
    Resource r1 = new Resource();
    Resource r2 = new Resource();
    AutoCloseables.close(IOException.class,
      () -> r1.closeWithException(new Exception("R1 exception")),
      () -> r2.closeWithException(new Exception("R3 exception"))
    );
  }

  private final class Resource {
    private boolean isClosed = false;

    public void closeWithException(Exception toBeThrown) throws Exception {
      isClosed = true;
      if (toBeThrown != null) {
        throw toBeThrown;
      }
    }

    public boolean isClosed() {
      return isClosed;
    }
  }
}
