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
package com.dremio;

import com.dremio.common.DeferredException;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

public class DeferredExceptionTest {

  @Test
  public void testLimitSuppressed() {
    Exception e = new Exception("root");
    Exception child = new Exception("child");
    Exception suppressed = new Exception("suppressed");

    Exception suppressedInner = new Exception("suppressedInner");

    // add a 100 suppressed exceptions to all child exceptions
    IntStream.range(0, 100).forEach(i -> suppressed.addSuppressed(suppressedInner));

    // add a 100 suppressed exceptions to all child exceptions
    IntStream.range(0, 100).forEach(i -> child.addSuppressed(suppressed));

    DeferredException deferredException = new DeferredException();
    // add the root
    deferredException.addException(e);

    // add the child exceptions to root, only first five should be added
    // each of child exception should only track 5 suppressed exceptions
    IntStream.range(0, 100).forEach(i -> deferredException.addException(child));

    Exception finalException = deferredException.getAndClear();

    Assert.assertEquals(4, finalException.getSuppressed().length);

    Throwable[] suppressedExceptions = finalException.getSuppressed();
    IntStream.range(0, 4)
        .forEach(i -> Assert.assertEquals(5, suppressedExceptions[i].getSuppressed().length));

    IntStream.range(0, 4)
        .forEach(
            i -> {
              Assert.assertEquals(
                  5, suppressedExceptions[i].getSuppressed()[i].getSuppressed().length);
            });
  }
}
