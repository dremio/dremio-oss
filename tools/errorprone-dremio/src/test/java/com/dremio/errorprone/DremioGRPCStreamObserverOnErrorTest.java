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
package com.dremio.errorprone;

import com.google.errorprone.CompilationTestHelper;
import org.junit.Before;
import org.junit.Test;

public class DremioGRPCStreamObserverOnErrorTest {
  private CompilationTestHelper helper;

  @Before
  public void setup() {
    helper = CompilationTestHelper.newInstance(DremioGRPCStreamObserverOnError.class, getClass());
  }

  @Test
  public void testSimple() {
    helper
        .addSourceLines(
            "Test.java",
            "import io.grpc.Status;",
            "import io.grpc.StatusException;",
            "import io.grpc.StatusRuntimeException;",
            "import io.grpc.stub.StreamObserver;",
            "",
            "public class Test {",
            "    private static class DummyStreamObserver implements StreamObserver<String> {",
            "        private StreamObserver<String> delegate;",
            "",
            "        public void onError(Throwable throwable) {",
            "            delegate.onError(throwable);",
            "        }",
            "",
            "        public void onNext(String str) {}",
            "        public void onCompleted() {}",
            "    }",
            "",
            "    void someMethod(Throwable t) {",
            "        DummyStreamObserver obs = new DummyStreamObserver();",
            "",
            "        obs.onError(new StatusException(Status.UNAVAILABLE));",
            "        obs.onError(new StatusRuntimeException(Status.NOT_FOUND));",
            "",
            "        // BUG: Diagnostic contains: StreamObserver.onError must be called with io.grpc.Status(Runtime)Exception, but it was: java.lang.Throwable",
            "        obs.onError(t);",
            "",
            "        // BUG: Diagnostic contains: StreamObserver.onError must be called with io.grpc.Status(Runtime)Exception, but it was: java.lang.RuntimeException",
            "        obs.onError(new RuntimeException());",
            "    }",
            "}")
        .doTest();
  }
}
