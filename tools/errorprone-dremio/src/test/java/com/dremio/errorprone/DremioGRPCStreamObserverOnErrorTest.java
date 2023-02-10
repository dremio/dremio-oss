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
            "import io.grpc.Status;\n"
                + "import io.grpc.StatusException;\n"
                + "import io.grpc.StatusRuntimeException;\n"
                + "import io.grpc.stub.StreamObserver;\n"
                + "\n"
                + "public class Test {\n"
                + "    private static class DummyStreamObserver implements StreamObserver<String> {\n"
                + "        private StreamObserver<String> delegate;\n"
                + "\n"
                + "        public void onError(Throwable throwable) {\n"
                + "            delegate.onError(throwable);\n"
                + "        }\n"
                + "\n"
                + "        public void onNext(String str) {}\n"
                + "        public void onCompleted() {}\n"
                + "    }\n"
                + "\n"
                + "    void someMethod(Throwable t) {\n"
                + "        DummyStreamObserver obs = new DummyStreamObserver();\n"
                + "\n"
                + "        obs.onError(new StatusException(Status.UNAVAILABLE));\n"
                + "        obs.onError(new StatusRuntimeException(Status.NOT_FOUND));\n"
                + "\n"
                + "        // BUG: Diagnostic contains: StreamObserver.onError must be called with io.grpc.Status(Runtime)Exception, but it was: java.lang.Throwable\n"
                + "        obs.onError(t);\n"
                + "\n"
                + "        // BUG: Diagnostic contains: StreamObserver.onError must be called with io.grpc.Status(Runtime)Exception, but it was: java.lang.RuntimeException\n"
                + "        obs.onError(new RuntimeException());\n"
                + "    }\n"
                + "}")
        .doTest();
  }
}
