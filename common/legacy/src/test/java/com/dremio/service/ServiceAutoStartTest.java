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
package com.dremio.service;

import static com.dremio.service.ServiceAutoStart.withAutoStart;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("checkstyle:VisibilityModifier")
@ExtendWith(MockitoExtension.class)
class ServiceAutoStartTest {
  @Mock FooImpl fooImpl;

  Foo foo;

  @BeforeEach
  void beforeEach() {
    foo = withAutoStart(Foo.class, fooImpl);
  }

  @Test
  void normalLifeCycle() throws Exception {
    foo.start();
    foo.bar();
    foo.close();

    verify(fooImpl, times(1)).start();
    verify(fooImpl).bar();
    verify(fooImpl).close();
  }

  @Test
  void startCalledMultipleTimes() throws Exception {
    foo.start();
    foo.start();

    verify(fooImpl, times(1)).start();
  }

  @Test
  void closeCalledWithoutStart() throws Exception {
    foo.close();

    verifyNoInteractions(fooImpl);
  }

  @Test
  void barCalledWithoutStart() {
    foo.bar();

    verify(fooImpl, times(1)).start();
    verify(fooImpl).bar();
  }

  interface Foo extends Service {
    void bar();
  }

  static class FooImpl implements Foo {
    @Override
    public void bar() {}

    @Override
    public void start() {}

    @Override
    public void close() {}
  }
}
