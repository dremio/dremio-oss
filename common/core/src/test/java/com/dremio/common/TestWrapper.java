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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TestWrapper {

  private interface SomeInterface {}

  private static class SomeBaseClass {}

  private static class SomeClassImplementingInterface implements Wrapper, SomeInterface {
    public SomeClassImplementingInterface() {}
  }

  private static class SomeClassExtendingBaseClass extends SomeBaseClass implements Wrapper {
    public SomeClassExtendingBaseClass() {}
  }

  private static class SomeSimpleWrapperClass implements Wrapper {
    public SomeSimpleWrapperClass() {}
  }

  @Test
  void unwrapInterface() {
    SomeClassImplementingInterface someClassImplementingInterface = new SomeClassImplementingInterface();
    assertThat(someClassImplementingInterface.unwrap(SomeInterface.class)).isInstanceOf(SomeInterface.class);
  }

  @Test
  void isWrapperForInterface() {
    SomeClassImplementingInterface someClassImplementingInterface = new SomeClassImplementingInterface();
    assertThat(someClassImplementingInterface.isWrapperFor(SomeInterface.class)).isTrue();
  }

  @Test
  void unwrapBaseClass() {
    SomeClassExtendingBaseClass someClassExtendingBaseClass = new SomeClassExtendingBaseClass();
    assertThat(someClassExtendingBaseClass.unwrap(SomeBaseClass.class)).isInstanceOf(SomeBaseClass.class);
  }

  @Test
  void isWrapperForBaseClass() {
    SomeClassExtendingBaseClass someClassExtendingBaseClass = new SomeClassExtendingBaseClass();
    assertThat(someClassExtendingBaseClass.isWrapperFor(SomeBaseClass.class)).isTrue();
  }

  @Test
  void unwrapNotImplementing() {
    SomeSimpleWrapperClass someClass = new SomeSimpleWrapperClass();
    assertThatThrownBy(() -> someClass.unwrap(SomeInterface.class))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContainingAll(
        SomeSimpleWrapperClass.class.getSimpleName(),
        "cannot be unwrapped into",
        SomeInterface.class.getSimpleName());
  }

  @Test
  void isWrapperForNotImplementing() {
    SomeSimpleWrapperClass someClass = new SomeSimpleWrapperClass();
    assertThat(someClass.isWrapperFor(SomeInterface.class)).isFalse();
  }

  @Test
  void unwrapNotExtending() {
    SomeSimpleWrapperClass someClass = new SomeSimpleWrapperClass();
    assertThatThrownBy(() -> someClass.unwrap(SomeBaseClass.class))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContainingAll(
        SomeSimpleWrapperClass.class.getSimpleName(),
        "cannot be unwrapped into",
        SomeBaseClass.class.getSimpleName());
  }

  @Test
  void isWrapperForNotExtending() {
    SomeSimpleWrapperClass someClass = new SomeSimpleWrapperClass();
    assertThat(someClass.isWrapperFor(SomeBaseClass.class)).isFalse();
  }
}
