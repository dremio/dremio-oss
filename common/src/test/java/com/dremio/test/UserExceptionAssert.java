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
package com.dremio.test;

import java.util.stream.Stream;

import org.assertj.core.api.ThrowableAssert;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.google.common.base.Objects;

/**
 * A custom AssertJ assertion class for matching the error type / error message / context of a
 * {@link UserException}
 */
public class UserExceptionAssert extends ThrowableAssert<UserException> {

  public UserExceptionAssert(UserException e) {
    super(e);
  }

  public static UserExceptionAssert assertThat(UserException actual) {
    return new UserExceptionAssert(actual);
  }

  public static UserExceptionAssert assertThatThrownBy(ThrowingCallable shouldRaiseUserException) {
    return (UserExceptionAssert) assertThat(
      catchThrowableOfType(shouldRaiseUserException, UserException.class)).hasBeenThrown();
  }

  public UserExceptionAssert hasContext(String expectedContext) {
    isNotNull();
    if (actual.getContextStrings().stream().noneMatch(c -> c.contains(expectedContext))) {
      failWithMessage("Expected context '%s' to be contained in '%s'", expectedContext,
        actual.getContextStrings());
    }
    return this;
  }

  public UserExceptionAssert hasContexts(String... expectedContexts) {
    isNotNull();
    Stream.of(expectedContexts).forEach(this::hasContext);
    return this;
  }

  public UserExceptionAssert hasErrorType(ErrorType expectedType) {
    isNotNull();
    if (!Objects.equal(actual.getErrorType(), expectedType)) {
      failWithMessage("Expected error type to be '%s' but was '%s'", expectedType,
        actual.getErrorType());
    }
    return this;
  }

  @Override
  public UserExceptionAssert hasMessage(String message) {
    // override needed for more precise return type
    super.hasMessage(message);
    return this;
  }

  @Override
  public UserExceptionAssert hasMessageContaining(String description) {
    // override needed for more precise return type
    super.hasMessageContaining(description);
    return this;
  }

  @Override
  public UserExceptionAssert hasMessageContaining(String description, Object... parameters) {
    // override needed for more precise return type
    super.hasMessageContaining(description, parameters);
    return this;
  }
}
