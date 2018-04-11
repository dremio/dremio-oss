/*
 * Copyright (C) 2017-2018 Dremio Corporation
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


import java.util.Arrays;

import javax.annotation.Nullable;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

/**
 * Matcher for UserException that matches if expected type and actual type are the same, and expected message is
 * contained in the actual message.
 *
 * For usage, check out TestInboundImpersonation.invalidPolicy()
 */
public class UserExceptionMatcher extends TypeSafeMatcher<UserException> {

  private final ErrorType expectedType;
  private final String expectedMessage;
  private final String[] expectedContexts;

  public UserExceptionMatcher(final ErrorType expectedType, final String expectedMessage, final String... expectedContexts) {
    this.expectedType = expectedType;
    this.expectedMessage = expectedMessage;
    this.expectedContexts = expectedContexts;
  }

  public UserExceptionMatcher(final ErrorType expectedType) {
    this(expectedType, null);
  }

  @Override
  protected boolean matchesSafely(final UserException e) {
    // Use .contains(...) to compare expected and actual message as the exact messages may differ.
    return (expectedType == e.getErrorType())
      && (expectedMessage == null || e.getMessage().contains(expectedMessage))
      && (expectedContexts == null || FluentIterable.from(Arrays.asList(expectedContexts)).filter(new Predicate<String>() {
            @Override
            public boolean apply(String input) {
              for (String context : e.getContextStrings()) {
                if (context.contains(input)) {
                  return true;
                }
              }
              return false;
            }
          }).size() == expectedContexts.length);
  }

  @Override
  public void describeTo(final Description description) {
    description.appendText("UserException of type: ")
      .appendValue(expectedType.toString());
    if (expectedMessage != null) {
      description.appendText(" with message that contains: \"")
        .appendText(expectedMessage)
        .appendText("\"");
    }
    if (expectedContexts != null) {
      description.appendValueList(" with contexts that contain every one of {", ",", "}", expectedContexts);
    }
  }

  @Override
  protected void describeMismatchSafely(final UserException e, final Description description) {
    description.appendText("UserException thrown was of type: ")
      .appendValue(e.getErrorType().toString());
    if (expectedMessage != null) {
      description.appendText(" with message: \"")
        .appendText(e.getMessage())
      .appendText("\"");
    }
    if (expectedContexts != null) {
      description.appendValueList(" with contexts {", ",", "}", e.getContextStrings());
    }
  }
}
