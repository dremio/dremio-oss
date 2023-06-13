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

import static com.google.errorprone.BugPattern.SeverityLevel.WARNING;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.BugPattern;
import com.google.errorprone.VisitorState;
import com.google.errorprone.bugpatterns.BugChecker;
import com.google.errorprone.bugpatterns.BugChecker.MethodInvocationTreeMatcher;
import com.google.errorprone.matchers.Description;
import com.google.errorprone.matchers.Matcher;
import com.google.errorprone.matchers.Matchers;
import com.google.errorprone.matchers.method.MethodMatchers;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.MethodInvocationTree;
import java.util.regex.Pattern;

@BugPattern(summary = "A logging statement should not use String.format", severity = WARNING)
@AutoService(BugChecker.class)
public class LogStatementWithStringFormat extends BugChecker
    implements MethodInvocationTreeMatcher {

  private static final Matcher<ExpressionTree> IS_LOGGING_METHOD =
      MethodMatchers.instanceMethod()
          .onDescendantOf("org.slf4j.Logger")
          .withNameMatching(
              Pattern.compile(
                  String.join("|", ImmutableSet.of("trace", "debug", "info", "warn", "error"))));

  private static final Matcher<ExpressionTree> IS_MARKER = Matchers.isSubtypeOf("org.slf4j.Marker");

  private static final Matcher<ExpressionTree> IS_STRING_FORMAT_METHOD =
      MethodMatchers.staticMethod().onClass("java.lang.String").named("format");

  @Override
  public Description matchMethodInvocation(MethodInvocationTree tree, VisitorState state) {
    if (!IS_LOGGING_METHOD.matches(tree, state)) {
      return Description.NO_MATCH;
    }
    int formatIndex = IS_MARKER.matches(tree.getArguments().get(0), state) ? 1 : 0;
    ExpressionTree expression = tree.getArguments().get(formatIndex);
    if (!IS_STRING_FORMAT_METHOD.matches(expression, state)) {
      return Description.NO_MATCH;
    }
    return buildDescription(tree).build();
  }
}
