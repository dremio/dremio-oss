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

import static com.google.errorprone.BugPattern.SeverityLevel.ERROR;

import com.google.auto.service.AutoService;
import com.google.errorprone.BugPattern;
import com.google.errorprone.BugPattern.LinkType;
import com.google.errorprone.VisitorState;
import com.google.errorprone.bugpatterns.BugChecker;
import com.google.errorprone.bugpatterns.BugChecker.MethodInvocationTreeMatcher;
import com.google.errorprone.matchers.Description;
import com.google.errorprone.matchers.Matcher;
import com.google.errorprone.matchers.method.MethodMatchers;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.MethodInvocationTree;

@BugPattern(
    summary = "Use caffeine cache instead of guava cache",
    severity = ERROR,
    linkType = LinkType.NONE)
@AutoService(BugChecker.class)
public class NoGuavaCacheUsage extends BugChecker implements MethodInvocationTreeMatcher {

  private static final Matcher<ExpressionTree> IS_GUAVA_CACHEBUILDER_BUILD =
      MethodMatchers.instanceMethod()
          .onExactClass("com.google.common.cache.CacheBuilder")
          .named("build");

  @Override
  public Description matchMethodInvocation(MethodInvocationTree tree, VisitorState state) {
    if (IS_GUAVA_CACHEBUILDER_BUILD.matches(tree.getMethodSelect(), state)) {
      return buildDescription(tree).build();
    }
    return Description.NO_MATCH;
  }
}
