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
import static com.google.errorprone.matchers.Description.NO_MATCH;
import static com.google.errorprone.matchers.Matchers.hasMethod;
import static com.google.errorprone.matchers.Matchers.isSubtypeOf;
import static com.google.errorprone.matchers.Matchers.methodIsNamed;

import com.google.auto.service.AutoService;
import com.google.errorprone.BugPattern;
import com.google.errorprone.BugPattern.LinkType;
import com.google.errorprone.VisitorState;
import com.google.errorprone.bugpatterns.BugChecker;
import com.google.errorprone.bugpatterns.BugChecker.ClassTreeMatcher;
import com.google.errorprone.matchers.Description;
import com.google.errorprone.matchers.Matcher;
import com.sun.source.tree.ClassTree;

/**
 * Classes that override com.dremio.common.Wrapper#unwrap should also override
 * com.dremio.common.Wrapper#isWrapperFor.
 */
@BugPattern(
    summary = "Classes that override unwrap should also override isWrapperFor.",
    severity = ERROR,
    linkType = LinkType.NONE)
@AutoService(BugChecker.class)
public class ImplementBothUnwrapAndIsWrapperFor extends BugChecker implements ClassTreeMatcher {

  private static final Matcher<ClassTree> IS_INSTANCE_OF_WRAPPER_MATCHER =
      isSubtypeOf("com.dremio.common.Wrapper");
  private static final Matcher<ClassTree> HAS_METHOD_UNWRAP_MATCHER =
      hasMethod(methodIsNamed("unwrap"));
  private static final Matcher<ClassTree> HAS_METHOD_ISWRAPPERFOR_MATCHER =
      hasMethod(methodIsNamed("isWrapperFor"));

  @Override
  public Description matchClass(ClassTree classTree, VisitorState state) {
    if (!IS_INSTANCE_OF_WRAPPER_MATCHER.matches(classTree, state)) {
      return NO_MATCH;
    }

    boolean hasUnwrapMethod = HAS_METHOD_UNWRAP_MATCHER.matches(classTree, state);
    boolean hasIsWrapperForMethod = HAS_METHOD_ISWRAPPERFOR_MATCHER.matches(classTree, state);

    if (hasUnwrapMethod) {
      if (hasIsWrapperForMethod) {
        return NO_MATCH;
      }
      return buildDescription(classTree)
          .setMessage(
              String.format(
                  "Class %s implements unwrap but does not implement isWrapperFor.",
                  classTree.getSimpleName()))
          .build();
    } else if (hasIsWrapperForMethod) {
      return buildDescription(classTree)
          .setMessage(
              String.format(
                  "Class %s implements isWrapperFor but does not implement unwrap.",
                  classTree.getSimpleName()))
          .build();
    }
    return NO_MATCH;
  }
}
