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
import com.google.errorprone.VisitorState;
import com.google.errorprone.bugpatterns.BugChecker;
import com.google.errorprone.bugpatterns.BugChecker.MethodInvocationTreeMatcher;
import com.google.errorprone.matchers.Description;
import com.google.errorprone.matchers.Matcher;
import com.google.errorprone.matchers.Matchers;
import com.google.errorprone.matchers.MethodVisibility.Visibility;
import com.google.errorprone.matchers.method.MethodMatchers;
import com.google.errorprone.util.ASTHelpers;
import com.sun.source.tree.ClassTree;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.Tree;
import java.util.List;

@BugPattern(
    summary = "StreamObserver.onError must be called with io.grpc.Status(Runtime)Exception",
    severity = ERROR)
@AutoService(BugChecker.class)
public class DremioGRPCStreamObserverOnError extends BugChecker
    implements MethodInvocationTreeMatcher {

  private static final Matcher<ExpressionTree> IS_STREAM_OBSERVER_ON_ERROR_CALL =
      MethodMatchers.instanceMethod()
          .onDescendantOf("io.grpc.stub.StreamObserver")
          .named("onError")
          .withParameters("java.lang.Throwable");

  private static final Matcher<Tree> IS_ALLOWED_EXCEPTION_TYPE =
      Matchers.anyOf(
          Matchers.isSubtypeOf("io.grpc.StatusException"),
          Matchers.isSubtypeOf("io.grpc.StatusRuntimeException"));

  private static final Matcher<MethodTree> IS_ON_ERROR_METHOD =
      Matchers.allOf(
          Matchers.methodIsNamed("onError"),
          Matchers.methodHasVisibility(Visibility.PUBLIC),
          Matchers.methodReturns(Matchers.isVoidType()),
          Matchers.methodHasParameters(List.of(Matchers.isSameType(Throwable.class))));

  private static final Matcher<ClassTree> IS_STREAM_OBSERVER_CLASS =
      Matchers.isSubtypeOf("io.grpc.stub.StreamObserver");

  @Override
  public Description matchMethodInvocation(MethodInvocationTree tree, VisitorState state) {
    if (!IS_STREAM_OBSERVER_ON_ERROR_CALL.matches(tree, state)) {
      return Description.NO_MATCH;
    }
    ExpressionTree throwable = tree.getArguments().get(0);
    if (IS_ALLOWED_EXCEPTION_TYPE.matches(throwable, state)) {
      return Description.NO_MATCH;
    }
    if (Matchers.allOf(
            Matchers.isSameType(Throwable.class),
            Matchers.enclosingMethod(IS_ON_ERROR_METHOD),
            Matchers.enclosingClass(IS_STREAM_OBSERVER_CLASS))
        .matches(throwable, state)) {
      // StreamObserver delegating Throwable to another StreamObserver is allowed
      return Description.NO_MATCH;
    }
    String message =
        String.format(
            "StreamObserver.onError must be called with io.grpc.Status(Runtime)Exception, but it was: %s",
            ASTHelpers.getType(throwable));
    return Description.builder(tree, this.canonicalName(), null, ERROR, message).build();
  }
}
