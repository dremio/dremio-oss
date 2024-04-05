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
package com.dremio.exec.planner.cost.janio;

import static com.dremio.exec.planner.cost.janio.CodeGeneratorUtil.argList;
import static com.dremio.exec.planner.cost.janio.CodeGeneratorUtil.paramList;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/** Generates the metadata dispatch to handlers. */
public class DispatchGenerator {
  private final Map<MetadataHandler<?>, String> metadataHandlerToName;

  public DispatchGenerator(Map<MetadataHandler<?>, String> metadataHandlerToName) {
    this.metadataHandlerToName = metadataHandlerToName;
  }

  public void dispatchMethod(
      StringBuilder buff,
      Method method,
      Collection<? extends MetadataHandler<?>> metadataHandlers) {
    String delegatingRelClass = HepRelVertex.class.getName();
    Map<MetadataHandler<?>, Set<Class<? extends RelNode>>> handlersToClasses =
        metadataHandlers.stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    Function.identity(), mh -> methodAndInstanceToImplementingClass(method, mh)));

    Set<Class<? extends RelNode>> delegateClassSet =
        handlersToClasses.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    List<Class<? extends RelNode>> delegateClassList = topologicalSort(delegateClassSet);
    buff.append("  private ").append(method.getReturnType().getName()).append(" ");
    CodeGeneratorUtil.dispatchMethodName(buff, method)
        .append("(\n")
        .append("      ")
        .append(RelNode.class.getName())
        .append(" r,\n")
        .append("      ")
        .append(RelMetadataQuery.class.getName())
        .append(" mq");
    paramList(buff, method, 2).append(") {\n");
    if (delegateClassList.isEmpty()) {
      throwUnknown(buff.append("    "), method).append("  }\n");
    } else {

      buff.append("    if (r instanceof ")
          .append(delegatingRelClass)
          .append(") {\n")
          .append("      do {\n")
          .append("        r = ((")
          .append(delegatingRelClass)
          .append(") r).getCurrentRel();\n")
          .append("      } while (r instanceof ")
          .append(delegatingRelClass)
          .append(");\n")
          .append("      return ");
      dispatchedCall(buff, "this", method, RelNode.class);
      buff.append("    }\n");

      buff.append(
          delegateClassList.stream()
              .map(
                  clazz ->
                      ifInstanceThenDispatch(method, metadataHandlers, handlersToClasses, clazz))
              .collect(Collectors.joining("    } else if ", "    if ", "    } else {\n")));
      throwUnknown(buff.append("      "), method).append("    }\n").append("  }\n");
    }
  }

  private StringBuilder ifInstanceThenDispatch(
      Method method,
      Collection<? extends MetadataHandler<?>> metadataHandlers,
      Map<MetadataHandler<?>, Set<Class<? extends RelNode>>> handlersToClasses,
      Class<? extends RelNode> clazz) {
    String handlerName = findProvider(metadataHandlers, handlersToClasses, clazz);
    StringBuilder buff =
        new StringBuilder()
            .append("(r instanceof ")
            .append(clazz.getName())
            .append(") {\n")
            .append("      return ");
    dispatchedCall(buff, handlerName, method, clazz);

    return buff;
  }

  private String findProvider(
      Collection<? extends MetadataHandler<?>> metadataHandlers,
      Map<MetadataHandler<?>, Set<Class<? extends RelNode>>> handlerToClasses,
      Class<? extends RelNode> clazz) {
    for (MetadataHandler<?> mh : metadataHandlers) {
      if (handlerToClasses.getOrDefault(mh, ImmutableSet.of()).contains(clazz)) {
        return this.metadataHandlerToName.get(mh);
      }
    }
    throw new RuntimeException();
  }

  private static StringBuilder throwUnknown(StringBuilder buff, Method method) {
    return buff.append("      throw new ")
        .append(IllegalArgumentException.class.getName())
        .append("(\"No handler for method [")
        .append(method)
        .append("] applied to argument of type [\" + r.getClass() + ")
        .append("\"]; we recommend you create a catch-all (RelNode) handler\"")
        .append(");\n");
  }

  private static void dispatchedCall(
      StringBuilder buff, String handlerName, Method method, Class<? extends RelNode> clazz) {
    buff.append(handlerName)
        .append(".")
        .append(method.getName())
        .append("((")
        .append(clazz.getName())
        .append(") r, mq");
    argList(buff, method, 2);
    buff.append(");\n");
  }

  private static Set<Class<? extends RelNode>> methodAndInstanceToImplementingClass(
      Method method, MetadataHandler<?> handler) {
    Set<Class<? extends RelNode>> set = new HashSet<>();
    for (Method m : handler.getClass().getMethods()) {
      Class<? extends RelNode> aClass = toRelClass(method, m);
      if (aClass != null) {
        set.add(aClass);
      }
    }
    return set;
  }

  private static Class<? extends RelNode> toRelClass(Method superMethod, Method candidate) {
    if (!superMethod.getName().equals(candidate.getName())
        || superMethod.getParameterCount() != candidate.getParameterCount()) {
      return null;
    } else {
      Class<?>[] cpt = candidate.getParameterTypes();
      Class<?>[] smpt = superMethod.getParameterTypes();
      if (!RelNode.class.isAssignableFrom(cpt[0]) || !RelMetadataQuery.class.equals(cpt[1])) {
        return null;
      }
      for (int i = 2; i < smpt.length; i++) {
        if (cpt[i] != smpt[i]) {
          return null;
        }
      }
      return (Class<? extends RelNode>) cpt[0];
    }
  }

  private static List<Class<? extends RelNode>> topologicalSort(
      Collection<Class<? extends RelNode>> list) {
    List<Class<? extends RelNode>> l = new ArrayList<>();
    ArrayDeque<Class<? extends RelNode>> s = new ArrayDeque<>(list);

    while (!s.isEmpty()) {
      Class<? extends RelNode> n = s.remove();

      boolean found = false;
      for (Class<? extends RelNode> other : s) {
        if (n.isAssignableFrom(other)) {
          found = true;
          break;
        }
      }
      if (found) {
        s.add(n);
      } else {
        l.add(n);
      }
    }
    return l;
  }
}
