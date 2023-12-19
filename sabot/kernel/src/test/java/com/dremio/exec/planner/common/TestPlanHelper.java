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

package com.dremio.exec.planner.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import com.dremio.common.SuppressForbidden;
import com.dremio.exec.planner.StatelessRelShuttleImpl;

public class TestPlanHelper {
  public static <TPlan extends RelNode, TClass> TClass findSingleNode(TPlan plan, Class<TClass> clazz, Map<String, String> attributes) {
    return findFirstNode(plan, clazz, attributes, true);
  }

  public static <TPlan extends RelNode, TClass> TClass findFirstNode(TPlan plan, Class<TClass> clazz, Map<String, String> attributes, boolean isSingle) {
    TargetNodeDescriptor descriptor =  new TargetNodeDescriptor(clazz, attributes);
    List<TClass> nodes= NodeFinder.find(plan, descriptor);
    assertThat(nodes).isNotNull();
    if (isSingle) {
      assertThat(nodes.size()).as("1 node is expected").isEqualTo(1);
    }

    TClass node = nodes.get(0);
    assertThat(node).as("Node is expected").isNotNull();

    return node;
  }

  public static <TPlan extends RelNode, TClass> List<TClass> findNodes(TPlan plan, Class<TClass> clazz, Map<String, String> attributes) {
    TargetNodeDescriptor descriptor =  new TargetNodeDescriptor(clazz, attributes);
    List<TClass> nodes= NodeFinder.find(plan, descriptor);
    assertThat(nodes).isNotNull();
    assertThat(nodes.size() > 1).as("Multiple nodes are expected").isTrue();

    return nodes;
  }

  public static class TargetNodeDescriptor {
    private Class clazz;
    private Map<String, String> attributes;

    public TargetNodeDescriptor(Class clazz, Map<String, String> attributes) {
      this.clazz = clazz;
      this.attributes = attributes;
    }
  }

  @SuppressForbidden
  public static class NodeFinder<T extends RelNode> extends StatelessRelShuttleImpl {
    private final List<T> collectedNodes = new ArrayList<>();
    private final TargetNodeDescriptor targetNodeDescriptor;
    public static<T> List<T> find(RelNode input, TargetNodeDescriptor targetNodeDescriptor) {
      NodeFinder finder = new NodeFinder(targetNodeDescriptor);
      finder.visit(input);
      return finder.collect();
    }

    public NodeFinder(TargetNodeDescriptor targetNodeDescriptor) {
      this.targetNodeDescriptor = targetNodeDescriptor;
    }

    public List<T> collect() {
      return collectedNodes;
    }

    private static Field getField(Class clazz, String fieldName)
      throws NoSuchFieldException {
      try {
        return clazz.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        Class superClass = clazz.getSuperclass();
        if (superClass==null) {
          throw e;
        } else {
          return getField(superClass, fieldName);
        }
      }
    }

    private boolean matchAttributes(T node) {
      if (targetNodeDescriptor.attributes == null) {
        return true;
      }

      try {
        for (Map.Entry<String, String> entry : targetNodeDescriptor.attributes.entrySet()) {
          String attribue = entry.getKey();
          Field field = getField(targetNodeDescriptor.clazz, attribue);
          field.setAccessible(true);
          Object value = field.get(node);
          if (!value.toString().toLowerCase().contains(entry.getValue().toLowerCase())) {
            return false;
          }
        }
      } catch (NoSuchFieldException | IllegalAccessException e) {
        fail("This should never happen");
      }

      return true;
    }

    @Override
    public RelNode visit(RelNode node) {
      return visitNode(node);
    }

    @Override
    public RelNode visit(TableScan scan) {
      return visitNode(scan);
    }

    private RelNode visitNode(RelNode node) {
      if (targetNodeDescriptor.clazz.isAssignableFrom(node.getClass())  && matchAttributes((T)node)) {
        collectedNodes.add((T)node);
      }
      return super.visit(node);
    }
  }
}
