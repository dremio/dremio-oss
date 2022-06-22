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
package com.dremio.exec.planner.logical;

import java.util.List;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

/**
 * Rewrites a rex tree using a list of rewrite rules
 */
public class RexRewriter {

  public static RexNode rewrite(RexNode node, List<RewriteRule> rules) {
    RewriteShuttle shuttle = new RewriteShuttle(rules);
    return node.accept(shuttle);
  }

  private static class RewriteShuttle extends RexShuttle {
    Rewriter rewriter;

    RewriteShuttle(List<RewriteRule> rules) {
      this.rewriter = new Rewriter(rules);
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexNode node = super.visitCall(call);

      if (node instanceof RexCall) {
        RexNode result = rewriter.rewrite((RexCall) node);
        if (result != null) {
          return result;
        }
      }
      return node;
    }
  }

  /**
   * Rule which defines how a RexNode should be rewritten
   */
  public abstract static class RewriteRule {
    protected final RexBuilder builder;

    public RewriteRule(RexBuilder builder) {
      this.builder = builder;
    }

    /**
     * rewrite a call
     * @param call
     * @return the rewritten call, or null if no rewrite
     */
    public abstract RexNode rewrite(RexCall call);
  }

  public static class Rewriter {
    protected final List<RewriteRule> rules;

    public Rewriter(List<RewriteRule> rules) {
      this.rules = rules;
    }

    public RexNode rewrite(RexCall call) {
      boolean match = false;
      outer: while (true) {
        for (RewriteRule rule : rules) {
          RexNode result = rule.rewrite(call);
          if (result != null) {
            match = true;
            if (result instanceof RexCall) {
              call = (RexCall) result;
              continue outer;
            }
            return result;
          }
        }
        return match ? call : null;
      }
    }
  }
}
