/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.fragment;

import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.Exchange;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.work.foreman.ForemanSetupException;

/**
 * Responsible for breaking a plan into its constituent Fragments.
 */
public class MakeFragmentsVisitor extends AbstractPhysicalVisitor<Fragment, Fragment, ForemanSetupException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MakeFragmentsVisitor.class);

  public final static MakeFragmentsVisitor INSTANCE = new MakeFragmentsVisitor();

  private MakeFragmentsVisitor() {
  }

  @Override
  public Fragment visitExchange(Exchange exchange, Fragment value) throws ForemanSetupException {
//    logger.debug("Visiting Exchange {}", exchange);
    if (value == null) {
      throw new ForemanSetupException("The simple fragmenter was called without a FragmentBuilder value.  This will only happen if the initial call to SimpleFragmenter is by a Exchange node.  This should never happen since an Exchange node should never be the root node of a plan.");
    }
    Fragment next = getNextBuilder();
    value.addReceiveExchange(exchange, next);
    next.addSendExchange(exchange, value);
    exchange.getChild().accept(this, next);
    return value;
  }

  @Override
  public Fragment visitOp(PhysicalOperator op, Fragment value)  throws ForemanSetupException {
//    logger.debug("Visiting Other {}", op);
    value = ensureBuilder(value);
    value.addOperator(op);
    for (PhysicalOperator child : op) {
      child.accept(this, value);
    }
    return value;
  }

  private Fragment ensureBuilder(Fragment value) throws ForemanSetupException {
    if (value != null) {
      return value;
    } else {
      return getNextBuilder();
    }
  }

  public Fragment getNextBuilder() {
    return new Fragment();
  }

}
