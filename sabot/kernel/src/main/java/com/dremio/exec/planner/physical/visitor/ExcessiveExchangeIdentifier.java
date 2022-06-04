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
package com.dremio.exec.planner.physical.visitor;

import java.util.Collections;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.Prel;

/**
 * If two fragments are both estimated to be parallelization one, remove the exchange separating them
 */
public final class ExcessiveExchangeIdentifier extends FragmentStatVisitor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExcessiveExchangeIdentifier.class);

  private ExcessiveExchangeIdentifier(long targetSliceSize) {
    super(targetSliceSize);
  }

  public static Prel removeExcessiveExchanges(Prel prel, long targetSliceSize) {
    ExcessiveExchangeIdentifier exchange = new ExcessiveExchangeIdentifier(targetSliceSize);
    return prel.accept(exchange, exchange.getNewStat());
  }

  @Override
  public Prel visitExchange(ExchangePrel prel, MajorFragmentStat parent) throws RuntimeException {
    parent.add(prel);
    MajorFragmentStat newFrag = new MajorFragmentStat();
    Prel newChild = ((Prel) prel.getInput()).accept(this, newFrag);

    if (newFrag.isSingular() && parent.isSingular() &&
        // if both of them have soft distribution, we can remove the exchange
        (!newFrag.isDistributionStrict() && !parent.isDistributionStrict())) {
      parent.merge(newFrag);
      //after merge, change to single stream
      RelTraitSet relTraits = newChild.getTraitSet().replace(DistributionTrait.SINGLETON);
      RelNode newSingletonChild = newChild.copy(relTraits, newChild.getInputs());
      return (Prel) newSingletonChild;
    } else {
      return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList((RelNode) newChild));
    }
  }
}
