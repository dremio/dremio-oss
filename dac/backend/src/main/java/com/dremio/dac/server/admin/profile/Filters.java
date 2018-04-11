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
package com.dremio.dac.server.admin.profile;

import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

interface Filters {
  Predicate<MinorFragmentProfile> hasOperators = new Predicate<MinorFragmentProfile>() {
    public boolean apply(MinorFragmentProfile arg0) {
      return arg0.getOperatorProfileCount() != 0;
    }
  };

  Predicate<MinorFragmentProfile> hasTimes = new Predicate<MinorFragmentProfile>() {
    public boolean apply(MinorFragmentProfile arg0) {
      return arg0.hasStartTime() && arg0.hasEndTime();
    }
  };

  Predicate<MinorFragmentProfile> hasOperatorsAndTimes = Predicates.and(Filters.hasOperators, Filters.hasTimes);

  Predicate<MinorFragmentProfile> missingOperatorsOrTimes = Predicates.not(hasOperatorsAndTimes);
}
