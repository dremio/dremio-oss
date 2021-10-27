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
package com.dremio.sabot.op.join.vhash;

/**
 * Operational metrics for extra condition matching in the hash join operator
 */
public interface ExtraConditionStats {
  /**
   * Counts number of evaluations done against the generated code for the extra condition expression.
   *
   * @return number of evaluations done
   */
  default long getEvaluationCount() {
    return 0L;
  }

  /**
   * Counts evaluations that matched the build side and probe side data.
   *
   * @return number of matches
   */
  default long getEvaluationMatchedCount() {
    return 0L;
  }

  /**
   * Records setup time in nanos.
   *
   * @return setup time in nanos
   */
  default long getSetupNanos() {
    return 0L;
  }

}
