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
package com.dremio.reflection.hints;

import com.dremio.sabot.kernel.proto.DisjointFilterExplanation;
import com.dremio.sabot.kernel.proto.FieldMissingExplanation;
import com.dremio.sabot.kernel.proto.FilterOverSpecifiedExplanation;
import com.dremio.sabot.kernel.proto.ReflectionExplanation;
import com.dremio.sabot.kernel.proto.ReflectionExplanationType;

public class ExplanationUtil {
  private ExplanationUtil() {
  }

  static ReflectionExplanation fieldMissingExplanation(String name, int columnIndex) {
    return new ReflectionExplanation()
        .setExplanation(ReflectionExplanationType.FIELD_MISSING)
        .setFieldMissing(new FieldMissingExplanation()
            .setColumnIndex(columnIndex)
            .setColumnName(name));
  }

  static ReflectionExplanation filterOverSpecified(String filterDescription) {
    return new ReflectionExplanation()
        .setExplanation(ReflectionExplanationType.FILTER_OVER_SPECIFIED)
        .setFilterOverSpecified(new FilterOverSpecifiedExplanation()
            .setFilter(filterDescription));
  }

  static ReflectionExplanation disjointFilterExplanation(String filterDescription) {
    return new ReflectionExplanation()
        .setExplanation(ReflectionExplanationType.DISJOINT_FILTER)
        .setDisjointFilter(new DisjointFilterExplanation()
            .setFilter(filterDescription));
  }

  static String toString(ReflectionExplanation reflectionExplanation) {
    switch (reflectionExplanation.getExplanation()){
      case FIELD_MISSING:
        return reflectionExplanation.getFieldMissing().toString();
      case DISJOINT_FILTER:
        return reflectionExplanation.getDisjointFilter().toString();
      case FILTER_OVER_SPECIFIED:
        return reflectionExplanation.getFilterOverSpecified().toString();
      default:
        return reflectionExplanation.toString();
    }
  }
}
