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
package com.dremio.common.types;

/**
 * Marker interface to determine entities which support standard coercion and up promotion rules of dremio data types.
 * To understand what comes under standard rules please refer to Table 1 of https://docs.dremio.com/software/sql-reference/data-types/
 */
public interface SupportsTypeCoercionsAndUpPromotions {

  TypeCoercionRules STANDARD_TYPE_COERCION_RULES = new TypeCoercionRules();
  SchemaUpPromotionRules STANDARD_TYPE_UP_PROMOTION_RULES = new SchemaUpPromotionRules();
  SchemaUpPromotionRules COMPLEX_INCOMPATIBLE_TO_VARCHAR_PROMOTION = new SchemaUpPromotionRulesComplexAndIncompatibleToVarchar();
  TypeCoercionRules COMPLEX_INCOMPATIBLE_TO_VARCHAR_COERCION = new TypeCoercionRulesComplexAndIncompatibleToVarchar();

  default TypeCoercionRules getTypeCoercionRules() {
    return STANDARD_TYPE_COERCION_RULES;
  }

  default SchemaUpPromotionRules getUpPromotionRules() {
    return STANDARD_TYPE_UP_PROMOTION_RULES;
  }

  default boolean isComplexToVarcharCoercionSupported() {
    return false;
  }
}
