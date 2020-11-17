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

package com.dremio.exec.resolver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.dremio.common.types.TypeProtos.MinorType;
import com.google.common.collect.ImmutableMap;

public class ResolverTypePrecedence {

  public static final ImmutableMap<MinorType, Integer> PRECEDENCE_MAP;
  public static final ImmutableMap<MinorType, Set<MinorType>> SECONDARY_IMPLICIT_CAST_RULES;
  public static int MAX_IMPLICIT_CAST_COST;

  static {
    /* The precedenceMap is used to decide whether it's allowed to implicitly "promote"
     * one type to another type.
     *
     * The order that each type is inserted into HASHMAP decides its precedence.
     * First in ==> lowest precedence.
     * A type of lower precedence can be implicitly "promoted" to type of higher precedence.
     * For instance, NULL could be promoted to any other type;
     * tinyint could be promoted into int; but int could NOT be promoted into tinyint (due to possible precision loss).
     */
    int i = 0;

    Map<MinorType, Integer> precMap = new HashMap<MinorType, Integer>();
    precMap.put(MinorType.NULL, i += 2);       // NULL is legal to implicitly be promoted to any other type
    precMap.put(MinorType.VARBINARY, i += 2);
    precMap.put(MinorType.VARCHAR, i += 2);
    precMap.put(MinorType.BIT, i += 2);
    precMap.put(MinorType.TINYINT, i += 2);   //type with few bytes is promoted to type with more bytes ==> no data loss.
    precMap.put(MinorType.UINT1, i += 2);     //signed is legal to implicitly be promoted to unsigned.
    precMap.put(MinorType.SMALLINT, i += 2);
    precMap.put(MinorType.UINT2, i += 2);
    precMap.put(MinorType.INT, i += 2);
    precMap.put(MinorType.UINT4, i += 2);
    precMap.put(MinorType.BIGINT, i += 2);
    precMap.put(MinorType.UINT8, i += 2);
    precMap.put(MinorType.DECIMAL, i += 2);
    precMap.put(MinorType.FLOAT4, i += 2);
    precMap.put(MinorType.FLOAT8, i += 2);
    precMap.put(MinorType.DATE, i += 2);
    precMap.put(MinorType.TIMESTAMP, i += 2);
    precMap.put(MinorType.TIME, i += 2);
    precMap.put(MinorType.INTERVALDAY, i+= 2);
    precMap.put(MinorType.INTERVALYEAR, i+= 2);
    precMap.put(MinorType.UNION, i += 2);
    precMap.put(MinorType.FIXEDSIZEBINARY, i += 2);
    precMap.put(MinorType.LIST, i += 2);
    precMap.put(MinorType.STRUCT, i += 2);
    PRECEDENCE_MAP = ImmutableMap.copyOf(precMap);
    MAX_IMPLICIT_CAST_COST = i;


    /* Currently implicit cast follows the precedence rules.
     * It may be useful to perform an implicit cast in
     * the opposite direction as specified by the precedence rules.
     *
     * For example: As per the precedence rules we can implicitly cast
     * from VARCHAR ---> BIGINT , but based upon some functions (eg: substr, concat)
     * it may be useful to implicitly cast from BIGINT ---> VARCHAR.
     *
     * To allow for such cases we have a secondary set of rules which will allow the reverse
     * implicit casts. Currently we only allow the reverse implicit cast to VARCHAR so we don't
     * need any cost associated with it, if we add more of these that may collide we can add costs.
     */
    Map<MinorType, Set<MinorType>> secondaryImplicitCastRules = new HashMap<>();
    HashSet<MinorType> rule = new HashSet<>();

    // Following cast functions should exist
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIME);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.INTERVALYEAR);
    rule.add(MinorType.INTERVALDAY);
    secondaryImplicitCastRules.put(MinorType.VARCHAR, rule);

    rule = new HashSet<>();

    // Be able to implicitly cast to VARBINARY
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.VARCHAR);
    secondaryImplicitCastRules.put(MinorType.VARBINARY, rule);

    SECONDARY_IMPLICIT_CAST_RULES = ImmutableMap.copyOf(secondaryImplicitCastRules);
  }

}
