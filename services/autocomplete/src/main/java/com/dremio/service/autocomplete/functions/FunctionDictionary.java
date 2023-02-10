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
package com.dremio.service.autocomplete.functions;

import static java.util.stream.Collectors.groupingBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;

import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.service.functions.FunctionListDictionary;
import com.dremio.service.functions.generator.FunctionFactory;
import com.dremio.service.functions.generator.FunctionMerger;
import com.dremio.service.functions.model.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Dictionary of case insensitive function name to SqlFunction.
 */
public final class FunctionDictionary {
  private final ImmutableMap<String, Function> map;

  private FunctionDictionary(ImmutableMap<String, Function> map) {
    Preconditions.checkNotNull(map);
    this.map = map;
  }

  public Optional<Function> tryGetValue(String functionName) {
    return Optional.ofNullable(this.map.get(functionName.toUpperCase()));
  }

  public Collection<Function> getValues() {
    return this.map.values();
  }
  public Collection<String> getKeys() { return this.map.keySet(); }

  public static FunctionDictionary create(List<Function> functions) {
    Map<String, Function> functionMap = new HashMap<>();
    for (Function function : functions) {
      functionMap.put(function.getName().toUpperCase(), function);
    }

    return new FunctionDictionary(ImmutableMap.copyOf(functionMap));
  }

  public static FunctionDictionary create(
    SqlOperatorTable sqlOperatorTable,
    SimpleCatalog<?> catalog,
    boolean generateSignatures) {
    final DremioCatalogReader catalogReader = new DremioCatalogReader(
      catalog,
      JavaTypeFactoryImpl.INSTANCE);
    final SqlOperatorTable chainedOperatorTable = ChainedSqlOperatorTable.of(
      sqlOperatorTable,
      catalogReader);

    FunctionFactory functionFactory = FunctionFactory.makeFunctionFactory(chainedOperatorTable);

    Map<String, List<SqlFunction>> functionsGroupedByName =  chainedOperatorTable
      .getOperatorList()
      .stream()
      .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
      .map(sqlOperator -> (SqlFunction) sqlOperator)
      .collect(groupingBy(sqlFunction -> sqlFunction.getName().toUpperCase()));

    List<Function> convertedFunctions = new ArrayList<>();
    for (String functionName : functionsGroupedByName.keySet()) {
      Optional<Function> optional = FunctionListDictionary.tryGetFunction(functionName);
      Function convertedFunction = optional.orElseGet(() ->
        generateSignatures
          ? FunctionMerger.merge(
          functionsGroupedByName
            .get(functionName)
            .stream()
            .map(functionFactory::fromSqlFunction)
            .collect(ImmutableList.toImmutableList()))
          : Function.builder()
          .name(functionName)
          .build());

      convertedFunctions.add(convertedFunction);
    }

    return create(convertedFunctions);
  }
}
