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
package com.dremio.exec.store.sys.functions;

import com.dremio.exec.server.SabotContext;

import java.util.*;
import java.util.stream.Collectors;

public class FunctionsInfoIterator implements Iterator<Object> {

  private Map<String, Map<String, Object>> funcsMap;
  private Iterator<SysTableFunctionsInfo> iterator;
  private List<SysTableFunctionsInfo> sqlOperatorsList;
  private final boolean filterEmptyReturnType;


  public FunctionsInfoIterator(final SabotContext sabotContext) {
    Map<String, Map<String, Object>> map = sabotContext.getFunctionImplementationRegistry().generateMapWithRegisteredFunctions();
    this.funcsMap = map;
    this.sqlOperatorsList = sabotContext.getFunctionImplementationRegistry().generateListWithCalciteFunctions();
    this.iterator = this.getIterator(map);
    this.filterEmptyReturnType = false;
  }

  private Iterator<SysTableFunctionsInfo> getIterator(Map<String, Map<String, Object>> functionsMap) {
    List<SysTableFunctionsInfo> sysTableFunctionsInfoList = new ArrayList<>();
    functionsMap.entrySet().iterator();
    for (Map.Entry<String, Map<String, Object>> functionEntry : functionsMap.entrySet()) {
      String functionName = functionEntry.getKey();
      Map<String, Object> functionInfo = functionEntry.getValue();
      List<Map<String, Object>> signaturesList = (List<Map<String, Object>>) functionInfo.get("signatures");
      for (Map<String, Object> signature : signaturesList) {
        String returnType = (String) signature.get("returnType");
        List<Map<String, Object>> parametersList = (List<Map<String, Object>>) signature.get("parameterList");
        List<FunctionParameterInfo> functionParameterInfoList = new ArrayList<>();
        for (Map<String, Object> parameter : parametersList) {
          String parameterName = (String) parameter.get("parameterName");
          String parameterType = (String) parameter.get("parameterType");
          Boolean isOptional = Objects.equals((String) parameter.get("parameterType"), "true");
          functionParameterInfoList.add(new FunctionParameterInfo(parameterName, parameterType, isOptional));
        }
        sysTableFunctionsInfoList.add(new SysTableFunctionsInfo(functionName, returnType, functionParameterInfoList.toString()));
      }
    }
    // Check if we have duplicate functions in Calcite's list that have no return type
    if (this.filterEmptyReturnType) {
      for (SysTableFunctionsInfo item : sysTableFunctionsInfoList) {
        List<SysTableFunctionsInfo> filteredSqlItems = this.sqlOperatorsList.stream()
          .filter(sqlItem -> sqlItem.getName() == item.getName()
            && sqlItem.getReturn_type() == "")
          .collect(Collectors.toList());
        this.sqlOperatorsList.removeAll(filteredSqlItems);
      }
      sysTableFunctionsInfoList.addAll(this.sqlOperatorsList);
      List<SysTableFunctionsInfo> cleanedSysTableFunctionsInfoList = sysTableFunctionsInfoList.stream()
        .filter(item -> item.getReturn_type() != "")
        .collect(Collectors.toList());
      sysTableFunctionsInfoList = cleanedSysTableFunctionsInfoList;
    } else {
      sysTableFunctionsInfoList.addAll(this.sqlOperatorsList);
    }

    return sysTableFunctionsInfoList.iterator();
  }

  @Override
  public boolean hasNext() {
    return this.iterator.hasNext();
  }

  @Override
  public SysTableFunctionsInfo next() {
    return this.iterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
