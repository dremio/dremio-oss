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

<@pp.dropOutputFile />

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/DirectoryExplorers.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import org.apache.arrow.memory.ArrowBuf;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import javax.inject.Inject;

/**
 * This file is generated with Freemarker using the template exec/java-exec/src/main/codegen/templates/DirectoryExplorers.java
 */
public class DirectoryExplorers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectoryExplorers.class);

  <#list [ { "name" : "\"maxdir\"", "functionClassName" : "MaxDir", "comparison" : "compareTo(curr) < 0", "goal" : "maximum", "comparisonType" : "case-sensitive"},
           { "name" : "\"imaxdir\"", "functionClassName" : "IMaxDir", "comparison" : "compareToIgnoreCase(curr) < 0", "goal" : "maximum", "comparisonType" : "case-insensitive"},
           { "name" : "\"mindir\"", "functionClassName" : "MinDir", "comparison" : "compareTo(curr) > 0", "goal" : "minimum", "comparisonType" : "case-sensitive"},
           { "name" : "\"imindir\"", "functionClassName" : "IMinDir", "comparison" : "compareToIgnoreCase(curr) > 0", "goal" : "minimum", "comparisonType" : "case-insensitive"}
  ] as dirAggrProps>


  @FunctionTemplate(name = ${dirAggrProps.name}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class ${dirAggrProps.functionClassName} implements SimpleFunction {

    @Param VarCharHolder schema;
    @Param VarCharHolder table;
    @Output NullableVarCharHolder out;
    @Inject ArrowBuf buffer;
    @Inject com.dremio.exec.store.PartitionExplorer partitionExplorer;
    @Inject FunctionErrorContext errorContext;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      Iterable<String> subPartitions;
      try {
        subPartitions = partitionExplorer.getSubPartitions(
            com.dremio.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(schema),
            com.dremio.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(table),
            new java.util.ArrayList<String>(),
            new java.util.ArrayList<String>());
      } catch (com.dremio.exec.store.PartitionNotFoundException e) {
        throw errorContext.error(e)
          .message(
            String.format("Error in %s function: Table %s does not exist in schema %s ",
                ${dirAggrProps.name},
                com.dremio.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(table),
                com.dremio.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(schema))
          )
          .build();
      }
      java.util.Iterator<String> partitionIterator = subPartitions.iterator();
      if (!partitionIterator.hasNext()) {
        throw errorContext.error()
          .message(
            String.format("Error in %s function: Table %s in schema %s does not contain sub-partitions.",
                ${dirAggrProps.name},
                com.dremio.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(table),
                com.dremio.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(schema)
            )
          )
          .build();
      }
      String subPartitionStr = partitionIterator.next();
      String curr;
      // find the ${dirAggrProps.goal} directory in the list using a ${dirAggrProps.comparisonType} string comparison
      while (partitionIterator.hasNext()){
        curr = partitionIterator.next();
        if (subPartitionStr.${dirAggrProps.comparison}) {
          subPartitionStr = curr;
        }
      }
      String[] subPartitionParts = subPartitionStr.split("/");
      subPartitionStr = subPartitionParts[subPartitionParts.length - 1];
      byte[] result = subPartitionStr.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      out.buffer = buffer = buffer.reallocIfNeeded(result.length);

      out.buffer.setBytes(0, subPartitionStr.getBytes(), 0, result.length);
      out.start = 0;
      out.end = result.length;
    }
  }
  </#list>
}
