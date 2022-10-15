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
package com.dremio.exec.expr.fn;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.calcite.sql.SqlFunction;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.exec.expr.fn.impl.hive.ObjectInspectorHelper;
import com.dremio.exec.planner.sql.HiveUDFOperator;
import com.dremio.exec.planner.sql.OperatorTable;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;

public class HiveFunctionRegistry implements PluggableFunctionRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveFunctionRegistry.class);

  private ArrayListMultimap<String, Class<? extends GenericUDF>> methodsGenericUDF = ArrayListMultimap.create();
  private ArrayListMultimap<String, Class<? extends UDF>> methodsUDF = ArrayListMultimap.create();
  private Set<Class<?>> nonDeterministicUDFs = new HashSet<>();

  /**
   * Scan the classpath for implementation of GenericUDF/UDF interfaces,
   * extracts function annotation and store the
   * (function name) --> (implementation class) mappings.
   * @param classpathScan
   */
  @Inject
  public HiveFunctionRegistry(final ScanResult classpathScan) {
    Set<Class<? extends GenericUDF>> genericUDFClasses = classpathScan.getImplementations(GenericUDF.class);
    for (Class<? extends GenericUDF> clazz : genericUDFClasses) {
      register(clazz, methodsGenericUDF);
    }

    Set<Class<? extends UDF>> udfClasses = classpathScan.getImplementations(UDF.class);
    for (Class<? extends UDF> clazz : udfClasses) {
      register(clazz, methodsUDF);
    }

    if (logger.isTraceEnabled()) {
      StringBuilder allHiveFunctions = new StringBuilder();
      for (Map.Entry<String, Class<? extends GenericUDF>> method : methodsGenericUDF.entries()) {
        allHiveFunctions.append(method.toString()).append("\n");
      }
      logger.trace("Registered Hive GenericUDFs: [\n{}]", allHiveFunctions);

      StringBuilder allUDFs = new StringBuilder();
      for (Map.Entry<String, Class<? extends UDF>> method : methodsUDF.entries()) {
        allUDFs.append(method.toString()).append("\n");
      }
      logger.trace("Registered Hive UDFs: [\n{}]", allUDFs);
      StringBuilder allNonDeterministic = new StringBuilder();
      for (Class<?> clz : nonDeterministicUDFs) {
        allNonDeterministic.append(clz.toString()).append("\n");
      }
      logger.trace("Registered Hive nonDeterministicUDFs: [\n{}]", allNonDeterministic);
    }
  }

  @Override
  public void register(OperatorTable operatorTable, boolean isDecimalV2Enabled) {
    for (String name : Sets.union(methodsGenericUDF.asMap().keySet(), methodsUDF.asMap().keySet())) {
      SqlFunction hiveFunction = new HiveUDFOperator(
        name.toUpperCase(),
        new PlugginRepositorySqlReturnTypeInference(this, isDecimalV2Enabled));
      operatorTable.add(name, hiveFunction);
    }
  }

  private <C,I> void register(Class<? extends I> clazz, ArrayListMultimap<String,Class<? extends I>> methods) {
    Description desc = clazz.getAnnotation(Description.class);
    String[] names;
    if (desc != null) {
      names = desc.name().split(",");
      for (int i=0; i<names.length; i++) {
        names[i] = names[i].trim();
      }
    }else{
      names = new String[]{clazz.getName().replace('.', '_')};
    }

    UDFType type = clazz.getAnnotation(UDFType.class);
    if (type != null && !type.deterministic()) {
      nonDeterministicUDFs.add(clazz);
    }


    for(int i=0; i<names.length;i++) {
      methods.put(names[i].toLowerCase(), clazz);
    }
  }

  /**
   * Find the UDF class for given function name and check if it accepts the given input argument
   * types. If a match is found, create a holder and return
   * @param call
   * @return
   */
  @Override
  public HiveFuncHolder getFunction(FunctionCall call) {
    HiveFuncHolder h;

    h = resolveFunction(call, false);
    if (h != null) {
      return h;
    }

    return resolveFunction(call, true);
  }

  /**
   * Helper method which resolves the given function call to a Hive UDF. It takes an argument
   * <i>varCharToStringReplacement</i> which tells to use hive STRING(true) or VARCHAR(false) type for Dremio VARCHAR type
   * and search Hive UDF registry using this replacement.
   *
   * TODO: This is a rudimentary function resolver. Need to include more implicit casting such as DECIMAL28 to
   * DECIMAL38 as Hive UDFs can accept only DECIMAL38 type.
   */
  private HiveFuncHolder resolveFunction(FunctionCall call, boolean varCharToStringReplacement) {
    HiveFuncHolder holder;

    CompleteType[] argTypes = new CompleteType[call.args.size()];
    ObjectInspector[] argOIs = new ObjectInspector[call.args.size()];
    for (int i=0; i<call.args.size(); i++) {
      try {
        argTypes[i] = call.args.get(i).getCompleteType();
        argOIs[i] = ObjectInspectorHelper.getObjectInspector(DataMode.REQUIRED, argTypes[i].toMinorType(),
             varCharToStringReplacement);
      } catch(Exception e) {
        // Hive throws errors if there are unsupported types. Consider there is no hive UDF supporting the
        // given argument types
        logger.info("Failed to find a hive function for given FunctionCall: '{}' and the argument number is '{}' and the error is", call, i, e);
        return null;
      }
    }

    String funcName = call.getName().toLowerCase();
    List<Exception> errors = new ArrayList<>();
    // search in GenericUDF list
    for (Class<? extends GenericUDF> clazz: methodsGenericUDF.get(funcName)) {
      holder = matchAndCreateGenericUDFHolder(clazz, argTypes, argOIs, errors);
      if (holder != null) {
        return holder;
      }
    }

    // search in UDF list
    for (Class<? extends UDF> clazz : methodsUDF.get(funcName)) {
      holder = matchAndCreateUDFHolder(call.getName(), clazz, argTypes, argOIs, errors);
      if (holder != null) {
        return holder;
      }
    }

    if (errors.size() == 0) {
      logger.info("Unable to find a hive function with the same name for the following function: '{}'", call);
    } else {
      logger.info("Failed to instantiate Hive class for the following function: '{}' and the error is '{}'", call, appendAllTheErrors(errors));
    }
    return null;
  }

  private String appendAllTheErrors(List<Exception> errors) {
    StringBuilder sb = new StringBuilder();
    for(int i=0;i<errors.size();i++) {
      if(i>0) {
        sb.append("; ");
      }
      sb.append(errors.get(i).getMessage());
    }
    return sb.toString();
  }

  private HiveFuncHolder matchAndCreateGenericUDFHolder(Class<? extends GenericUDF> udfClazz,
                                              CompleteType[] argTypes,
                                              ObjectInspector[] argOIs, List<Exception> errors) {
    // probe UDF to find if the arg types and acceptable
    // if acceptable create a holder object
    try {
      GenericUDF udfInstance = udfClazz.newInstance();
       ObjectInspector returnOI = udfInstance.initialize(argOIs);
      return new HiveFuncHolder(
        udfClazz,
        argTypes,
        returnOI,
        CompleteType.fromMinorType(ObjectInspectorHelper.getMinorType(returnOI)),
        nonDeterministicUDFs.contains(udfClazz));
    } catch (Exception e) {
      errors.add(e);
      /*ignore this*/
    }

    return null;
  }

  private HiveFuncHolder matchAndCreateUDFHolder(String udfName,
                                                 Class<? extends UDF> udfClazz,
                                                 CompleteType[] argTypes,
                                                 ObjectInspector[] argOIs, List<Exception> errors) {
    try {
      GenericUDF udfInstance = new GenericUDFBridge(udfName, false/* is operator */, udfClazz.getName());
      ObjectInspector returnOI = udfInstance.initialize(argOIs);

      return new HiveFuncHolder(
        udfName,
        udfClazz,
        argTypes,
        returnOI,
        CompleteType.fromMinorType(ObjectInspectorHelper.getMinorType(returnOI)),
        nonDeterministicUDFs.contains(udfClazz));
    } catch (Exception e) {
      errors.add(e);
      /*ignore this*/
    }

    return null;
  }

  ArrayListMultimap<String, Class<? extends GenericUDF>> getMethodsGenericUDF() {
    return methodsGenericUDF;
  }

  ArrayListMultimap<String, Class<? extends UDF>> getMethodsUDF() {
    return methodsUDF;
  }
}
