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
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.gandiva.evaluator.ExpressionRegistry;
import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;

import com.google.common.collect.Sets;

/**
 * Wrapper around gandiva ExpressionRegistry.
 */
public class GandivaRegistryWrapper {
  private static volatile GandivaRegistryWrapper INSTANCE;

  private final Set<FunctionSignature> supportedFunctionsNonDecimal = Sets.newHashSet();
  private final Set<FunctionSignature> supportedFunctionsDecimal;
  private final Set<ArrowType> supportedTypes;
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GandivaRegistryWrapper.class);


  private GandivaRegistryWrapper() throws GandivaException {
    this.supportedTypes = ExpressionRegistry.getInstance().getSupportedTypes();

    Set<FunctionSignature> signatures = ExpressionRegistry.getInstance().getSupportedFunctions();
    Set<FunctionSignature> updatedSignatures = new HashSet<>();
    for (FunctionSignature signature : signatures) {
      logger.debug("Gandiva function " + signature.getName());
      logger.debug("return type is " + signature.getReturnType());
      logger.debug("signature is " + signature);
      FunctionSignature updated = signature;
      if (shouldBlackListFunction(signature)) {
        continue;
      }

      // To make this fit in dremio model of type inference, add dummy args for precision and
      // scale.
      if (signature.getName().equals("castDECIMAL") || signature.getName().equals("castDECIMALNullOnOverflow")) {
        List<List<ArrowType>> args = new ArrayList<>(signature.getParamTypes());
        List<ArrowType> p1 = new ArrayList<ArrowType>();
        p1.add(new ArrowType.Int(64, true)); // precision
        args.add(p1);

        List<ArrowType> p2 = new ArrayList<ArrowType>();
        p2.add(new ArrowType.Int(64, true)); // scale
        args.add(p2);

        updated = new FunctionSignature(signature.getName(), signature.getReturnType(),
          signature.getReturnListType(), args);
      }
      updatedSignatures.add(updated);
      addNonDecimalMethods(signature, updated);
    }
    this.supportedFunctionsDecimal = updatedSignatures;
  }

  private boolean shouldBlackListFunction(FunctionSignature signature) {
    ArrowType.Date dateDay = new ArrowType.Date(DateUnit.DAY);
    boolean dateDayArg = false;
    for (List<ArrowType> args : signature.getParamTypes()) {
      if (args.get(0).equals(dateDay)) {
        dateDayArg = true;
        break;
      }
    }

    // suppress all date32 functions. date32 is not a supported dremio type;
    if (signature.getReturnType() != null && (dateDayArg || signature.getReturnType().equals(dateDay))) {
      return true;
    }

    // blacklisting to_date variants on string param till DX-24037 is fixed.
    if (signature.getName().equalsIgnoreCase("to_date") || signature.getName().equalsIgnoreCase("castDATE")) {
      return signature.getParamTypes().size() == 2
        && signature.getParamTypes().get(0).get(0).equals(new ArrowType.Utf8())
        && signature.getParamTypes().get(1).get(0).equals(new ArrowType.Utf8());
    }

    // DX-32437; blacklisting temporarily
    if (signature.getName().equalsIgnoreCase("convert_replaceUTF8")) {
      return true;
    }

    //DX-84842
    if (signature.getName().equalsIgnoreCase("array_contains") ||
      signature.getName().equalsIgnoreCase("array_remove")) {
      return true;
    }

    if ((signature.getName().equalsIgnoreCase("yearweek") || signature.getName().equalsIgnoreCase("weekofyear") || signature.getName().equalsIgnoreCase("day")) && !ArrowTypeID.Int.equals(signature.getReturnType().getTypeID())) {
      return true;
    }

    //blacklisted while upgrading arrow and DX-44699
    String[] blacklisted_funcs = {"castVARBINARY", "regexp_replace", "castINTERVALDAY", "castNULLABLEINTERVALDAY", "castBIT", "castBOOLEAN", "to_timestamp", "from_utc_timestamp", "to_utc_timestamp"};

    for(String str: blacklisted_funcs) {
      if (signature.getName().equalsIgnoreCase(str)) {
        return true;
      }
    }

    return false;
  }

  private void addNonDecimalMethods(FunctionSignature signature, FunctionSignature updated) {
    Optional<List<ArrowType>> decimalParam = signature.getParamTypes()
                                                .stream()
                                                .filter(arrowType -> arrowType.get(0).getTypeID()
                                                .equals(ArrowType.ArrowTypeID.Decimal))
                                                .findFirst();
    boolean decimalReturnType = signature.getReturnType().getTypeID().equals(ArrowType
      .ArrowTypeID.Decimal);
    // ignore all decimal functions if v2 is not enabled.
    if (!decimalParam.isPresent() && !decimalReturnType) {
      supportedFunctionsNonDecimal.add(updated);
    }
  }

  public static GandivaRegistryWrapper getInstance() throws GandivaException {
    if (INSTANCE == null) {
      synchronized (GandivaRegistryWrapper.class) {
        INSTANCE = new GandivaRegistryWrapper();
      }
    }
    return INSTANCE;
  }

  public Set<ArrowType> getSupportedTypes() {
    return supportedTypes;
  }

  public Set<FunctionSignature> getSupportedFunctionsIncludingDecimal() {
    return supportedFunctionsDecimal;
  }

  public Set<FunctionSignature> getSupportedFunctionsExcludingDecimal() {
    return supportedFunctionsNonDecimal;
  }
}
