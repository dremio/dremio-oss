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
import java.util.stream.Collectors;

import org.apache.arrow.gandiva.evaluator.ExpressionRegistry;
import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.google.common.collect.Sets;

/**
 * Wrapper around gandiva ExpressionRegistry.
 */
public class GandivaRegistryWrapper {
  private static volatile GandivaRegistryWrapper INSTANCE;

  private final Set<FunctionSignature> supportedFunctionsNonDecimal = Sets.newHashSet();
  private final Set<FunctionSignature> supportedFunctionsDecimal;
  private final Set<ArrowType> supportedTypes;

  private GandivaRegistryWrapper() throws GandivaException {
    this.supportedTypes = ExpressionRegistry.getInstance().getSupportedTypes();

    Set<FunctionSignature> signatures = ExpressionRegistry.getInstance().getSupportedFunctions();
    Set<FunctionSignature> updatedSignatures = new HashSet<>();
    for (FunctionSignature signature : signatures) {
      FunctionSignature updated = signature;
      if (shouldBlackListFunction(signature)) {
        continue;
      }

      // To make this fit in dremio model of type inference, add dummy args for precision and
      // scale.
      if (signature.getName().equals("castDECIMAL") || signature.getName().equals("castDECIMALNullOnOverflow")) {
        List<ArrowType> args = new ArrayList<>(signature.getParamTypes());
        args.add(new ArrowType.Int(64, true)); // precision
        args.add(new ArrowType.Int(64, true)); // scale

        updated = new FunctionSignature(signature.getName(), signature.getReturnType(), args);
      }
      updatedSignatures.add(updated);
      addNonDecimalMethods(signature, updated);
    }
    this.supportedFunctionsDecimal = updatedSignatures;
  }

  private boolean shouldBlackListFunction(FunctionSignature signature) {
    ArrowType.Date dateDay = new ArrowType.Date(DateUnit.DAY);
    List<ArrowType> dateDayArgs =
      signature.getParamTypes().stream().filter(type-> {
        return type.equals(dateDay);
      }).collect(Collectors.toList());

    // suppress all date32 functions. date32 is not a supported dremio type;
    if (!dateDayArgs.isEmpty() || signature.getReturnType().equals(dateDay)) {
      return true;
    }

    return false;
  }

  private void addNonDecimalMethods(FunctionSignature signature, FunctionSignature updated) {
    Optional<ArrowType> decimalParam = signature.getParamTypes()
                                                .stream()
                                                .filter(arrowType -> arrowType.getTypeID()
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
