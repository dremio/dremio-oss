/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.exec.expr.fn.impl;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

public class ContainsFunctions {

  @FunctionTemplate(name = "contains", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Contains implements SimpleFunction {

    @Param
    FieldReader field;
    @Param
    VarCharHolder query;
    @Output
    NullableBitHolder out;

    public void setup() {
    }

    public void eval() {
      if (true) {
        throw new RuntimeException("Contains function is not supported in this context");
      }
    }
  }
}
