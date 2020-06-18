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
package com.dremio.exec.expr.fn.interpreter;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import javax.annotation.Nullable;
import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ValueHolderHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.types.Types.MinorType;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.NullExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.exec.expr.FunctionHolderExpr;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.SimpleFunctionHolder;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

public class InterpreterEvaluator {

  public static ValueHolder evaluateConstantExpr(FunctionContext functionContext, LogicalExpression expr) {
    InitVisitor initVisitor = new InitVisitor(functionContext);
    EvalVisitor evalVisitor = new EvalVisitor(null, functionContext);
    expr.accept(initVisitor, null);
    return expr.accept(evalVisitor, -1);
  }

  public static void evaluate(VectorAccessible incoming, FunctionContext functionContext, ValueVector outVV, LogicalExpression expr) {
    evaluate(incoming.getRecordCount(), functionContext, incoming, outVV, expr);
  }

  public static void evaluate(int recordCount, FunctionContext functionContext, VectorAccessible incoming, ValueVector outVV, LogicalExpression expr) {

    InitVisitor initVisitor = new InitVisitor(functionContext);
    EvalVisitor evalVisitor = new EvalVisitor(incoming, functionContext);

    expr.accept(initVisitor, incoming);

    for (int i = 0; i < recordCount; i++) {
      ValueHolder out = expr.accept(evalVisitor, i);
      TypeHelper.setValueSafe(outVV, i, out);
    }

    outVV.setValueCount(recordCount);

  }

  private static class InitVisitor extends AbstractExprVisitor<LogicalExpression, VectorAccessible, RuntimeException> {

    private FunctionContext functionContext;

    protected InitVisitor(FunctionContext functionContext) {
      super();
      this.functionContext = functionContext;
    }

    @Override
    public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression holderExpr, VectorAccessible incoming) {
      if (! (holderExpr.getHolder() instanceof SimpleFunctionHolder)) {
        throw new UnsupportedOperationException("Only Dremio simple UDF can be used in interpreter mode!");
      }

      SimpleFunctionHolder holder = (SimpleFunctionHolder) holderExpr.getHolder();

      for (int i = 0; i < holderExpr.args.size(); i++) {
        holderExpr.args.get(i).accept(this, incoming);
      }

      try {
        SimpleFunction interpreter = holder.createInterpreter();
        Field[] fields = interpreter.getClass().getDeclaredFields();
        for (Field f : fields) {
          if ( f.getAnnotation(Inject.class) != null ) {
            f.setAccessible(true);
            Class<?> fieldType = f.getType();
            if (FunctionContext.INJECTABLE_GETTER_METHODS.get(fieldType) != null) {
              Method method = functionContext.getClass().getMethod(FunctionContext.INJECTABLE_GETTER_METHODS.get(fieldType));
              f.set(interpreter, method.invoke(functionContext));
            } else {
              // Invalid injectable type provided, this should have been caught in FunctionConverter
              throw new IllegalArgumentException("Invalid injectable type requested in UDF: " + fieldType.getSimpleName());
            }
          } else { // do nothing with non-inject fields here
            continue;
          }
        }

        ((FunctionHolderExpr) holderExpr).setInterpreter(interpreter);
        return holderExpr;

      } catch (Exception ex) {
        throw new RuntimeException("Error in evaluating function of " + holderExpr.getName() + ": ", ex);
      }
    }

    @Override
    public LogicalExpression visitUnknown(LogicalExpression e, VectorAccessible incoming) throws RuntimeException {
      for (LogicalExpression child : e) {
        child.accept(this, incoming);
      }

      return e;
    }
  }


  public static class EvalVisitor extends AbstractExprVisitor<ValueHolder, Integer, RuntimeException> {
    private VectorAccessible incoming;
    private FunctionContext functionContext;

    protected EvalVisitor(VectorAccessible incoming, FunctionContext functionContext) {
      super();
      this.incoming = incoming;
      this.functionContext = functionContext;
    }

    public ArrowBuf getManagedBufferIfAvailable() {
      return functionContext.getManagedBuffer();
    }

    @Override
    public ValueHolder visitFunctionCall(FunctionCall call, Integer value) throws RuntimeException {
      return visitUnknown(call, value);
    }

    @Override
    public ValueHolder visitSchemaPath(SchemaPath path,Integer value) throws RuntimeException {
      return visitUnknown(path, value);
    }

    @Override
    public ValueHolder visitDateConstant(ValueExpressions.DateExpression dateExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getDateMilliHolder(dateExpr.getDate());
    }

    @Override
    public ValueHolder visitTimeConstant(ValueExpressions.TimeExpression timeExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getTimeMilliHolder(timeExpr.getTime());
    }

    @Override
    public ValueHolder visitTimeStampConstant(ValueExpressions.TimeStampExpression timestampExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getTimeStampMilliHolder(timestampExpr.getTimeStamp());
    }

    @Override
    public ValueHolder visitIntervalYearConstant(ValueExpressions.IntervalYearExpression intExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getIntervalYearHolder(intExpr.getIntervalYear());
    }

    @Override
    public ValueHolder visitIntervalDayConstant(ValueExpressions.IntervalDayExpression intExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getIntervalDayHolder(intExpr.getIntervalDay(), intExpr.getIntervalMillis());
    }

    @Override
    public ValueHolder visitBooleanConstant(ValueExpressions.BooleanExpression e,Integer value) throws RuntimeException {
      return ValueHolderHelper.getBitHolder(e.getBoolean() == false ? 0 : 1);
    }

    @Override
    public ValueHolder visitNullConstant(TypedNullConstant e,Integer value) throws RuntimeException {
      // create a value holder for the given type, defaults to NULL value if not set
      return TypeHelper.createValueHolder(getArrowMinorType(e.getCompleteType().toMinorType()));
    }

    // TODO - review what to do with these
    // **********************************
    @Override
    public ValueHolder visitConvertExpression(ConvertExpression e,Integer value) throws RuntimeException {
      return visitUnknown(e, value);
    }

    @Override
    public ValueHolder visitNullExpression(NullExpression e,Integer value) throws RuntimeException {
      return visitUnknown(e, value);
    }
    // TODO - review what to do with these (2 functions above)
    //********************************************

    @Override
    public ValueHolder visitFunctionHolderExpression(FunctionHolderExpression holderExpr, Integer inIndex) {
      if (! (holderExpr.getHolder() instanceof SimpleFunctionHolder)) {
        throw new UnsupportedOperationException("Only Dremio simple UDF can be used in interpreter mode!");
      }

      SimpleFunctionHolder holder = (SimpleFunctionHolder) holderExpr.getHolder();

      ValueHolder [] args = new ValueHolder [holderExpr.args.size()];
      for (int i = 0; i < holderExpr.args.size(); i++) {
        args[i] = holderExpr.args.get(i).accept(this, inIndex);
        // In case function use "NULL_IF_NULL" policy.
        if (holder.getNullHandling() == FunctionTemplate.NullHandling.NULL_IF_NULL) {
          // Case 1: parameter is non-nullable, argument is nullable.
          if (holder.getParameters()[i].getOldType().getMode() == DataMode.REQUIRED) {
            // Case 1.1 : argument is null, return null value holder directly.
            if (TypeHelper.isNull(args[i])) {
              return TypeHelper.createValueHolder(getArrowMinorType(holderExpr.getCompleteType().toMinorType()));
            } else {
              // Case 1.2: argument is nullable but not null value, deNullify it.
              args[i] = TypeHelper.deNullify(args[i]);
            }
          } else if (holder.getParameters()[i].getOldType().getMode() == DataMode.OPTIONAL) {
            // Case 2: parameter is nullable, argument is non-nullable. Nullify it.
            args[i] = TypeHelper.nullify(args[i]);
          }
        } else {
          DataMode argMode = TypeHelper.getValueHolderMajorType(args[i].getClass()).getMode();
          if (holder.getParameters()[i].getOldType().getMode() != argMode) {
            if (argMode == DataMode.OPTIONAL) {
              args[i] = TypeHelper.deNullify(args[i]);
            } else {
              args[i] = TypeHelper.nullify(args[i]);
            }
          }
        }
      }

      try {
        SimpleFunction interpreter =  ((FunctionHolderExpr) holderExpr).getInterpreter();

        Preconditions.checkArgument(interpreter != null, "interpreter could not be null when use interpreted model to evaluate function " + holder.getRegisteredNames()[0]);

        // the current input index to assign into the next available parameter, found using the @Param notation
        // the order parameters are declared in the java class for the Function is meaningful
        int currParameterIndex = 0;
        Field outField = null;
        try {
          Field[] fields = interpreter.getClass().getDeclaredFields();
          for (Field f : fields) {
            // if this is annotated as a parameter to the function
            if ( f.getAnnotation(Param.class) != null ) {
              f.setAccessible(true);
              if (currParameterIndex < args.length) {
                f.set(interpreter, args[currParameterIndex]);
              }
              currParameterIndex++;
            } else if ( f.getAnnotation(Output.class) != null ) {
              f.setAccessible(true);
              outField = f;
              // create an instance of the holder for the output to be stored in
              f.set(interpreter, f.getType().newInstance());
            }
          }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        if (args.length != currParameterIndex ) {
          throw new IllegalArgumentException(
              String.format("Wrong number of parameters provided to interpreted expression evaluation " +
                  "for function %s, expected %d parameters, but received %d.",
                  holderExpr.getName(), currParameterIndex, args.length));
        }
        if (outField == null) {
          throw new IllegalArgumentException("Malformed Function without a return type: " + holderExpr.getName());
        }
        interpreter.setup();
        interpreter.eval();
        ValueHolder out = (ValueHolder) outField.get(interpreter);
        out = TypeHelper.nullify(out);

        if (holder.getNullHandling() == NullHandling.NULL_IF_NULL) {
          TypeHelper.setNotNull(out);
        }

        return out;

      } catch (Exception ex) {
        throw new RuntimeException("Error in evaluating function of " + holderExpr.getName(), ex);
      }

    }

    @Override
    public ValueHolder visitBooleanOperator(BooleanOperator op, Integer inIndex) {
      // Apply short circuit evaluation to boolean operator.
      if (op.getName().equals("booleanAnd")) {
        return visitBooleanAnd(op, inIndex);
      }else if(op.getName().equals("booleanOr")) {
        return visitBooleanOr(op, inIndex);
      } else {
        throw new UnsupportedOperationException("BooleanOperator can only be booleanAnd, booleanOr. You are using " + op.getName());
      }
    }

    @Override
    public ValueHolder visitIfExpression(IfExpression ifExpr, Integer inIndex) throws RuntimeException {
      ValueHolder condHolder = ifExpr.ifCondition.condition.accept(this, inIndex);

      Preconditions.checkArgument (condHolder instanceof BitHolder || condHolder instanceof NullableBitHolder,
          "IfExpression's condition does not have type of BitHolder or NullableBitHolder.");

      Trivalent flag = isBitOn(condHolder);

      switch (flag) {
        case TRUE:
          return ifExpr.ifCondition.expression.accept(this, inIndex);
        case FALSE:
        case NULL:
          return ifExpr.elseExpression.accept(this, inIndex);
        default:
          throw new UnsupportedOperationException("No other possible choice. Something is not right");
      }
    }

    @Override
    public ValueHolder visitIntConstant(ValueExpressions.IntExpression e, Integer inIndex) throws RuntimeException {
      return ValueHolderHelper.getIntHolder(e.getInt());
    }

    @Override
    public ValueHolder visitFloatConstant(ValueExpressions.FloatExpression fExpr, Integer value) throws RuntimeException {
      return ValueHolderHelper.getFloat4Holder(fExpr.getFloat());
    }

    @Override
    public ValueHolder visitLongConstant(ValueExpressions.LongExpression intExpr, Integer value) throws RuntimeException {
      return ValueHolderHelper.getBigIntHolder(intExpr.getLong());
    }

    @Override
    public ValueHolder visitDoubleConstant(ValueExpressions.DoubleExpression dExpr, Integer value) throws RuntimeException {
      return ValueHolderHelper.getFloat8Holder(dExpr.getDouble());
    }

    @Override
    public ValueHolder visitDecimalConstant(ValueExpressions.DecimalExpression dExpr, Integer value) throws RuntimeException {
      return ValueHolderHelper.getNullableDecimalHolder(getManagedBufferIfAvailable(), dExpr.getDecimal(), dExpr.getPrecision(), dExpr.getScale());
    }

    @Override
    public ValueHolder visitQuotedStringConstant(final ValueExpressions.QuotedString e, Integer value) throws RuntimeException {
      return getConstantValueHolder(e.value, getArrowMinorType(e.getCompleteType().toMinorType()), new Function<ArrowBuf, ValueHolder>() {
        @Nullable
        @Override
        public ValueHolder apply(ArrowBuf buffer) {
          return ValueHolderHelper.getVarCharHolder(buffer, e.value);
        }
      });
    }


    @Override
    public ValueHolder visitUnknown(LogicalExpression e, Integer inIndex) throws RuntimeException {
      if (e instanceof ValueVectorReadExpression) {
        return visitValueVectorReadExpression((ValueVectorReadExpression) e, inIndex);
      } else {
        return super.visitUnknown(e, inIndex);
      }

    }

    protected ValueHolder visitValueVectorReadExpression(ValueVectorReadExpression e, Integer inIndex)
        throws RuntimeException {
      CompleteType ct = e.getCompleteType();

      if(!ct.isScalar()){
        throw new RuntimeException("Unable to read value.");
      }
      ValueVector vv = incoming.getValueAccessorById(ct.getValueVectorClass(), e.getFieldId().getFieldIds()).getValueVector();
      return TypeHelper.getValue(vv, inIndex.intValue());
    }

    // Use Kleene algebra for three-valued logic :
    //  value of boolean "and" when one side is null
    //    p       q     p and q
    //    true    null     null
    //    false   null     false
    //    null    true     null
    //    null    false    false
    //    null    null     null
    //  "and" : 1) if any argument is false, return false. false is earlyExitValue.
    //          2) if none argument is false, but at least one is null, return null.
    //          3) finally, return true (finalValue).
    private ValueHolder visitBooleanAnd(BooleanOperator op, Integer inIndex) {
      ValueHolder [] args = new ValueHolder [op.args.size()];
      boolean hasNull = false;
      for (int i = 0; i < op.args.size(); i++) {
        args[i] = op.args.get(i).accept(this, inIndex);

        Trivalent flag = isBitOn(args[i]);

        switch (flag) {
          case FALSE:
            return TypeHelper.nullify(ValueHolderHelper.getBitHolder(0));
          case NULL:
            hasNull = true;
          case TRUE:
        }
      }

      if (hasNull) {
        return ValueHolderHelper.getNullableBitHolder(true, 0);
      } else {
        return TypeHelper.nullify(ValueHolderHelper.getBitHolder(1));
      }
    }

    //  value of boolean "or" when one side is null
    //    p       q       p and q
    //    true    null     true
    //    false   null     null
    //    null    true     true
    //    null    false    null
    //    null    null     null
    private ValueHolder visitBooleanOr(BooleanOperator op, Integer inIndex) {
      ValueHolder [] args = new ValueHolder [op.args.size()];
      boolean hasNull = false;
      for (int i = 0; i < op.args.size(); i++) {
        args[i] = op.args.get(i).accept(this, inIndex);

        Trivalent flag = isBitOn(args[i]);

        switch (flag) {
          case TRUE:
            return TypeHelper.nullify(ValueHolderHelper.getBitHolder(1));
          case NULL:
            hasNull = true;
          case FALSE:
        }
      }

      if (hasNull) {
        return ValueHolderHelper.getNullableBitHolder(true, 0);
      } else {
        return TypeHelper.nullify(ValueHolderHelper.getBitHolder(0));
      }
    }

    public enum Trivalent {
      FALSE,
      TRUE,
      NULL
    }

    private Trivalent isBitOn(ValueHolder holder) {
      Preconditions.checkArgument(holder instanceof BitHolder || holder instanceof NullableBitHolder,
          "Input does not have type of BitHolder or NullableBitHolder.");

      if ( (holder instanceof BitHolder && ((BitHolder) holder).value == 1)) {
        return Trivalent.TRUE;
      } else if (holder instanceof NullableBitHolder && ((NullableBitHolder) holder).isSet == 1 && ((NullableBitHolder) holder).value == 1) {
        return Trivalent.TRUE;
      } else if (holder instanceof NullableBitHolder && ((NullableBitHolder) holder).isSet == 0) {
        return Trivalent.NULL;
      } else {
        return Trivalent.FALSE;
      }
    }

    private ValueHolder getConstantValueHolder(String value, MinorType type, Function<ArrowBuf, ValueHolder> holderInitializer) {
      return functionContext.getConstantValueHolder(value, type, holderInitializer);
    }

  }

}
