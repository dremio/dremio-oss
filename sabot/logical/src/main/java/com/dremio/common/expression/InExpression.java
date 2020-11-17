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
package com.dremio.common.expression;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.util.ByteFunctionHelpers;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.LongHashSet;
import com.dremio.common.expression.fn.impl.HashHelper;
import com.dremio.common.expression.visitors.ExprVisitor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;

/**
 * A special expression used during compilation that is an optimization over a multi-or.
 *
 * Holds variable evaluation expression that will be checked against a set of constant value expression.
 * Basically the compilation equivalent of an IN expression holding only literals in SQL
 */
public class InExpression extends LogicalExpressionBase {

  @VisibleForTesting
  public static final AtomicLong COUNT = new AtomicLong();

  private final LogicalExpression eval;
  private final List<LogicalExpression> constants;

  public InExpression(LogicalExpression eval, List<LogicalExpression> constants) {
    assert COUNT.incrementAndGet() > 0;
    this.eval = eval;
    this.constants = constants;
  }

  public LogicalExpression getEval() {
    return eval;
  }

  public List<LogicalExpression> getConstants() {
    return constants;
  }

  @Override
  public CompleteType getCompleteType() {
    return CompleteType.BIT;
  }

  public CompleteType getInType() {
    return eval.getCompleteType();
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitInExpression(this, value);
  }

  public boolean isVarType() {
    return eval.getCompleteType().isVariableWidthScalar();
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return ImmutableList.<LogicalExpression>builder().add(eval).addAll(constants).build().iterator();
  }

  public JClass getListType(JCodeModel model) {
    final CompleteType type = eval.getCompleteType();
    if(CompleteType.BIGINT.equals(type)) {
      return model.ref(LongSet.class);
    }else if(CompleteType.INT.equals(type)) {
      return model.ref(IntSet.class);
    }else if(CompleteType.DATE.equals(type)) {
      return model.ref(LongSet.class);
    }else if(CompleteType.TIME.equals(type)) {
      return model.ref(IntSet.class);
    }else if(CompleteType.TIMESTAMP.equals(type)) {
      return model.ref(LongSet.class);
    }else if(CompleteType.VARCHAR.equals(type)) {
      return model.ref(BytesSet.class);
    }else if(CompleteType.VARBINARY.equals(type)) {
      return model.ref(BytesSet.class);
    } else {
      throw new UnsupportedOperationException(type.toString() + " does not support in list optimizations.");
    }
  }

  public JClass getHolderType(JCodeModel model) {
    return CodeModelArrowHelper.getHolderType(getInType(), model);
  }

  @Override
  public String toString() {
    return eval.toString() + " IN ( " + Joiner.on(", ").join(constants) + " )";
  }

  public static class LongSet {
    private final LongHashSet longSet = new LongHashSet();

    public LongSet(NullableBigIntHolder[] holders) {
      for(NullableBigIntHolder h : holders) {
        if(h.isSet == 0) {
          // one null is never equal to another null.
          continue;
        }

        longSet.add(h.value);
      }
    }

    public LongSet(NullableDateMilliHolder[] holders) {
      for(NullableDateMilliHolder h : holders) {
        if(h.isSet == 0) {
          // one null is never equal to another null.
          continue;
        }

        longSet.add(h.value);
      }
    }

    public LongSet(NullableTimeStampMilliHolder[] holders) {
      for(NullableTimeStampMilliHolder h : holders) {
        if(h.isSet == 0) {
          // one null is never equal to another null.
          continue;
        }

        longSet.add(h.value);
      }
    }

    public int isContained(int isSet, long value) {
      if(isSet == 0) {
        return 0;
      }

      return longSet.contains(value) ? 1 : 0;
    }
  }

  public static class IntSet {
    private final IntHashSet intSet = new IntHashSet();

    public IntSet(NullableIntHolder[] holders) {
      for(NullableIntHolder h : holders) {
        if(h.isSet == 0) {
          // one null is never equal to another null.
          continue;
        }

        intSet.add(h.value);
      }
    }

    public IntSet(NullableTimeMilliHolder[] holders) {
      for(NullableTimeMilliHolder h : holders) {
        if(h.isSet == 0) {
          // one null is never equal to another null.
          continue;
        }

        intSet.add(h.value);
      }
    }

    public int isContained(int isSet, int value) {
      if(isSet == 0) {
        return 0;
      }

      return intSet.contains(value) ? 1 : 0;
    }
  }

  private static class BytesHolder {
    private final int start;
    private final int end;
    private final ArrowBuf buf;

    public BytesHolder(int start, int end, ArrowBuf buf) {
      super();
      this.start = start;
      this.end = end;
      this.buf = buf;
    }

    @Override
    public int hashCode() {
      return HashHelper.hash32(start, end, buf, 0);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (! (obj instanceof BytesHolder) ) {
        return false;
      }

      BytesHolder other = (BytesHolder) obj;
      return ByteFunctionHelpers.equal(buf, start, end, other.buf, other.start, other.end) == 1;
    }

  }

  public static class BytesSet {
    private final HashSet<BytesHolder> byteArraySet = new HashSet<>();

    public BytesSet(NullableVarBinaryHolder[] holders) {
      for(NullableVarBinaryHolder h : holders) {
        if(h.isSet == 0) {
          // one null is never equal to another null.
          continue;
        }
        byteArraySet.add(new BytesHolder(h.start, h.end, h.buffer));
      }
    }

    public BytesSet(NullableVarCharHolder[] holders) {
      for(NullableVarCharHolder h : holders) {
        if(h.isSet == 0) {
          // one null is never equal to another null.
          continue;
        }
        byteArraySet.add(new BytesHolder(h.start, h.end, h.buffer));
      }
    }

    public int isContained(int isSet, int start, int end, ArrowBuf buf) {
      if(isSet == 0) {
        // one null is never equal to another null.
        return 0;
      }

      return byteArraySet.contains(new BytesHolder(start, end, buf)) ? 1 : 0;
    }
  }
}
