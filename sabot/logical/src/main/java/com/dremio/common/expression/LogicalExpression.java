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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.parser.ExprLexer;
import com.dremio.common.expression.parser.ExprParser;
import com.dremio.common.expression.parser.ExprParser.parse_return;
import com.dremio.common.expression.visitors.ExprVisitor;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

//@JsonDeserialize(using = LogicalExpression.De.class)  // Excluded as we need to register this with the SabotConfig.
@JsonSerialize(using = LogicalExpression.Se.class)
public interface LogicalExpression extends Iterable<LogicalExpression>{
  static final Logger logger = LoggerFactory.getLogger(LogicalExpression.class);

  abstract CompleteType getCompleteType();

  <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E;

  default int getSizeOfChildren() {
    return 0;
  }

  @Override
  default Iterator<LogicalExpression> iterator() {
    return Collections.emptyIterator();
  }

  int getSelfCost();
  int getCumulativeCost();

  public static class De extends StdDeserializer<LogicalExpression> {
    public De() {
      super(LogicalExpression.class);
    }

    @Override
    public LogicalExpression deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      String expr = jp.getText();

      if (expr == null || expr.isEmpty()) {
        return null;
      }
      try {
        // logger.debug("Parsing expression string '{}'", expr);
        ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        ExprParser parser = new ExprParser(tokens);

        parse_return ret = parser.parse();

        return ret.e;
      } catch (RecognitionException e) {
        throw new RuntimeException(e);
      }
    }

  }

  public static class Se extends StdSerializer<LogicalExpression> {

    protected Se() {
      super(LogicalExpression.class);
    }

    @Override
    public void serialize(LogicalExpression value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
        JsonGenerationException {
      StringBuilder sb = new StringBuilder();
      ExpressionStringBuilder esb = new ExpressionStringBuilder();
      value.accept(esb, sb);
      jgen.writeString(sb.toString());
    }

  }

}
