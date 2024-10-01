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
package com.dremio.exec.planner.sql.evaluator;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.test.TemporarySystemProperties;
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CurrentTimeEvaluatorTests {

  @Mock private EvaluationContext cx;
  @Mock private ContextInformation contextInformation;
  @Mock private RexCall call;

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  private CurrentTimeEvaluator currentTimeEvaluator;

  @Test
  public void testCurrentTimeWithCustomTimeZone() {
    try {
      properties.set("user.timezone", "Etc/GMT-3");
      long currentTimeMillis = System.currentTimeMillis();
      GregorianCalendar current = new GregorianCalendar();
      current.setTimeInMillis(currentTimeMillis);
      currentTimeEvaluator = CurrentTimeEvaluator.INSTANCE;
      when(contextInformation.getQueryStartTime()).thenReturn(currentTimeMillis);
      when(cx.getContextInformation()).thenReturn(contextInformation);
      when(cx.getRexBuilder()).thenReturn(new DremioRexBuilder(JavaTypeFactoryImpl.INSTANCE));
      when(call.getType())
          .thenReturn(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIME, 3));
      RexNode node = currentTimeEvaluator.evaluate(cx, call);
      verify(cx).getRexBuilder();
      verify(call).getType();
      verify(contextInformation).getQueryStartTime();
      verify(cx).getContextInformation();
      RexLiteral literal = (RexLiteral) node;
      GregorianCalendar calendar = (GregorianCalendar) literal.getValue();
      // current time is in UTC, so we need to add 3 hours to get the current time in GMT-3
      Assert.assertEquals(
          calendar.get(Calendar.HOUR_OF_DAY), (current.get(Calendar.HOUR_OF_DAY) + 3) % 24);
      Assert.assertEquals(calendar.get(Calendar.MINUTE), current.get(Calendar.MINUTE));
      Assert.assertEquals(calendar.get(Calendar.SECOND), current.get(Calendar.SECOND));
    } finally {
      properties.clear("user.timezone");
    }
  }

  @Test
  public void testCurrentTime() {
    long currentTimeMillis = System.currentTimeMillis();
    GregorianCalendar current = new GregorianCalendar();
    current.setTimeInMillis(currentTimeMillis);
    currentTimeEvaluator = CurrentTimeEvaluator.INSTANCE;
    when(contextInformation.getQueryStartTime()).thenReturn(currentTimeMillis);
    when(cx.getContextInformation()).thenReturn(contextInformation);
    when(cx.getRexBuilder()).thenReturn(new DremioRexBuilder(JavaTypeFactoryImpl.INSTANCE));
    when(call.getType())
        .thenReturn(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIME, 3));
    RexNode node = currentTimeEvaluator.evaluate(cx, call);
    verify(cx).getRexBuilder();
    verify(call).getType();
    verify(contextInformation).getQueryStartTime();
    verify(cx).getContextInformation();
    RexLiteral literal = (RexLiteral) node;
    GregorianCalendar calendar = (GregorianCalendar) literal.getValue();
    Assert.assertEquals(calendar.get(Calendar.HOUR_OF_DAY), current.get(Calendar.HOUR_OF_DAY));
    Assert.assertEquals(calendar.get(Calendar.MINUTE), current.get(Calendar.MINUTE));
    Assert.assertEquals(calendar.get(Calendar.SECOND), current.get(Calendar.SECOND));
  }
}
