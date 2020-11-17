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
package com.dremio.exec.planner.sql;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.validate.SqlConformance;

import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.impl.ParserImpl;
import com.dremio.exec.planner.sql.parser.impl.ParserWithCompoundIdConverter;
import com.dremio.sabot.rpc.user.UserSession;

public class ParserConfig implements SqlParser.Config {
  // exists only for old tests that haven't yet been updated.
  public static final String LEGACY_USE_BACKTICKS = SqlUtils.LEGACY_USE_BACKTICKS;

  public static final Quoting QUOTING;

  static {
    QUOTING = "true".equalsIgnoreCase(System.getProperty(LEGACY_USE_BACKTICKS)) ? Quoting.BACK_TICK : Quoting.DOUBLE_QUOTE;
  }

  private final Quoting quoting;
  private final long identifierMaxLength;
  private final boolean supportFullyQualifiedProjections;
  private final boolean withCalciteComplexTypeSupport;

  public ParserConfig(Quoting quoting, final long identifierMaxLength, boolean withCalciteComplexTypeSupport) {
    this(quoting, identifierMaxLength, false, withCalciteComplexTypeSupport);
  }

  public ParserConfig(Quoting quoting, final long identifierMaxLength, final boolean supportFullyQualifiedProjections, boolean withCalciteComplexTypeSupport) {
    this.identifierMaxLength = identifierMaxLength;
    this.quoting = quoting;
    this.supportFullyQualifiedProjections = supportFullyQualifiedProjections;
    this.withCalciteComplexTypeSupport = withCalciteComplexTypeSupport;
  }

  public static ParserConfig newInstance(UserSession session, PlannerSettings settings) {
    Quoting quote = session.getInitialQuoting() != null ? session.getInitialQuoting() : ParserConfig.QUOTING;
    return new ParserConfig(quote, settings.getIdentifierMaxLength(), session.supportFullyQualifiedProjections(), settings.isFullNestedSchemaSupport());
  }

  @Override
  public int identifierMaxLength() {
    return (int) identifierMaxLength;
  }

  @Override
  public Casing quotedCasing() {
    return Casing.UNCHANGED;
  }

  @Override
  public Casing unquotedCasing() {
    return Casing.UNCHANGED;
  }

  public ParserConfig cloneWithSystemDefault(){
    if(quoting == QUOTING && !supportFullyQualifiedProjections){
      return this;
    }
    return new ParserConfig(QUOTING, identifierMaxLength, withCalciteComplexTypeSupport);
  }

  @Override
  public Quoting quoting() {
    return quoting;
  }

  @Override
  public boolean caseSensitive() {
    return false;
  }

  @Override
  public SqlConformance conformance() {
    return DremioSqlConformance.INSTANCE;
  }

  @Override
  public boolean allowBangEqual() {
    return conformance().isBangEqualAllowed();
  }

  @Override
  public SqlParserImplFactory parserFactory() {
    if (supportFullyQualifiedProjections) {
      return ParserImpl.FACTORY;
    }
    return ParserWithCompoundIdConverter.getParserImplFactory(withCalciteComplexTypeSupport);
  }
}
