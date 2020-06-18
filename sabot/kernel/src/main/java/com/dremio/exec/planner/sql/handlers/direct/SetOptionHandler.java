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
package com.dremio.exec.planner.sql.handlers.direct;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.NlsString;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.EagerCachingOptionManager;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Converts a {@link SqlNode} representing "ALTER .. SET option = value" and "ALTER ... RESET ..." statements to a
 * {@link PhysicalPlan}. See {@link SqlSetOption}. These statements have side effects i.e. the options within the
 * system context or the session context are modified. The resulting {@link DirectPlan} returns to the client a string
 * that is the name of the option that was updated.
 */
public class SetOptionHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SetOptionHandler.class);

  private final QueryContext context;
  private final UserSession session;

  public SetOptionHandler(QueryContext context) {
    super();
    this.context = context;
    this.session = context.getSession();
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws ValidationException, RelConversionException, IOException,
      ForemanSetupException {
    final OptionValidatorListing optionValidatorProvider = session.getOptions().getOptionValidatorListing();
    final QueryOptionManager queryOptionManager = new QueryOptionManager(optionValidatorProvider);
    final OptionManager options = OptionManagerWrapper.Builder.newBuilder()
      .withOptionManager(new DefaultOptionManager(optionValidatorProvider))
      .withOptionManager(new EagerCachingOptionManager(context.getSystemOptionManager()))
      .withOptionManager(session.getSessionOptionManager())
      .withOptionManager(queryOptionManager)
      .build();
    final SqlSetOption option = SqlNodeUtil.unwrap(sqlNode, SqlSetOption.class);
    final String name = option.getName().toString();

    final SqlNode value = option.getValue();
    if (value != null && !(value instanceof SqlLiteral)) {
      throw UserException.validationError()
          .message("Dremio does not support assigning non-literal values in SET statements.")
          .build(logger);
    }

    final String scope = option.getScope();
    final OptionValue.OptionType type;
    if (scope == null) { // No scope mentioned assumed SESSION
      type = OptionType.SESSION;
    } else {
      switch (scope.toLowerCase()) {
        case "session":
          type = OptionType.SESSION;
          break;
        case "system":
          type = OptionType.SYSTEM;
          break;
        default:
          throw UserException.validationError()
              .message("Invalid OPTION scope %s. Scope must be SESSION or SYSTEM.", scope)
              .build(logger);
      }
    }

    // Currently, we convert multi-part identifier to a string.
    if (value != null) { // SET option
      final OptionValue optionValue = createOptionValue(name, type, (SqlLiteral) value);
      options.setOption(optionValue);
    } else { // RESET option
      if ("ALL".equalsIgnoreCase(name)) {
        session.setDefaultSchemaPath(null);
        options.deleteAllOptions(type);
      } else {
        options.deleteOption(name, type);
      }
    }


    return Collections.singletonList(SimpleCommandResult.successful("%s updated.", name));
  }

  private static OptionValue createOptionValue(final String name, final OptionValue.OptionType type,
                                               final SqlLiteral literal) {
    final Object object = literal.getValue();
    final SqlTypeName typeName = literal.getTypeName();
    switch (typeName) {
    case DECIMAL: {
      final BigDecimal bigDecimal = (BigDecimal) object;
      if (bigDecimal.scale() == 0) {
        return OptionValue.createLong(type, name, bigDecimal.longValue());
      } else {
        return OptionValue.createDouble(type, name, bigDecimal.doubleValue());
      }
    }

    case DOUBLE:
    case FLOAT:
      return OptionValue.createDouble(type, name, ((BigDecimal) object).doubleValue());

    case SMALLINT:
    case TINYINT:
    case BIGINT:
    case INTEGER:
      return OptionValue.createLong(type, name, ((BigDecimal) object).longValue());

    case VARBINARY:
    case VARCHAR:
    case CHAR:
      return OptionValue.createString(type, name, ((NlsString) object).getValue());

    case BOOLEAN:
      return OptionValue.createBoolean(type, name, (Boolean) object);

    default:
      throw UserException.validationError()
        .message("Dremio doesn't support assigning literals of type %s in SET statements.", typeName)
        .build(logger);
    }
  }
}
