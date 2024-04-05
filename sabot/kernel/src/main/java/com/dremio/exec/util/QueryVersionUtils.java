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
package com.dremio.exec.util;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotQueryContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.sabot.rpc.user.ChangeTrackingUserSession;
import com.dremio.sabot.rpc.user.UserSession;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

/**
 * This class is used to validate view sql. It is used to validate if any versioned table in the
 * view SQL has an undefined version context. Example : Current context is set to [aCatalog1, ETL
 * branch] create view V1 as select * from aCatalog1.T1 JOIN aCatalog2.T2 on TRUE; => This will FAIL
 * since T2 is not fully qualified with a version context.
 *
 * <p>create view V1 as select * from aCatalog1.T1 JOIN aCatalog2.T2 AT BRANCH dev on TRUE; => This
 * will PASS - T2 is fully qualified with a version context.
 */
public class QueryVersionUtils {

  public static ConvertedRelNode checkForUnspecifiedVersionsAndReturnRelNode(
      final SqlNode sqlNode,
      final List<String> pathContext,
      final SabotQueryContext sabotContext,
      final Map<String, VersionContext> sourceVersionMapping,
      Optional<UserSession> inputUserSession)
      throws Exception {
    // Create a new QueryContext with the flag errorOnUnspecifiedVersion set to true.
    try (final QueryContext queryContext =
        queryContextForVersionValidation(
            sabotContext, pathContext, sourceVersionMapping, inputUserSession)) {
      final SqlConverter convertor = QueryVersionUtils.getNewConverter(queryContext);
      queryContext.setGroupResourceInformation(sabotContext.getClusterResourceInformation());

      final SqlHandlerConfig config =
          new SqlHandlerConfig(queryContext, convertor, AttemptObservers.of(), null);
      // The  table validation step in validateAndConvert will throw an exception if any table in
      // the query has an unspecified VersionContext.
      return SqlToRelTransformer.validateAndConvert(config, sqlNode);
    } catch (ValidationException | RelConversionException e) {
      // Calcite exception could wrap exceptions in layers. Find the root cause to get the original
      // error message.
      Throwable rootCause = e;
      while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
        rootCause = rootCause.getCause();
      }
      throw UserException.validationError(e)
          .message("Validation of view sql failed. %s ", rootCause.getMessage())
          .buildSilently();
    }
  }

  public static QueryContext queryContextForVersionValidation(
      final SabotQueryContext sabotContext,
      final List<String> pathContext,
      final Map<String, VersionContext> sourceVersionMapping,
      Optional<UserSession> inputUserSession) {
    final UserBitShared.QueryId queryId = new AttemptId().toQueryId();
    final UserSession userSession =
        userSessionForVersionValidation(
            sabotContext.getOptionValidatorListing(),
            sabotContext.getOptionManager(),
            pathContext,
            sourceVersionMapping,
            inputUserSession);

    return new QueryContext(userSession, sabotContext, queryId);
  }

  private static UserSession userSessionForVersionValidation(
      final OptionValidatorListing optionValidatorListing,
      final OptionManager optionManager,
      final List<String> pathContext,
      final Map<String, VersionContext> sourceVersionMapping,
      Optional<UserSession> userSession) {
    if (userSession.isPresent()) {
      if (userSession.get() instanceof ChangeTrackingUserSession) {
        return ChangeTrackingUserSession.Builder.newBuilderWithCopy(
                (ChangeTrackingUserSession) userSession.get())
            .withDefaultSchema(pathContext)
            .withSourceVersionMapping(sourceVersionMapping)
            .withErrorOnUnspecifiedVersion(true)
            .build();
      } else {
        return UserSession.Builder.newBuilderWithCopy(userSession.get())
            .withDefaultSchema(pathContext)
            .withSourceVersionMapping(sourceVersionMapping)
            .withErrorOnUnspecifiedVersion(true)
            .build();
      }
    } else {
      return UserSession.Builder.newBuilder()
          .withSessionOptionManager(
              new SessionOptionManagerImpl(optionValidatorListing), optionManager)
          .withDefaultSchema(pathContext)
          .withSourceVersionMapping(sourceVersionMapping)
          .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
          .withCredentials(
              UserBitShared.UserCredentials.newBuilder().setUserName(SYSTEM_USERNAME).build())
          .withCheckMetadataValidity(false)
          .withNeverPromote(false)
          .withErrorOnUnspecifiedVersion(true)
          .build();
    }
  }

  public static SqlConverter getNewConverter(final QueryContext context) {
    return new SqlConverter(
        context.getPlannerSettings(),
        context.getOperatorTable(),
        context,
        MaterializationDescriptorProvider.EMPTY,
        context.getFunctionRegistry(),
        context.getSession(),
        AbstractAttemptObserver.NOOP,
        context.getSubstitutionProviderFactory(),
        context.getConfig(),
        context.getScanResult(),
        context.getRelMetadataQuerySupplier());
  }
}
