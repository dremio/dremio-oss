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
package com.dremio.exec.planner.sql.handlers;

import static java.util.Objects.requireNonNull;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.MergeBranchResult;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.ParserUtil;
import com.dremio.exec.planner.sql.parser.SqlMergeBranch;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.plugins.MergeBranchOptions;
import com.dremio.sabot.rpc.user.UserSession;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeResponse;

/** Handler for merging branch. */
public class MergeBranchHandler extends BaseVersionHandler<MergeBranchResult> {
  private final UserSession userSession;

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MergeBranchHandler.class);

  public MergeBranchHandler(QueryContext context) {
    super(context.getCatalog(), context.getOptions());
    this.userSession = requireNonNull(context.getSession());
  }

  @Override
  public List<MergeBranchResult> toResult(String sql, SqlNode sqlNode)
      throws ForemanSetupException {
    checkFeatureEnabled("MERGE BRANCH syntax is not supported.");

    final SqlMergeBranch mergeBranch =
        requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlMergeBranch.class));
    final SqlIdentifier sourceIdentifier = mergeBranch.getSourceName();
    final String sourceName =
        VersionedHandlerUtils.resolveSourceName(
            sourceIdentifier, userSession.getDefaultSchemaPath());

    final VersionedPlugin versionedPlugin = getVersionedPlugin(sourceName);
    final String sourceBranchName = requireNonNull(mergeBranch.getSourceBranchName()).toString();
    final VersionContext sessionVersion = userSession.getSessionVersionForSource(sourceName);

    String targetBranchName;
    if (mergeBranch.getTargetBranchName() != null) {
      targetBranchName = mergeBranch.getTargetBranchName().toString();
    } else if (sessionVersion.isBranch()) {
      targetBranchName = sessionVersion.getValue();
    } else {
      throw UserException.validationError()
          .message("No target branch to merge into.")
          .buildSilently();
    }

    final boolean isDryRun = mergeBranch.getIsDryRun().booleanValue();
    final MergeBehavior defaultMergeBehavior = mergeBranch.getDefaultMergeBehavior();
    if (defaultMergeBehavior != null) {
      // we should check if our ff is turned on in this case, because if default merge behavior is
      // not null, the sql contains the syntax, ON CONFLICT. In this case, we should ensure the user
      // turned on the feature flag.
      requireFeatureFlag(
          "Specifying merge behavior is not supported.",
          ExecConstants.ENABLE_MERGE_BRANCH_BEHAVIOR);
    }
    final MergeBehavior mergeBehavior1 = mergeBranch.getMergeBehavior1();
    final MergeBehavior mergeBehavior2 = mergeBranch.getMergeBehavior2();
    Map<ContentKey, MergeBehavior> mergeBehaviorMap = new HashMap<>();

    validateMergeBehaviorMap(mergeBranch);
    setMergeBehaviorMap(
        sourceName, mergeBehaviorMap, mergeBehavior1, mergeBranch.getExceptContentList1());
    setMergeBehaviorMap(
        sourceName, mergeBehaviorMap, mergeBehavior2, mergeBranch.getExceptContentList2());

    final MergeBranchOptions mergeBranchOptions =
        MergeBranchOptions.builder()
            .setDryRun(isDryRun)
            .setDefaultMergeBehavior(defaultMergeBehavior)
            .setMergeBehaviorMap(mergeBehaviorMap)
            .build();

    List<MergeBranchResult> result = new ArrayList<>();
    try {
      MergeResponse mergeResponse =
          versionedPlugin.mergeBranch(sourceBranchName, targetBranchName, mergeBranchOptions);

      result.add(
          generateMergeBranchMessage(
              sourceBranchName,
              targetBranchName,
              Optional.ofNullable(mergeResponse.getExpectedHash()),
              mergeResponse.wasSuccessful(),
              isDryRun));

      if (!mergeResponse.wasSuccessful()) {
        mergeResponse.getDetails().stream()
            .map(MergeResponse.ContentKeyDetails::getConflict)
            .filter(Objects::nonNull)
            .forEach(
                conflict ->
                    result.add(
                        new MergeBranchResult(
                            conflict.message(),
                            String.valueOf(conflict.key()),
                            conflict.conflictType().toString())));
      }
    } catch (ReferenceNotFoundException e) {
      result.add(
          generateMergeBranchMessage(
              sourceBranchName, targetBranchName, Optional.empty(), false, isDryRun));
      result.add(
          new MergeBranchResult(
              String.format(
                  "Merge branch %s into %s failed due to: %s",
                  sourceBranchName, targetBranchName, e),
              null,
              HandlerUtils.SQL_STATUS_FAILURE));
    }
    return result;
  }

  private MergeBranchResult generateMergeBranchMessage(
      String sourceBranchName,
      String targetBranchName,
      Optional<String> hash,
      boolean wasSuccessful,
      boolean dryRun) {
    String message;
    if (dryRun) {
      message =
          String.format(
              "Branch %s %s merged into %s",
              sourceBranchName, wasSuccessful ? "can be" : "cannot be", targetBranchName);
    } else {
      if (wasSuccessful) {
        message =
            String.format(
                "Branch %s has been merged into %s at %s",
                sourceBranchName,
                targetBranchName,
                hash.orElseGet(
                    () -> {
                      logger.error("Unknown commit hash while merge branch was successful.");
                      return "[unknown hash]";
                    }));

      } else {
        message =
            String.format("Failed to merge branch %s into %s", sourceBranchName, targetBranchName);
      }
    }
    return new MergeBranchResult(message, null, HandlerUtils.statusCode(wasSuccessful));
  }

  void validateMergeBehaviorMap(SqlMergeBranch mergeBranch) {
    final MergeBehavior defaultMergeBehavior = mergeBranch.getDefaultMergeBehavior();
    final MergeBehavior mergeBehavior1 = mergeBranch.getMergeBehavior1();
    final MergeBehavior mergeBehavior2 = mergeBranch.getMergeBehavior2();

    if (defaultMergeBehavior == null) {
      return;
    }

    EnumSet<MergeBehavior> mergeBehaviorEnumSet = EnumSet.noneOf(MergeBehavior.class);
    Stream.of(defaultMergeBehavior, mergeBehavior1, mergeBehavior2)
        .filter(Objects::nonNull)
        .forEach(
            mergeBehavior -> {
              if (!mergeBehaviorEnumSet.add(mergeBehavior)) {
                throw UserException.validationError()
                    .message(
                        String.format(
                            "Merge behavior must be distinct. %s found more than once.",
                            ParserUtil.mergeBehaviorToSql(mergeBehavior)))
                    .buildSilently();
              }
            });
  }

  private void setMergeBehaviorMap(
      String sourceName,
      Map<ContentKey, MergeBehavior> mergeBehaviorMap,
      MergeBehavior mergeBehavior,
      SqlNodeList keyList) {

    if (mergeBehavior == null) {
      return;
    }

    keyList.forEach(
        keyNode -> {
          SqlIdentifier key = (SqlIdentifier) keyNode;
          List<String> nameElements = key.names;

          if (!sourceName.equals(nameElements.get(0))) {
            throw UserException.validationError()
                .message(
                    String.format(
                        "Content must be from source [%s], but [%s] does not.",
                        sourceName, keyNode))
                .buildSilently();
          }

          // In nessie, we do not need the source name in the key. So removing it.
          ContentKey contentKey = ContentKey.of(nameElements.subList(1, nameElements.size()));
          if (mergeBehaviorMap.containsKey(contentKey)) {
            throw UserException.validationError()
                .message("Each key can only be used once in an EXCEPT clause.")
                .buildSilently();
          }
          mergeBehaviorMap.put(contentKey, mergeBehavior);
        });
  }

  @Override
  public Class<MergeBranchResult> getResultType() {
    return MergeBranchResult.class;
  }
}
