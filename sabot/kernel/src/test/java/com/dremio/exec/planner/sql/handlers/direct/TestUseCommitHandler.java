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

import static com.dremio.exec.ExecConstants.ENABLE_USE_VERSION_SYNTAX;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.parser.SqlUseCommit;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;

public class TestUseCommitHandler {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void toResult_sets_UserSession() throws ForemanSetupException {
    String commitHash = RandomStringUtils.randomAlphanumeric(10);

    UserSession userSession = UserSession.Builder.newBuilder().build();

    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);

    UseCommitHandler useCommitHandler = new UseCommitHandler(userSession, optionManager);
    final SqlUseCommit useCommit = new SqlUseCommit(
      SqlParserPos.ZERO,
      new SqlIdentifier(commitHash, SqlParserPos.ZERO));

    List<SimpleCommandResult> result = useCommitHandler.toResult("", useCommit);
    assertFalse(result.isEmpty());
    assertTrue(result.get(0).ok);
    assertTrue(commitHash.equals(userSession.getVersionContext().get().getCommitHash()));
  }

  @Test
  public void optionDisabled_throws() throws ForemanSetupException {
    String commitHash = RandomStringUtils.randomAlphanumeric(10);

    UserSession userSession = mock(UserSession.class);

    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(false);

    UseCommitHandler useCommitHandler = new UseCommitHandler(userSession, optionManager);
    final SqlUseCommit useCommit = new SqlUseCommit(
      SqlParserPos.ZERO,
      new SqlIdentifier(commitHash, SqlParserPos.ZERO));

    thrown.expect(UserException.class);
    useCommitHandler.toResult("", useCommit);
  }
}
