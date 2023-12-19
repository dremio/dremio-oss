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

package com.dremio.service.script;

import java.util.List;

import com.dremio.service.script.proto.ScriptProto;

/**
 * Dummy class for no operations in script service.
 */
public class NoOpScriptServiceImpl implements ScriptService {

  public static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(NoOpScriptServiceImpl.class);

  public NoOpScriptServiceImpl() {
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting NoOpScriptServiceImpl");
    logger.info("NoOp Script Service is up.");
  }

  @Override
  public List<ScriptProto.Script> getScripts(int offset,
                                             int limit,
                                             String search,
                                             String orderBy,
                                             String filter,
                                             String createdBy) {
    throw new UnsupportedOperationException("getScripts not implemented in NoOpScriptServiceImpl");
  }

  @Override
  public ScriptProto.Script createScript(ScriptProto.ScriptRequest scriptRequest)
    throws DuplicateScriptNameException {
    throw new UnsupportedOperationException("createScript not implemented in NoOpScriptServiceImpl");
  }

  @Override
  public ScriptProto.Script updateScript(String scriptId,
                                         ScriptProto.ScriptRequest scriptRequest)
    throws ScriptNotFoundException, DuplicateScriptNameException, ScriptNotAccessible {
    throw new UnsupportedOperationException("updateScript not implemented in NoOpScriptServiceImpl");
  }

  @Override
  public ScriptProto.Script updateScriptContext(String scriptId, String sessionId) {
    throw new UnsupportedOperationException("updateScriptContext not implemented in NoOpScriptServiceImpl");
}

  @Override
  public ScriptProto.Script getScriptById(String scriptId)
    throws ScriptNotFoundException, ScriptNotAccessible {
    throw new UnsupportedOperationException("getScriptById not implemented in NoOpScriptServiceImpl");
  }

  @Override
  public void deleteScriptById(String scriptId)
    throws ScriptNotFoundException, ScriptNotAccessible {
    throw new UnsupportedOperationException(
      "deleteScriptById not implemented in NoOpScriptServiceImpl");
  }

  @Override
  public Long getCountOfMatchingScripts(String search, String filter, String createdBy) {
    throw new UnsupportedOperationException(
      "getCountOfMatchingScripts not implemented in NoOpScriptServiceImpl");
  }

  @Override
  public void close() throws Exception {

  }
}
