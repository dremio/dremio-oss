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

import com.dremio.service.Service;
import com.dremio.service.script.proto.ScriptProto.Script;
import com.dremio.service.script.proto.ScriptProto.ScriptRequest;

/**
 * Service to interact with Script
 */
public interface ScriptService extends Service {
  /**
   * get list of scripts based on various parameters provided
   *
   * @param offset
   * @param limit
   * @param search
   * @param orderBy
   * @param filter
   * @param createdBy
   * @return
   */
  List<Script> getScripts(int offset,
                          int limit,
                          String search,
                          String orderBy,
                          String filter,
                          String createdBy);

  /**
   * create script
   *
   * @param scriptRequest
   * @return
   * @throws DuplicateScriptNameException
   * @throws MaxScriptsLimitReachedException
   */
  Script createScript(ScriptRequest scriptRequest) throws DuplicateScriptNameException, MaxScriptsLimitReachedException;

  /**
   * update script
   *
   * @param scriptId
   * @param scriptRequest
   * @return
   * @throws ScriptNotFoundException
   * @throws DuplicateScriptNameException
   * @throws ScriptNotAccessible
   */
  Script updateScript(String scriptId, ScriptRequest scriptRequest)
    throws ScriptNotFoundException, DuplicateScriptNameException, ScriptNotAccessible;

  /**
   * get script by id
   *
   * @param scriptId
   * @return
   * @throws ScriptNotFoundException
   * @throws ScriptNotAccessible
   */
  Script getScriptById(String scriptId) throws ScriptNotFoundException, ScriptNotAccessible;

  /**
   * delete script by id
   *
   * @param scriptId
   * @throws ScriptNotFoundException
   * @throws ScriptNotAccessible
   */
  void deleteScriptById(String scriptId) throws ScriptNotFoundException, ScriptNotAccessible;

  /**
   * get count of all scripts created by current user based on search and filter
   *
   * @param search
   * @param filter
   * @param createdBy
   * @return
   */
  Long getCountOfMatchingScripts(String search, String filter, String createdBy);
}
