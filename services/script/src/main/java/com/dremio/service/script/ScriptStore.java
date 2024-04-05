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

import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.service.Service;
import com.dremio.service.script.proto.ScriptProto.Script;
import java.util.Optional;

/** Store for Script */
public interface ScriptStore extends Service {
  /**
   * get script by script id.
   *
   * @param scriptId
   * @return
   */
  Optional<Script> get(String scriptId) throws ScriptNotFoundException;

  /**
   * get script by name
   *
   * @param name
   * @return
   * @throws ScriptNotFoundException
   */
  Optional<Script> getByName(String name) throws ScriptNotFoundException;

  /**
   * @param scriptId
   * @param script
   * @return
   * @throws ScriptNotFoundException
   */
  Script create(String scriptId, Script script);

  /**
   * create/update script using scriptId
   *
   * @param scriptId
   * @return
   */
  Script update(String scriptId, Script script) throws ScriptNotFoundException;

  /**
   * delete a script by scriptId
   *
   * @param scriptId
   */
  void delete(String scriptId) throws ScriptNotFoundException;

  /**
   * @param condition
   * @return
   */
  Iterable<Document<String, Script>> getAllByCondition(FindByCondition condition);

  /**
   * @param condition
   * @return
   */
  long getCountByCondition(SearchTypes.SearchQuery condition);
}
