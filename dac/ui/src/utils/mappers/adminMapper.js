/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import uuid from 'uuid';

class AdminMapper {
  mapSourceNodesList(json) {
    return json.map((node) => {
      return {
        id: uuid.v4(),
        name: node.name,
        ip: node.ip,
        cpu: node.cpu,
        memory: node.memory,
        port: node.port,
        status: node.status
      };
    });
  }

  mapGroup(json) {
    return json.groups.map((group) => {
      return {
        id: uuid.v4(),
        name: group.name,
        users: group.users,
        data: group.data
      };
    });
  }
}

const adminMapper = new AdminMapper();

export default adminMapper;
