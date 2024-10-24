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

import { cloneDeep } from "lodash";

class SourcesMapper {
  newSource(sourceType, data) {
    const info = cloneDeep(data); // Do not mutate the original values
    delete info.credentials;
    info.config = info.config || {};
    if (info.config.hostList) {
      for (const host of info.config.hostList) {
        delete host.id;
      }
    }
    if (info.config.propertyList) {
      for (const property of info.config.propertyList) {
        delete property.id;
      }
    }
    if (info.config.secretPropertyList) {
      for (const property of info.config.secretPropertyList) {
        delete property.id;
      }
    }
    if (info.config.authenticationTimeoutMillis) {
      info.config.authenticationTimeoutMillis = Number(
        info.config.authenticationTimeoutMillis,
      );
    }
    if (info.config.subpartitionSize) {
      info.config.subpartitionSize = Number(info.config.subpartitionSize);
    }

    // Temp hack to support SECRET auth option in the UI:
    // If user chose MASTER option, wipe out secret field, otherwise next time it will cause the UI to assume SECRET type
    if (
      info.config.authenticationType === "MASTER" &&
      info.config.secretResourceUrl
    ) {
      info.config.secretResourceUrl = "";
    } else if (info.config.authenticationType === "KERBEROS") {
      info.config.useKerberos = true;
      info.config.username = null;
      info.config.password = null;
    }
    return { ...info, type: sourceType };
  }
}

const sourcesMapper = new SourcesMapper();

export default sourcesMapper;
