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

import {
  InlineResponse200,
  Privilege,
  GranteeType,
} from "../ArcticCatalogGrants.types";

export const sampleArcticCatalogGrantsResponse: InlineResponse200 = {
  tag: "34c316a9-9c4e-4356-9e90-3af3c1769707",
  availablePrivileges: [
    Privilege.USAGE,
    Privilege.COMMIT,
    Privilege.CREATEBRANCH,
    Privilege.CREATETAG,
    Privilege.WRITESETTINGS,
    Privilege.READSETTINGS,
    Privilege.MANAGEGRANTS,
  ],
  owner: "eb012c61-4418-4e8b-a6ab-e53aebbf8646",
  grants: [
    {
      granteeId: "b9ce6f0e-0eac-485d-ae4a-e700e2d9dd2d",
      privileges: [Privilege.USAGE, Privilege.COMMIT],
      name: "user1@dremio.com",
      granteeType: GranteeType.USER,
    },
    {
      granteeId: "641d551d-5181-4f13-ad20-a27d88fb1512",
      privileges: [Privilege.USAGE, Privilege.MANAGEGRANTS],
      name: "Catalog Administrators",
      granteeType: GranteeType.ROLE,
    },
  ],
};
