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
  type GrantsResponse,
  Privilege,
  GranteeType,
  GrantType,
} from "../Grants.types";

export const sampleGrantsResponse = {
  availablePrivileges: [
    Privilege.CREATECLOUD,
    Privilege.CREATEUSER,
    Privilege.CREATEROLE,
    Privilege.CREATEPROJECT,
    Privilege.CONFIGURE_SECURITY,
    Privilege.MANAGEGRANTS,
    Privilege.CREATECATALOG,
  ],
  owner: "eb012c61-4418-4e8b-a6ab-e53aebbf8646",
  grants: [
    {
      name: "admin@dremio.com",
      granteeType: GranteeType.USER,
      grantType: GrantType.ORG,
      granteeId: "eb012c61-4418-4e8b-a6ab-e53aebbf8646",
      privileges: [Privilege.CREATEUSER, Privilege.CREATEROLE],
      tag: "934712c9-9cab-4045-ae4f-ca64be080bd0",
    },
    {
      name: "user@dremio.com",
      granteeType: GranteeType.USER,
      grantType: GrantType.ORG,
      granteeId: "641d551d-5181-4f13-ad20-a27d88fb1512",
      privileges: [Privilege.CREATECATALOG],
      tag: "134712a9-9cbb-4244-ab4e-ca62be080bd0",
    },
  ],
  totalResults: 2,
} as GrantsResponse;

export const sampleGrantsResponsePUT = {
  availablePrivileges: [
    Privilege.CREATECLOUD,
    Privilege.CREATEUSER,
    Privilege.CREATEROLE,
    Privilege.CREATEPROJECT,
    Privilege.CONFIGURE_SECURITY,
    Privilege.MANAGEGRANTS,
    Privilege.CREATECATALOG,
  ],
  owner: "eb012c61-4418-4e8b-a6ab-e53aebbf8646",
  grants: [
    {
      name: "admin@dremio.com",
      granteeType: GranteeType.USER,
      grantType: GrantType.ORG,
      granteeId: "eb012c61-4418-4e8b-a6ab-e53aebbf8646",
      privileges: [
        Privilege.CREATEUSER,
        Privilege.CREATEROLE,
        Privilege.CREATECLOUD,
      ],
      tag: "abc712c9-9cab-4045-ae4f-ca64be080bd0",
    },
    {
      name: "user@dremio.com",
      granteeType: GranteeType.USER,
      grantType: GrantType.ORG,
      granteeId: "641d551d-5181-4f13-ad20-a27d88fb1512",
      privileges: [Privilege.CREATECATALOG],
      tag: "556712a9-9cbb-4244-ab4e-ca62be080bd0",
    },
  ],
  totalResults: 2,
} as GrantsResponse;
