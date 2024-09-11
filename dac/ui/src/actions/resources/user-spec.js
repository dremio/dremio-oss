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
import { expect } from "chai";

import { RSAA } from "redux-api-middleware";

import { APIV2Call } from "@app/core/APICall";
import * as Actions from "./user";

describe("user actions", () => {
  describe("test load user", () => {
    it("with username", () => {
      const apiCall = new APIV2Call().path("user").path("test").uncachable();

      expect(
        Actions.loadUser({ userName: "test" })((obj) => obj)
          [RSAA].endpoint.toString()
          .split("=")[0],
      ).to.equal(apiCall.toString().split("=")[0]);
    });

    it("with url unsafe username", () => {
      const apiCall = new APIV2Call().path("user").path("test?./").uncachable();

      expect(
        Actions.loadUser({ userName: "test?./" })((obj) => obj)
          [RSAA].endpoint.toString()
          .split("=")[0],
      ).to.equal(apiCall.toString().split("=")[0]);
    });
  });
});
