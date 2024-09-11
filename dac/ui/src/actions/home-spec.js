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
import Immutable from "immutable";
import { RSAA } from "redux-api-middleware";

import { APIV2Call } from "@app/core/APICall";
import * as Actions from "./home";

describe("home actions", () => {
  describe("test convert to folder", () => {
    it("to passthrough url", () => {
      const apiCall = new APIV2Call().fullpath(
        "/folder/foo/bar/baz?test=ad./ad",
      );

      const folder = Immutable.fromJS({
        links: {
          format: "/folder/foo/bar/baz?test=ad./ad",
        },
      });

      expect(
        Actions.convertFolderToDataset({ folder })((obj) => obj)[RSAA].endpoint,
      ).to.eql(apiCall);
    });
  });

  describe("test remove folder format", () => {
    it("to passthrough url", () => {
      const apiCall = new APIV2Call().fullpath(
        "/folder/foo/bar/baz?test=ad./ad",
      );

      const dataset = Immutable.fromJS({
        links: {
          delete_format: "/folder/foo/bar/baz?test=ad./ad",
        },
      });

      expect(
        Actions.convertDatasetToFolder(dataset)((obj) => obj)[RSAA].endpoint,
      ).to.eql(apiCall);
    });
  });
});
