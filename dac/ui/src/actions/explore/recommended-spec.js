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

import transformModelMapper from "utils/mappers/ExplorePage/Transform/transformModelMapper";

import { APIV2Call } from "@app/core/APICall";
import * as Actions from "./recommended.js";

describe("recommended actions", () => {
  describe("test action creators", () => {
    const transform = Immutable.fromJS({ transformType: "extract" });
    const actionType = "extract";
    const dataset = Immutable.fromJS({
      apiLinks: {
        self: "/dataset/tmp.UNTITLED/version/12345",
      },
    });

    it("test result of calling of function loadTransformCards", () => {
      const data = {
        cards: [],
      };
      const meta = {
        transformType: "extract",
        method: "default",
        actionType,
        viewId: Actions.LOAD_TRANSFORM_CARDS_VIEW_ID,
      };

      const apiCall = new APIV2Call().paths(
        "/dataset/tmp.UNTITLED/version/12345/extract"
      );

      const expectedResult = {
        [RSAA]: {
          types: [
            { type: Actions.RUN_SELECTION_TRANSFORM_START, meta },
            { type: Actions.RUN_SELECTION_TRANSFORM_SUCCESS, meta },
            { type: Actions.RUN_SELECTION_TRANSFORM_FAILURE, meta },
          ],
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(
            transformModelMapper.transformExportPostMapper(data, actionType)
          ),
          endpoint: apiCall,
        },
      };
      const realResult = Actions.loadTransformCards(
        data,
        transform,
        dataset,
        actionType
      )((obj) => obj)[RSAA];
      expect(realResult).to.eql(expectedResult[RSAA]);
    });

    it("test result of calling of function loadTransformCardPreview", () => {
      const index = 0;
      const data = {
        cards: [],
        rule: {
          type: "position",
          position: {
            startIndex: {
              value: 0,
              direction: "FROM_THE_START",
            },
            endIndex: {
              value: 0,
              direction: "FROM_THE_START",
            },
          },
        },
      };
      const meta = {
        transformType: "extract",
        method: "default",
        actionType,
        index,
      };

      const apiCall = new APIV2Call().paths(
        "/dataset/tmp.UNTITLED/version/12345/extract_preview"
      );

      const expectedResult = {
        [RSAA]: {
          types: [
            { type: Actions.TRANSFORM_CARD_PREVIEW_START, meta },
            { type: Actions.TRANSFORM_CARD_PREVIEW_SUCCESS, meta },
            { type: Actions.TRANSFORM_CARD_PREVIEW_FAILURE, meta },
          ],
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(
            transformModelMapper.transformDynamicPreviewMapper(data, actionType)
          ),
          endpoint: apiCall,
        },
      };
      const realResult = Actions.loadTransformCardPreview(
        data,
        transform,
        dataset,
        actionType,
        index
      )((obj) => obj)[RSAA];
      expect(realResult).to.eql(expectedResult[RSAA]);
    });

    it("test result of calling of function loadTransformValuesPreview", () => {
      const data = {
        cards: [],
        rule: {
          type: "position",
          position: {
            startIndex: {
              value: 0,
              direction: "FROM_THE_START",
            },
            endIndex: {
              value: 0,
              direction: "FROM_THE_START",
            },
          },
        },
      };
      const meta = {
        transformType: "extract",
        method: "default",
      };

      const apiCall = new APIV2Call().paths(
        "/dataset/tmp.UNTITLED/version/12345/extract_values_preview"
      );

      const expectedResult = {
        [RSAA]: {
          types: [
            {
              type: Actions.RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_START,
              meta,
            },
            {
              type: Actions.RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_SUCCESS,
              meta,
            },
            {
              type: Actions.RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_FAILURE,
              meta,
            },
          ],
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(
            transformModelMapper.mapTransformValuesPreview(data, actionType)
          ),
          endpoint: apiCall,
        },
      };
      const realResult = Actions.loadTransformValuesPreview(
        data,
        transform,
        dataset,
        actionType
      )((obj) => obj)[RSAA];
      expect(realResult).to.eql(expectedResult[RSAA]);
    });
  });
});
