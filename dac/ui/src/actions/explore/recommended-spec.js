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
import { expect } from 'chai';
import { CALL_API } from 'redux-api-middleware';

import transformModelMapper from 'utils/mappers/ExplorePage/Transform/transformModelMapper';
import { API_URL_V2 } from 'constants/Api';

import * as Actions from './recommended.js';

describe('recommended actions', () => {
  describe('test action creators', () => {

    const transform = Immutable.fromJS({transformType: 'extract'});
    const actionType = 'extract';
    const dataset = Immutable.fromJS({
      apiLinks: {
        self: '/dataset/tmp.UNTITLED/version/12345'
      }
    });

    it('test result of calling of function loadTransformCards', () => {
      const data = {
        cards : []
      };
      const meta = {
        transformType: 'extract',
        method: 'default',
        actionType,
        viewId: Actions.LOAD_TRANSFORM_CARDS_VIEW_ID
      };
      const expectedResult = {
        [CALL_API]: {
          types: [
            { type: Actions.RUN_SELECTION_TRANSFORM_START, meta },
            { type: Actions.RUN_SELECTION_TRANSFORM_SUCCESS, meta },
            { type: Actions.RUN_SELECTION_TRANSFORM_FAILURE, meta }
          ],
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(transformModelMapper.transformExportPostMapper(data, actionType)),
          endpoint: `${API_URL_V2}${dataset.getIn(['apiLinks', 'self'])}/${actionType}`
        }
      };
      const realResult = Actions.loadTransformCards(data, transform, dataset, actionType)((obj) => obj)[CALL_API];
      expect(realResult).to.eql(expectedResult[CALL_API]);
    });

    it('test result of calling of function loadTransformCardPreview', () => {
      const index = 0;
      const data = {
        cards : [],
        rule: {
          type: 'position',
          position: {
            startIndex: {
              value: 0,
              direction: 'FROM_THE_START'
            },
            endIndex: {
              value: 0,
              direction: 'FROM_THE_START'
            }
          }
        }
      };
      const meta = {
        transformType: 'extract',
        method: 'default',
        actionType, index };
      const expectedResult = {
        [CALL_API]: {
          types: [
            { type: Actions.TRANSFORM_CARD_PREVIEW_START, meta },
            { type: Actions.TRANSFORM_CARD_PREVIEW_SUCCESS, meta },
            { type: Actions.TRANSFORM_CARD_PREVIEW_FAILURE, meta }
          ],
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(transformModelMapper.transformDynamicPreviewMapper(data, actionType)),
          endpoint: `${API_URL_V2}${dataset.getIn(['apiLinks', 'self'])}/${actionType}_preview`
        }
      };
      const realResult = Actions.loadTransformCardPreview(data, transform, dataset, actionType, index)(
        (obj) => obj)[CALL_API];
      expect(realResult).to.eql(expectedResult[CALL_API]);
    });

    it('test result of calling of function loadTransformValuesPreview', () => {
      const data = {
        cards : [],
        rule: {
          type: 'position',
          position: {
            startIndex: {
              value: 0,
              direction: 'FROM_THE_START'
            },
            endIndex: {
              value: 0,
              direction: 'FROM_THE_START'
            }
          }
        }
      };
      const meta = {
        transformType: 'extract',
        method: 'default'
      };
      const url = `${API_URL_V2}${dataset.getIn(['apiLinks', 'self'])}/${actionType}_values_preview`;
      const expectedResult = {
        [CALL_API]: {
          types: [
            { type: Actions.RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_START, meta },
            { type: Actions.RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_SUCCESS, meta },
            { type: Actions.RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_FAILURE, meta }
          ],
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(transformModelMapper.mapTransformValuesPreview(data, actionType)),
          endpoint: url
        }
      };
      const realResult = Actions.loadTransformValuesPreview(data, transform, dataset, actionType)(
        (obj) => obj)[CALL_API];
      expect(realResult).to.eql(expectedResult[CALL_API]);
    });
  });
});
