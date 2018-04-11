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

import { API_URL_V2 } from 'constants/Api';

import * as Actions from './sqlActions.js';

describe('sql actions', () => {
  describe('test constants', () => {
    it('verify existence of CREATE_DATASET', () => {
      expect(Actions.CREATE_DATASET_START).to.not.be.undefined;
      expect(Actions.CREATE_DATASET_SUCCESS).to.not.be.undefined;
      expect(Actions.CREATE_DATASET_FAILURE).to.not.be.undefined;
    });
    it('verify existence of CREATE_DATASET_FROM_EXISTING', () => {
      expect(Actions.CREATE_DATASET_FROM_EXISTING_START).to.not.be.undefined;
      expect(Actions.CREATE_DATASET_FROM_EXISTING_SUCCESS).to.not.be.undefined;
      expect(Actions.CREATE_DATASET_FROM_EXISTING_FAILURE).to.not.be.undefined;
    });
    it('verify existence of MOVE_DATASET', () => {
      expect(Actions.MOVE_DATASET_START).to.not.be.undefined;
      expect(Actions.MOVE_DATASET_SUCCESS).to.not.be.undefined;
      expect(Actions.MOVE_DATASET_FAILURE).to.not.be.undefined;
    });
  });

  describe('test action creators', () => {
    it('test result of calling of function createDataset', () => {
      const cpath = 'bla.bla';
      const location = cpath;
      const dataset = {
        data: { name: ''}
      };
      const expectedResult = {
        [CALL_API]: {
          types: [Actions.CREATE_DATASET_START, Actions.CREATE_DATASET_SUCCESS, Actions.CREATE_DATASET_FAILURE],
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(dataset),
          endpoint: `${API_URL_V2}/dataset${location}`
        }
      };
      const realResult = Actions.createDataset(cpath, dataset)((obj) => obj)[CALL_API];
      expect(realResult).to.eql(expectedResult[CALL_API]);
    });
    it('test result of calling of function createDatasetFromExisting', () => {
      const existingDataset = ['dataset', '1'];
      const fullPathTarget = ['dataset', '1'];
      const dataset = {
        data: { name: ''}
      };
      const datasetConfig = {
        data: { name: ''}
      };
      const realResult =
        Actions.createDatasetFromExisting(existingDataset, fullPathTarget, dataset)((obj) => obj)[CALL_API];
      expect(realResult.types[1].type).to.eql(Actions.CREATE_DATASET_FROM_EXISTING_SUCCESS);
      expect(realResult.method).to.eql('PUT');
      expect(realResult.body).to.eql(JSON.stringify(datasetConfig));
      expect(realResult.endpoint).to.eql(`${API_URL_V2}/dataset/dataset.%221%22/copyFrom/dataset.%221%22`);
    });
    it('test result of calling of function createDatasetFromExisting', () => {
      const fullPathSource = ['bla', 'bla'];
      const fullPathTarget = ['bla', 'bla2'];
      const dataset = {
        data: { name: ''}
      };
      const realResult = Actions.moveDataSet(fullPathSource,  fullPathTarget, dataset)((obj) => obj)[CALL_API];
      expect(realResult.types[1].type).to.eql(Actions.MOVE_DATASET_SUCCESS);
      expect(realResult.method).to.eql('POST');
      expect(realResult.endpoint).to.eql(`${API_URL_V2}/dataset/bla.bla/moveTo/bla.bla2`);
    });
  });
});
