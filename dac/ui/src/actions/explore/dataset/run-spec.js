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
import { RSAA } from 'redux-api-middleware';
import { API_URL_V2 } from 'constants/Api';

import * as Actions from './run';

const viewId = 'viewId';

const dataset = Immutable.fromJS({
  datasetVersion: '123',
  tipVersion: 'tip123',
  apiLinks: {
    self: '/dataset/foo/version/123'
  }
});

describe('dataset/run', () => {
  describe('runDataset', () => {
    it('should return RSAA', () => {
      const result = Actions.runDataset(dataset, viewId)(obj => obj)[RSAA];
      expect(result.types[0].meta).to.eql({dataset, viewId});
      expect(result.method).to.eql('GET');
      expect(result.endpoint).to.eql(`${API_URL_V2}${dataset.getIn(['apiLinks', 'self'])}/run?tipVersion=tip123`);
    });

  });

  describe('transformAndRunDataset', () => {
    it('should return RSAA', () => {
      const sql = 'select * from foo';
      const transformData = {type: 'updateSQL', sql};
      const result = Actions.transformAndRunDataset(dataset, transformData, viewId)(obj => obj)[RSAA];
      expect(result.types[0].meta).to.eql({entity: dataset, viewId});
      expect(result.method).to.eql('POST');
      expect(result.body).to.eql(JSON.stringify(transformData));
      expect(result.endpoint).to.startWith(
        `${API_URL_V2}${dataset.getIn(['apiLinks', 'self'])}/transformAndRun?newVersion=`
      );
    });
  });

});
