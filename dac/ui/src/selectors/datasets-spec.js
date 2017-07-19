/*
 * Copyright (C) 2017 Dremio Corporation
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
import Immutable from 'immutable';

import { getDatasetConfigs } from './datasets';

import entities from './mocks/entities.json';
import spaceList from './mocks/spaceList.json';
import sourceList from './mocks/sourceList.json';

const props = {
  routeParams: {resourceId: '2222'},
  pageType: 'space'
};
const state = {
  resources: {
    entities: Immutable.fromJS(entities),
    sourceList: Immutable.fromJS(sourceList),
    spaceList: Immutable.fromJS(spaceList)
  }
};
// TODO should be refactored and updated, here used old mocked data
describe('datasets selectors', () => {
  describe('getDatasetConfigs selector', () => {
    it('should return 1 file', () => {
      const res = getDatasetConfigs(state, props).toList().toJS();
      res.forEach(item => delete item.id);
      expect(res.length).to.eql(0);
      expect(res).to.eql([]);
    });
  });
});
