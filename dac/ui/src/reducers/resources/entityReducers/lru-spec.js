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
import Immutable from 'immutable';

import { ACCESS_ENTITY } from 'actions/resources/lru';
import lruReducer from './lru';

const initialState = Immutable.Map({
  someEntity: Immutable.OrderedMap([['id1', 1], ['id2', 2], ['id3', 3]])
});

describe('lruReducer', () => {

  it('returns unaltered state by default', () => {
    const result = lruReducer(initialState, {type: 'bla'});
    expect(result).to.equal(initialState);
  });

  describe('ACCESS_ENTITY', () => {
    it('should move accessed id to last', () => {
      const result = lruReducer(initialState, {type: ACCESS_ENTITY, meta: {entityType: 'someEntity', entityId: 'id1'}});
      expect(result.get('someEntity').last()).to.equal(1);
    });
  });
});
