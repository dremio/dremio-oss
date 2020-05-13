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
import Immutable from 'immutable';
import {NEW_UNTITLED_FAILURE, NEW_UNTITLED_SUCCESS} from '@app/actions/explore/dataset/new';
import {LOAD_EXPLORE_ENTITIES_FAILURE, LOAD_EXPLORE_ENTITIES_SUCCESS} from '@app/actions/explore/dataset/get';
import data from './datasetSummary';

describe('datasetSummary reducer', () => {

  const initialState = Immutable.fromJS({
    datasetSummary: null
  });

  it('returns unaltered state by default', () => {
    const result = data(initialState, {type: 'foo'});
    expect(result).to.equal(initialState);
  });

  it('should set datasetSummary to null for new success', () => {
    const result = data(initialState, {type: NEW_UNTITLED_SUCCESS});
    expect(result.get('datasetSummary')).to.be.null;
  });
  it('should set datasetSummary to null for load success', () => {
    const result = data(initialState, {type: LOAD_EXPLORE_ENTITIES_SUCCESS});
    expect(result.get('datasetSummary')).to.be.null;
  });
  it('should set datasetSummary to null for load failure w/o payload prop', () => {
    const result = data(initialState, {type: NEW_UNTITLED_FAILURE, payload: {response: {}}});
    expect(result.get('datasetSummary')).to.be.null;
  });
  it('should set datasetSummary to null for load failure w/o payload prop', () => {
    const result = data(initialState, {type: LOAD_EXPLORE_ENTITIES_FAILURE, payload: {response: {details: {}}}});
    expect(result.get('datasetSummary')).to.be.null;
  });

  it('should set datasetSummary if new failure and payload prop', () => {
    const result = data(initialState, {
      type: NEW_UNTITLED_FAILURE,
      payload: {response: {details: {datasetSummary: 'foo'}}}
    });
    expect(result.get('datasetSummary')).to.equal('foo');
  });
  it('should set datasetSummary if load failure and payload prop', () => {
    const result = data(initialState, {
      type: LOAD_EXPLORE_ENTITIES_FAILURE,
      payload: {response: {details: {datasetSummary: 'foo'}}}
    });
    expect(result.get('datasetSummary')).to.equal('foo');
  });
});
