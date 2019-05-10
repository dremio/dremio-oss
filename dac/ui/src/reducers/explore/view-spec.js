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
import { RUN_TABLE_TRANSFORM_SUCCESS } from 'actions/explore/dataset/common';
import { LOAD_EXPLORE_ENTITIES_SUCCESS } from 'actions/explore/dataset/get';
import { RUN_DATASET_SUCCESS } from 'actions/explore/dataset/run';
import reducer from './view';

describe('explore/view reducer', () => {

  const commonState = Immutable.fromJS({
    isPreviewMode: false
  });

  it('Preview mode is enabled by default', () => {
    expect(reducer(undefined, {}).isPreviewMode).to.equal(true);
  });

  it('Preview mode is enabled, when preview is fetched', () => {
    expect(reducer(commonState, { type: LOAD_EXPLORE_ENTITIES_SUCCESS }).isPreviewMode).to.equal(true);
  });

  it('Preview mode is enabled, when query is altered', () => {
    expect(reducer(commonState, { type: RUN_TABLE_TRANSFORM_SUCCESS }).isPreviewMode).to.equal(true);
  });

  it('Preview mode is disabled, when user run a query', () => {
    expect(reducer(Immutable.fromJS({
      isPreviewMode: true
    }), { type: RUN_DATASET_SUCCESS }).isPreviewMode).to.equal(false);
  });
});
