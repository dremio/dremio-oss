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

import {CONVERT_DATASET_TO_FOLDER_SUCCESS} from 'actions/home';

import folder from './folder';

describe('folder', () => {

  const initialState =  Immutable.fromJS({
    folder: {
      fooId: {
        queryable: true
      }
    }
  });

  it('returns unaltered state by default', () => {
    const result = folder(initialState, {type: 'bla'});
    expect(result).to.equal(initialState);
  });

  it('sets queryable to false', () => {
    const result = folder(initialState, {
      type: CONVERT_DATASET_TO_FOLDER_SUCCESS,
      meta: {folderId: 'fooId'}
    });
    expect(result.getIn(['folder', 'fooId', 'queryable'])).to.be.false;
  });
});
