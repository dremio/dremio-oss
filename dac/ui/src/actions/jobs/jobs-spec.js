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
import localStorageUtils from '@app/utils/storageUtils/localStorageUtils.js';
import { API_URL_V2 } from '@app/constants/Api.js';

import * as Actions from './jobs.js';

describe('graph actions', () => {
  describe('test constants', () => {
    it('verify existence of LOAD_EXPLORE_GRAPH', () => {
      expect(Actions.FILTER_JOBS_REQUEST).to.not.be.undefined;
      expect(Actions.FILTER_JOBS_SUCCESS).to.not.be.undefined;
      expect(Actions.FILTER_JOBS_FAILURE).to.not.be.undefined;
    });
  });
});

describe('showJobProfile', () => {
  it('adds auth token in url', () => {
    const profileUrl = '/profiles/234ec562-1fe6-7ee1-b3c4-a74e57720300?attempt=0';
    const thunk = Actions.showJobProfile(profileUrl);
    const dispatch = sinon.stub();
    sinon.stub(localStorageUtils, 'getAuthToken').returns('test_token');
    const getState = () => ({ routing: {} });
    thunk(dispatch, getState);

    expect(dispatch).to.be.calledOnce;
    const args = dispatch.getCall(0).args;
    // extract state parameter for react-router-redux's push action
    expect(args[0].payload.args[0]).to.be.eql({
      state: {
        modal:'JobProfileModal',
        profileUrl:`${API_URL_V2}/profiles/234ec562-1fe6-7ee1-b3c4-a74e57720300?attempt=0&Authorization=test_token`
      }
    }, 'auth token must be added to a profile url');

    localStorageUtils.getAuthToken.restore();
  });
});
