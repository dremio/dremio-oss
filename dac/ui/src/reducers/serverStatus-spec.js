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
import moment from 'moment';

import serverStatus from 'reducers/serverStatus';
import {
  SCHEDULE_CHECK_SERVER_STATUS,
  CHECK_SERVER_STATUS_START,
  CHECK_SERVER_STATUS_SUCCESS
} from 'actions/serverStatus';

describe('serverStatus reducer', () => {

  const initialState =  Immutable.fromJS({
    status: 'ok'
  });

  it('returns unaltered state by default', () => {
    const result = serverStatus(initialState, {type: 'bla'});
    expect(result).to.equal(initialState);
  });

  it('should reset on schedule check', () => {
    const result = serverStatus(initialState.set('foo', 'bar'), {
      type: SCHEDULE_CHECK_SERVER_STATUS
    });
    expect(result.get('foo')).to.be.undefined;
  });

  it('should set server status on success', () => {
    const result = serverStatus(initialState, {
      type: CHECK_SERVER_STATUS_SUCCESS,
      payload: 'foo'
    });
    expect(result.get('status')).to.eql('foo');
  });

  it('should save moment and delay on check start', () => {
    const result = serverStatus(initialState, {
      type: CHECK_SERVER_STATUS_START,
      meta: {delay: 100}
    });
    expect(result.get('delay')).to.eql(100);
    expect(result.get('lastCheckMoment') instanceof moment).to.be.true;
  });
});
