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
import { ApiError } from 'redux-api-middleware/lib/errors';

import { CALL_MOCK_API, mockApiMiddleware } from '.';

describe('mockApiMiddleware', () => {

  let store;
  let next;
  let action;
  let clock;

  beforeEach(() => {
    store = {dispatch: sinon.spy(), getState: sinon.spy()};
    next = sinon.stub().returnsArg(0);
    action = {
      [CALL_MOCK_API]: {
        types: ['REQUEST', 'SUCCESS', 'FAILURE'],
        method: 'GET',
        endpoint: '/',
        mockResponse: {
          responseJSON: {
            response: 'foo'
          }
        }
      }
    };
    clock = sinon.useFakeTimers();
  });

  afterEach(() => {
    clock.restore();
  });

  it('ignores non mock api actions', () => {
    mockApiMiddleware(store)(next)({type: 'foo'});
    expect(next.args[0][0]).to.eql({type: 'foo'});
    expect(store.dispatch.notCalled).to.be.true;
  });

  it('dispatches request, and success actions when ok is undefined', () => {
    const promise = mockApiMiddleware(store)(next)(action);
    expect(store.dispatch.args[0][0]).to.eql({type: 'REQUEST'});
    const ret = expect(promise).to.eventually.eql({
      type: 'SUCCESS',
      meta: undefined,
      payload: action[CALL_MOCK_API].mockResponse.responseJSON
    });
    clock.tick(1000);
    return ret;
  });

  it('dispatches request, and success actions when ok is true', () => {
    action[CALL_MOCK_API].mockResponse.ok = true;
    const promise = mockApiMiddleware(store)(next)(action);

    expect(store.dispatch.args[0][0]).to.eql({type: 'REQUEST'});

    const ret = expect(promise).to.eventually.eql({
      type: 'SUCCESS',
      meta: undefined,
      payload: action[CALL_MOCK_API].mockResponse.responseJSON
    });

    clock.tick(1000);
    return ret;
  });

  it('dispatches request, and failure actions when ok is false', () => {
    action[CALL_MOCK_API].mockResponse = {
      ...action[CALL_MOCK_API].mockResponse,
      ok: false,
      status: 500,
      statusText: 'Internal Server Error'
    };
    const promise = mockApiMiddleware(store)(next)(action);

    expect(store.dispatch.args[0][0]).to.eql({type: 'REQUEST'});

    const ret = expect(promise).to.eventually.eql({
      type: 'FAILURE',
      meta: undefined,
      payload: new ApiError(500, 'Internal Server Error', {response: 'foo'}),
      error: true
    });
    clock.tick(1000);
    return ret;
  });

  it('resolves mockResponse if it is a function', () => {
    action[CALL_MOCK_API].mockResponse = sinon.stub().returns({
      responseJSON: {resolved: true}
    });

    mockApiMiddleware(store)(next)(action);
    expect(store.dispatch.args[0][0]).to.eql({type: 'REQUEST'});
    expect(action[CALL_MOCK_API].mockResponse.calledWith(action)).to.be.true;
  });

  it('returns a resolved promise, like redux-api-middleware', () => {
    const p = mockApiMiddleware(store)(next)(action);
    const ret = expect(p).to.eventually.eql({
      type: 'SUCCESS',
      meta: undefined,
      payload: action[CALL_MOCK_API].mockResponse.responseJSON
    });
    clock.tick(1000);
    return ret;
  });

  it('resolves payload function', () => {
    action[CALL_MOCK_API].types[1] = {type: 'SUCCESS', payload: () => {
      return new Promise((resolve) => resolve('completed'));
    }};

    const p = mockApiMiddleware(store)(next)(action);
    const ret = expect(p).to.eventually.eql({
      type: 'SUCCESS',
      meta: undefined,
      payload: 'completed'
    });
    clock.tick(1000);
    return ret;
  });

});
