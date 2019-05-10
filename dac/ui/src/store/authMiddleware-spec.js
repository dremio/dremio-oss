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
import { push } from 'react-router-redux';
import * as accountActions from 'actions/account';

import * as routes from 'routes';

import authMiddleware, {UNAUTHORIZED_URL_PARAM, isUnauthorisedReason} from './authMiddleware';


describe('auth middleware', () => {
  let store;
  let middleware;
  beforeEach(() => {
    store = {
      getState() {
        return {
          account: Immutable.fromJS({
            user: {
              userName: 'dremio'
            }
          })
        };
      }
    };
    middleware = authMiddleware(store);
  });
  it('should generate push, then UNAUTHORIZED_ERROR for a 401 error payload', () => {
    const action = {
      error: 'an error',
      payload: {
        status: 401
      }
    };

    sinon.stub(routes, 'getLoginUrl').returns('/foo');
    const next = sinon.spy();
    middleware(next)(action);

    expect(next.args[0][0]).to.eql(push(`/foo&${UNAUTHORIZED_URL_PARAM}`));
    expect(next.args[1][0]).to.eql(accountActions.unauthorizedError());

    routes.getLoginUrl.restore();
  });

  it('should not generate push for 401 if already at /login', () => {
    const action = {
      error: 'an error',
      payload: {
        status: 401
      }
    };

    window.location.pathname = routes.LOGIN_PATH;
    sinon.stub(routes, 'getLoginUrl').returns('/foo');
    const next = sinon.spy();
    middleware(next)(action);

    expect(next.args[1][0]).to.eql(accountActions.unauthorizedError());

    routes.getLoginUrl.restore();
  });

  it('should do nothing and pass the action back for a 401 payload that is of type LOGIN_USER_FAILURE', () => {
    const action = {
      type: accountActions.LOGIN_USER_FAILURE,
      error: 'an error',
      payload: {
        status: 401
      }
    };

    const next = sinon.spy((result) => {
      expect(result).to.deep.equal(action);
    });

    middleware(next)(action);

    expect(next).to.have.been.calledOnce;
  });

  it('should generate push, then NO_USERS_ERROR for a "No User Available" 403 error payload', () => {
    const action = {
      error: 'an error',
      payload: {
        status: 403,
        response: {
          errorMessage: 'No User Available'
        }
      }
    };

    const next = sinon.spy();
    middleware(next)(action);

    expect(next.args[0][0]).to.eql(push('/signup'));
    expect(next.args[1][0]).to.eql(accountActions.noUsersError());
  });

  it('should not generate push for 403 if already at /signup', () => {
    const action = {
      error: 'an error',
      payload: {
        status: 403,
        response: {
          errorMessage: 'No User Available'
        }
      }
    };

    window.location.pathname = routes.SIGNUP_PATH;
    const next = sinon.spy();
    middleware(next)(action);

    expect(next.args[1][0]).to.eql(accountActions.noUsersError());
  });

  it('should do nothing and pass the action back for a 403 payload that is not "No User Available"', () => {
    const next = sinon.spy((result) => {
      expect(result).to.deep.equal(action);
    });

    const action = {
      error: 'an error',
      payload: {
        status: 403,
        response: {
          errorMessage: 'foo bar'
        }
      }
    };
    middleware(next)(action);


    delete action.payload.response;
    middleware(next)(action);

    expect(next).to.have.been.calledTwice;
  });

  it('should detect unauthorized reason in location', () => {
    expect(isUnauthorisedReason({search: 'abc'})).to.equal(false);
    expect(isUnauthorisedReason({search: 'abc&reason'})).to.equal(false);
    expect(isUnauthorisedReason({search: 'abc&reason=401'})).to.equal(true);
  });

});
