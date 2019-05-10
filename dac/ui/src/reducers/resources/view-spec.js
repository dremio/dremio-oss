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
import { RequestError, ApiError, InternalError } from 'redux-api-middleware';

import { RESET_VIEW_STATE, UPDATE_VIEW_STATE, DISMISS_VIEW_STATE_ERROR } from 'actions/resources';
import { CLEAR_ENTITIES } from 'actions/resources/entities';

import viewReducer, { getErrorMessage, getErrorDetails, NO_INTERNET_MESSAGE } from './view';

const VIEW_ID = 'someViewId';

describe('reducers.resources.view', () => {
  const initialState =  Immutable.fromJS({
    someViewId: {
      viewId: VIEW_ID, isInProgress: true, isFailed: false, invalidated: false
    }
  });

  it('returns unaltered state by default', () => {
    const result = viewReducer(initialState, {type: 'bla'});
    expect(result).to.equal(initialState);
  });

  describe('RESET_VIEW_STATE', () => {
    it('should reset isInProgress, isFailed and invalidated, and keep viewId', () => {
      const needsResetingState =  Immutable.fromJS({
        someViewId: {
          viewId: VIEW_ID, isInProgress: true, isFailed: true, invalidated: true
        }
      });
      const result = viewReducer(needsResetingState, {type: RESET_VIEW_STATE, meta: {viewId: VIEW_ID}});
      expect(result.get(VIEW_ID).toJS()).to.eql({
        viewId: VIEW_ID, isInProgress: false, isFailed: false, invalidated: false, error: null, isWarning: false
      });
    });
  });

  describe('UPDATE_VIEW_STATE', () => {
    it('should merge meta.viewState into viewState', () => {
      const result = viewReducer(initialState, {
        type: UPDATE_VIEW_STATE, meta: {viewId: VIEW_ID, viewState: {foo: 'bar'}}
      });
      expect(result.get(VIEW_ID).get('viewId')).to.equal(VIEW_ID);
      expect(result.get(VIEW_ID).get('foo')).to.equal('bar');
    });
  });

  describe('DISMISS_VIEW_STATE_ERROR', () => {
    it('should set dismissed on viewState.error', () => {
      const result = viewReducer(initialState, {
        type: DISMISS_VIEW_STATE_ERROR, meta: {viewId: VIEW_ID}
      });
      expect(result.get(VIEW_ID).get('viewId')).to.equal(VIEW_ID);
      expect(result.get(VIEW_ID).getIn(['error', 'dismissed'])).to.be.true;
    });
  });

  describe('CLEAR_ENTITIES', () => {
    it('should clear view state', () => {
      const result = viewReducer(initialState, {
        type: CLEAR_ENTITIES
      });
      expect(result).to.eql(Immutable.Map());
    });
  });


  describe('getErrorMessage()', () => {

    it('should return name and message from error', () => {
      const error = new InternalError('message');
      expect(getErrorMessage({
        payload: error
      })).to.eql({
        errorMessage: 'InternalError: message.'
      });
    });

    it('should return errorMessage from response of ApiError if present', () => {
      expect(getErrorMessage({
        payload: new ApiError('status', 500, {
          errorMessage: 'apiError'
        })
      })).to.eql({
        errorMessage: 'apiError'
      });
    });

    it('should return message from response of ApiError if no response errorMessage', () => {
      const error = new ApiError('status', 500);
      expect(getErrorMessage({
        payload: error
      })).to.eql({
        errorMessage: error.message
      });
    });

    it('should return special message for RequestError', () => {
      expect(getErrorMessage({
        payload: new RequestError()
      })).to.eql({
        errorMessage: NO_INTERNET_MESSAGE
      });
    });

    it('should allow overriding error message with action.meta', () => {
      expect(getErrorMessage({
        meta: {errorMessage: 'different'},
        payload: new RequestError()
      })).to.eql({
        errorMessage: 'different'
      });
    });
  });

  describe('getErrorDetails', () => {
    it('should return details from response', () => {
      const error = new ApiError('status', 500, {details: 'details'});
      expect(getErrorDetails({payload: error})).to.equal('details');
    });

    it('should return undefined if no details', () => {
      expect(getErrorDetails({})).to.be.undefined;
      expect(getErrorDetails({payload: {}})).to.be.undefined;
      expect(getErrorDetails({payload: {response: {}}})).to.be.undefined;
    });
  });
});
