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
import { call } from 'redux-saga/effects';
import Immutable from 'immutable';

import { SHOW_CONFIRMATION_DIALOG } from 'actions/confirmation';
import {
  handleTransformHistoryCheck, transformHistoryCheck, shouldShowWarningModal, confirmTransform
} from './transformHistoryCheck';

describe('transformHistoryCheck saga', () => {

  const dataset = Immutable.fromJS({
    datasetVersion: '123'
  });
  let gen;
  let next;
  describe('handleTransformHistoryCheck', () => {
    it('should call transformHistoryCheck and continueCallback if confirmed', () => {
      const continueCallback = sinon.spy();
      gen = handleTransformHistoryCheck({meta: {dataset, continueCallback}});
      next = gen.next();
      expect(next.value).to.eql(call(transformHistoryCheck, dataset));
      next = gen.next(true);
      expect(next.value).to.eql(call(continueCallback));
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should call transformHistoryCheck and cancelCallback if canceled', () => {
      const cancelCallback = sinon.spy();
      gen = handleTransformHistoryCheck({meta: {dataset, cancelCallback}});
      next = gen.next();
      expect(next.value).to.eql(call(transformHistoryCheck, dataset));
      next = gen.next(false);
      expect(next.value).to.eql(call(cancelCallback));
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should call transformHistoryCheck, but not continueCallback if confirmed and continueCallback is not defined', () => {
      gen = handleTransformHistoryCheck({meta: {dataset}});
      next = gen.next();
      expect(next.value).to.eql(call(transformHistoryCheck, dataset));
      next = gen.next(true);
      expect(next.done).to.be.true;
    });

    it('should call transformHistoryCheck, but not cancelCallback if canceled and cancelCallback is not defined', () => {
      gen = handleTransformHistoryCheck({meta: {dataset}});
      next = gen.next();
      expect(next.value).to.eql(call(transformHistoryCheck, dataset));
      next = gen.next(false);
      expect(next.done).to.be.true;
    });
  });

  describe('transformHistoryCheck', () => {
    it('should check history and show modal if necessary', () => {
      gen = transformHistoryCheck(dataset);
      next = gen.next();
      next = gen.next('history');
      expect(next.value.CALL.fn).to.equal(shouldShowWarningModal);
      next = gen.next(true);
      expect(next.value).to.eql(call(confirmTransform));
      next = gen.next(true);
      expect(next.value).to.be.true;
      expect(next.done).to.be.true;
    });

    it('should check history and return true if model not necessary', () => {
      gen = transformHistoryCheck(dataset);
      next = gen.next();
      next = gen.next('history');
      expect(next.value.CALL.fn).to.equal(shouldShowWarningModal);
      next = gen.next(false);
      expect(next.value).to.be.true;
      expect(next.done).to.be.true;
    });
  });

  describe('shouldShowWarningModal', () => {
    it('should return false if dataset version is not found or is the first history item', () => {
      const history = Immutable.fromJS([{datasetVersion: 'v1'}, {datasetVersion: 'v2'}]);
      expect(shouldShowWarningModal(history, dataset)).to.be.false;
      expect(shouldShowWarningModal(history, Immutable.fromJS({datasetVersion: 'v1'}))).to.be.false;
      expect(shouldShowWarningModal(history, Immutable.fromJS({datasetVersion: 'v2'}))).to.be.true;
    });
  });

  describe('confirmTransform', () => {
    it('should return true if confirmed', () => {
      gen = confirmTransform();
      next = gen.next();
      expect(next.value.PUT.action.type).to.eq(SHOW_CONFIRMATION_DIALOG);
      next = gen.next(); // select(getLocation)
      next = gen.next();
      expect(next.value.RACE).to.not.be.undefined;
      next = gen.next({confirm: true});
      expect(next.value).to.be.true;
      expect(next.done).to.be.true;
    });

    it('should return false if not confirmed', () => {
      gen = confirmTransform();
      next = gen.next();
      expect(next.value.PUT.action.type).to.eq(SHOW_CONFIRMATION_DIALOG);
      next = gen.next(); // select(getLocation)
      next = gen.next();
      expect(next.value.RACE).to.not.be.undefined;
      next = gen.next({cancel: true});
      expect(next.value).to.be.false;
      expect(next.done).to.be.true;
    });
  });
});
