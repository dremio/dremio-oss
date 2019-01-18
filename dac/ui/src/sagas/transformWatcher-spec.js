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
import { put } from 'redux-saga/effects';
import { delay } from 'redux-saga';
import { CALL_API } from 'redux-api-middleware';

import { SHOW_CONFIRMATION_DIALOG } from 'actions/confirmation';
import { cancelTransform } from 'actions/explore/dataset/transform';
import { hideConfirmationDialog } from 'actions/confirmation';

import { unwrapAction } from './utils';

import {
  performWatchedTransform,
  cancelTransformWithModal,
  TransformCanceledError,
  TransformCanceledByLocationChangeError
} from './transformWatcher';

describe('transformWatcher saga', () => {
  let gen;
  let next;
  const apiAction = {
    [CALL_API]: {
      types: ['START', 'SUCCESS', 'FAILURE']
    }
  };

  describe('performWatchedTransform', () => {
    beforeEach(() => {
      gen = performWatchedTransform(apiAction, 'viewId');
      next = gen.next();
      expect(next.value).to.eql(put(apiAction));
      next = gen.next(new Promise(() => {}));
      next = gen.next(() => true); // getExplorePageLocationChangePredicate
      expect(next.value.RACE).to.not.be.undefined;
    });

    it('should throw TransformCanceledError if cancel wins the race', () => {
      expect(() => {
        gen.next({cancel: 'cancel'});
      }).to.throw(TransformCanceledError);
    });

    it('should hide the modal and throw TransformCanceledError if resetNewQuery wins the race', () => {
      next = gen.next({resetNewQuery: true});
      expect(next.value).to.eql(put(hideConfirmationDialog()));
      expect(() => {
        next = gen.next();
      }).to.throw(TransformCanceledError);
    });

    it('should hide the modal and return tableTransform if transform wins the race', () => {
      next = gen.next({tableTransform: 'tableTransform'});
      expect(next.value).to.eql(put(hideConfirmationDialog()));
      next = gen.next();
      expect(next.done).to.be.true;
      expect(next.value).to.equal('tableTransform');
    });

    it('should hide the modal and throw TransformCanceledByLocationChangeError if location change wins', () => {
      next = gen.next({locationChange: 'locationChange'});
      expect(next.value).to.eql(put(hideConfirmationDialog()));
      expect(() => {
        next = gen.next();
      }).to.throw(TransformCanceledByLocationChangeError);
    });
  });

  describe('cancelTransformWithModal', () => {
    it('should delay, shows the modal, wait for confirm, put cancel, and return true', () => {
      gen = cancelTransformWithModal('viewId');
      next = gen.next();
      expect(next.value.CALL.fn).to.equal(delay);
      next = gen.next();
      expect(unwrapAction(next.value.PUT.action).type).to.equal(SHOW_CONFIRMATION_DIALOG);
      next = gen.next();
      next = gen.next();
      expect(next.value).to.eql(put(cancelTransform('viewId')));
      next = gen.next();
      expect(next.value).to.be.true;
    });
  });
});
