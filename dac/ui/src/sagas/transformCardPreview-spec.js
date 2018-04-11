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
import { updateTransformCard } from 'actions/explore/recommended';
import { handleTransformCardPreview, getRequestPredicate } from './transformCardPreview';

describe('transformCardPreview saga', () => {
  describe('handleTransformCardPreview()', () => {
    let gen;
    let next;
    beforeEach(() => {
      gen = handleTransformCardPreview({meta: 'someMeta'});
      next = gen.next();
      expect(typeof next.value.RACE.success).to.not.be.undefined;
      expect(typeof next.value.RACE.failure).to.not.be.undefined;
      expect(typeof next.value.RACE.anotherRequest).to.not.be.undefined;
      expect(typeof next.value.RACE.reset).to.not.be.undefined;
    });

    it('should updateTransformCard on success', () => {
      const success = {payload: 'successPayload', meta: 'successMeta'};
      next = gen.next({success});
      expect(next.value).to.eql(put(updateTransformCard(success.payload, success.meta)));
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should not updateTransformCard if success.error', () => {
      next = gen.next({success: {error: true}});
      expect(next.done).to.be.true;
    });

    it('should not updateTransformCard on other race result', () => {
      next = gen.next({anotherRequest: true});
      expect(next.done).to.be.true;
    });
  });

  describe('getRequestPredicate()', () => {
    it('should return true only if action.type, transformType, method and card index are equal ', () => {
      const actionType = 'someAction';
      const meta = {
        transformType: 'someTransform',
        method: 'someMethod',
        index: 0
      };

      const predicate = getRequestPredicate(actionType, meta);
      expect(predicate({type: actionType, meta})).to.be.true;
      expect(predicate({type: 'foo', meta})).to.be.false;
      expect(predicate({type: actionType, meta: {...meta, transformType: 'foo'}})).to.be.false;
      expect(predicate({type: actionType, meta: {...meta, method: 'foo'}})).to.be.false;
      expect(predicate({type: actionType, meta: {...meta, index: 75}})).to.be.false;
    });
  });
});
