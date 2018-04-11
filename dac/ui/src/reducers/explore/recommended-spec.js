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

import * as Actions from 'actions/explore/recommended';
import transformViewMapper from 'utils/mappers/ExplorePage/Transform/transformViewMapper';
import reducer from './recommended';

describe('explore/recommended reducer', () => {

  describe('initialState', () => {
    const result = reducer(null, {});
    it('contains initial transform state', () => {
      expect(result.getIn(['transform', 'default', 'cards'])).to.eql(Immutable.List());
    });
  });

  describe('RESET_RECOMMENDED_TRANSFORMS', () => {
    it('should reset', () => {
      const state = Immutable.fromJS({transform:{foo:{}}});
      const result = reducer(state, {type: Actions.RESET_RECOMMENDED_TRANSFORMS});
      expect(result.get('transform').keySeq().toArray()).to.eql(['default']);
    });
  });

  describe('TRANSFORM_CARD_PREVIEW', () => {
    const meta = {
      transformType: 'extract',
      method: 'default',
      index: 0,
      actionType: 'extract'
    };
    describe('START', () => {
      const result = reducer(null, {type: Actions.TRANSFORM_CARD_PREVIEW_START, meta});
      it('sets card isInProgress and unsets isFailed', () => {
        expect(
          result.getIn(['transform', meta.transformType, meta.method, 'cards', meta.index, 'isInProgress'])
        ).to.be.true;
        expect(
          result.getIn(['transform', meta.transformType, meta.method, 'cards', meta.index, 'isFailed'])
        ).to.be.false;
      });
    });

    describe('UPDATE_TRANSFORM_CARD', () => {
      let result;
      let card;
      sinon.stub(transformViewMapper, 'mapTransformRules').returns({
        cards: [{
          examplesList: ['foo'],
          unmatchedCount: 2,
          matchedCount: 8,
          description: 'foo description'
        }]
      });

      beforeEach(() => {
        result = reducer(null, {type: Actions.UPDATE_TRANSFORM_CARD, meta});
        card = result.getIn(['transform', meta.transformType, meta.method, 'cards', meta.index]);
      });

      it('unsets card isInProgress and isFailed', () => {
        expect(card.get('isInProgress')).to.be.false;
        expect(card.get('isFailed')).to.be.false;
      });

      it('makes sure cards is a list', () => {
        expect(
          result.getIn(['transform', meta.transformType, meta.method, 'cards'])
        ).to.be.an.instanceof(Immutable.List);
      });

      it('sets card values', () => {
        expect(card.get('examplesList')).to.eql(Immutable.List(['foo']));
        expect(card.get('unmatchedCount')).to.eql(2);
        expect(card.get('matchedCount')).to.eql(8);
        expect(card.get('description')).to.eql('foo description');
      });
    });
  });
});
