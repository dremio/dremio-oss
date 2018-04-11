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

import { CUSTOM_JOIN } from 'constants/explorePage/joinTabs';
import * as Actions from 'actions/explore/join';
import reducer from './join';

describe('explore/join reducer', () => {

  describe('initialState', () => {
    const result = reducer(null, {});
    it('contains initial transform state', () => {
      expect(result.getIn(['custom'])).to.eql(Immutable.Map({
        joinDatasetPathList: null,
        joinVersion: null,
        recommendation: null
      }));
      expect(result.getIn(['recommended'])).to.eql(Immutable.Map({
        recommendedJoins: Immutable.List(),
        activeRecommendedJoin: Immutable.Map()
      }));
    });
  });

  describe('UPDATE_JOIN_DATASET_VERSION', () => {
    it('should update the custom state values', () => {
      const values = {
        joinDatasetPathList: ['path1', 'path2'],
        joinVersion: 'joinVersion'
      };

      const result = reducer(null, {
        type: Actions.UPDATE_JOIN_DATASET_VERSION,
        ...values
      });

      expect(result.get('custom').toJS()).to.eql(values);
    });
  });

  describe('CLEAR_JOIN_DATASET', () => {
    it('should reset the custom join state', () => {
      const result = reducer(null, {
        type: Actions.CLEAR_JOIN_DATASET
      });

      expect(result.get('custom').toJS()).to.eql({
        joinDatasetPathList: null,
        joinVersion: null
      });
    });
  });

  describe('SET_JOIN_TAB', () => {
    it('should set the join tab state', () => {
      const state = Immutable.fromJS({
        joinTab: 'value',
        noData: false,
        step: 2,

        custom: {
          joinDatasetPathList: ['1'],
          joinVersion: 'version'
        }
      });

      const result = reducer(state, {
        type: Actions.SET_JOIN_TAB,
        tabId: 'tabId'
      });

      expect(result.get('custom').toJS()).to.eql({
        joinDatasetPathList: null,
        joinVersion: null
      });

      expect(result.get('joinTab')).to.eql('tabId');
      expect(result.get('noData')).to.eql(true);
      expect(result.get('step')).to.eql(null);
    });
  });

  describe('RESET_JOINS', () => {
    it('should reset the state', () => {
      const state = Immutable.fromJS({
        joinTab: 'value',
        noData: false,
        step: 2,

        custom: {
          joinDatasetPathList: ['1'],
          joinVersion: 'version'
        },

        recommended: {
          recommendedJoins: ['join'],
          activeRecommendedJoin: {foo: 'bar'}
        }
      });

      const result = reducer(state, {
        type: Actions.RESET_JOINS
      });

      expect(result.toJS()).to.eql({
        joinTab: null,
        step: null,
        noData: null,

        custom: {
          joinDatasetPathList: null,
          joinVersion: null,
          recommendation: null
        },

        recommended: {
          recommendedJoins: [],
          activeRecommendedJoin: {}
        }
      });
    });
  });

  describe('SET_JOIN_STEP', () => {
    it('should set the step state value', () => {
      const result = reducer(null, {
        type: Actions.SET_JOIN_STEP,
        step: 'step'
      });

      expect(result.get('step')).to.eql('step');
    });
  });

  describe('EDIT_RECOMMENDED_JOIN', () => {
    it('should set the tab to custom, step to 2 and set the right custom values', () => {
      const state = Immutable.fromJS({
        joinTab: 'value',
        noData: false,
        step: 2,

        custom: {
          joinDatasetPathList: ['1'],
          joinVersion: 'version'
        }
      });

      const data = {
        type: Actions.EDIT_RECOMMENDED_JOIN,
        recommendation: Immutable.fromJS({
          rightTableFullPathList: Immutable.fromJS(['a', 'path']),
          version: 'version'
        })
      };

      const result = reducer(state, data);

      expect(result.get('custom').toJS()).to.eql({
        joinDatasetPathList: ['a', 'path'],
        joinVersion: data.version,
        recommendation: data.recommendation.toJS()
      });

      expect(result.get('joinTab')).to.eql(CUSTOM_JOIN);
      expect(result.get('noData')).to.eql(true);
      expect(result.get('step')).to.eql(2);
    });
  });

  describe('LOAD_RECOMMENDED_JOIN_START', () => {
    it('should clear out the recommended state', () => {
      const result = reducer(null, {
        type: Actions.LOAD_RECOMMENDED_JOIN_START
      });

      expect(result.getIn(['recommended', 'recommendedJoins']).size).to.eql(0);
      expect(result.getIn(['recommended', 'activeRecommendedJoin']).size).to.eql(0);
    });
  });

  describe('LOAD_RECOMMENDED_JOIN_SUCCESS', () => {
    it('should update recommendedJoins in the recommended state', () => {
      const result = reducer(null, {
        type: Actions.LOAD_RECOMMENDED_JOIN_SUCCESS,
        payload: {
          recommendations: [{recommendation: true}]
        }
      });

      expect(result.getIn(['recommended', 'recommendedJoins']).size).to.eql(1);
    });
  });

  describe('SET_ACTIVE_RECOMMENDED_JOIN', () => {
    it('should update activeRecommendedJoin in the recommended state', () => {
      const result = reducer(null, {
        type: Actions.SET_ACTIVE_RECOMMENDED_JOIN,
        recommendation: {recommendation: true}
      });

      expect(result.getIn(['recommended', 'activeRecommendedJoin'])).to.eql({recommendation: true});
    });
  });

  describe('RESET_ACTIVE_RECOMMENDED_JOIN', () => {
    it('should reset activeRecommendedJoin in the recommended state', () => {
      const result = reducer(null, {
        type: Actions.RESET_ACTIVE_RECOMMENDED_JOIN
      });

      expect(result.getIn(['recommended', 'activeRecommendedJoin'])).to.eql(Immutable.Map());
    });
  });
});
