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
import Immutable  from 'immutable';
import * as ActionTypes from 'actions/explore/join';
import { EDIT_RECOMMENDED_JOIN } from 'actions/explore/join';
import { CUSTOM_JOIN } from 'constants/explorePage/joinTabs';

const initialState = Immutable.fromJS({
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

export default function join(oldState, action) {
  const state = oldState || initialState;

  switch (action.type) {
  case ActionTypes.UPDATE_JOIN_DATASET_VERSION: {
    return state.merge({
      custom: {
        joinDatasetPathList: action.joinDatasetPathList,
        joinVersion: action.joinVersion
      }
    });
  }

  case ActionTypes.CLEAR_JOIN_DATASET: {
    return state.merge({
      custom: {
        joinDatasetPathList: null,
        joinVersion: null
      }
    });
  }

  case ActionTypes.SET_JOIN_TAB: {
    return state.merge({
      joinTab: action.tabId,
      noData: true,
      step: null,

      custom: {
        joinDatasetPathList: null,
        joinVersion: null
      }
    });
  }

  case ActionTypes.RESET_JOINS: {
    return initialState;
  }

  case ActionTypes.SET_JOIN_STEP: {
    return state.set('step', action.step);
  }

  case EDIT_RECOMMENDED_JOIN: {
    const dataset = action.recommendation.get('rightTableFullPathList').toJS();

    return state.merge({
      joinTab: CUSTOM_JOIN,
      noData: true,
      step: 2,

      custom: {
        joinDatasetPathList: dataset,
        joinVersion: action.version,
        recommendation: action.recommendation
      }
    });
  }

  case ActionTypes.LOAD_RECOMMENDED_JOIN_START: {
    return state.setIn(['recommended', 'recommendedJoins'], Immutable.List([]))
      .setIn(['recommended', 'activeRecommendedJoin'], Immutable.Map());
  }

  case ActionTypes.LOAD_RECOMMENDED_JOIN_SUCCESS: {
    return state.setIn(['recommended', 'recommendedJoins'], Immutable.fromJS(action.payload.recommendations || []));
  }

  case ActionTypes.SET_ACTIVE_RECOMMENDED_JOIN: {
    return state.setIn(['recommended', 'activeRecommendedJoin'], action.recommendation);
  }

  case ActionTypes.RESET_ACTIVE_RECOMMENDED_JOIN: {
    return state.setIn(['recommended', 'activeRecommendedJoin'], Immutable.Map());
  }

  default:
    return state;
  }
}
