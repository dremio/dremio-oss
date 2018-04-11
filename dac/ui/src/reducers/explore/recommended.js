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
import * as ActionTypes from 'actions/explore/recommended';
import * as CleanActionTypes from 'actions/explore/dataset/get';

import transformViewMapper from 'utils/mappers/ExplorePage/Transform/transformViewMapper';
import transforms from 'utils/explore/transforms';

const initialState = Immutable.fromJS({
  transform: {
    default: {
      cards: [],
      values: {},
      data: {},
      isInProgress: false,
      isFailed: false
    }
  },
  cleanData: {
    newFieldName: '',
    newFieldNamePrefix: '',
    single: [],
    split: {
      dataTypes: [],
      availableValuesCount: 0,
      availableValues: []
    },
    isInProgress: true,
    isFailed: false
  }
});

export default function grid(oldState, action) {
  const state = oldState || initialState;

  const {transformType, method, actionType, index} = action.meta || {};
  switch (action.type) {

  case ActionTypes.RESET_RECOMMENDED_TRANSFORMS: {
    return state.set('transform', initialState.get('transform'));
  }

  case ActionTypes.RUN_SELECTION_TRANSFORM_START: {
    return state.setIn(['transform', transformType, method, 'cards'], Immutable.List());
  }

  case ActionTypes.RUN_SELECTION_TRANSFORM_SUCCESS: {
    const mappedData = transformViewMapper.mapTransformRules(action.payload, actionType);
    const cards = mappedData.length ? mappedData : mappedData.cards;
    const values = mappedData.values || {};
    return state.setIn(['transform', transformType, method, 'cards'], Immutable.fromJS(cards))
                .setIn(['transform', transformType, method, 'values'], Immutable.fromJS(values));
  }

  case ActionTypes.RUN_SELECTION_TRANSFORM_FAILURE: {
    return state.setIn(['transform', transformType, method, 'cards'], Immutable.fromJS([]));
  }

  case ActionTypes.TRANSFORM_CARD_PREVIEW_START: {
    // this case is needed when we click on Extract Text... (don't select text in column)
    // we don't call loadTransformCards when open empty card. That's why we don't create Immutable.List in TRANSFORM_CARD_PREVIEW_START
    // in this case, we have not ['transform', transformType, method, 'cards'] yet
    if (!state.getIn(['transform', transformType, method, 'cards'])) {
      return state.setIn(['transform', transformType, method, 'cards'],
                         Immutable.fromJS([{isInProgress: true, isFailed: false}]));
    }
    return state.setIn(['transform', transformType, method, 'cards', index, 'isInProgress'], true)
                .setIn(['transform', transformType, method, 'cards', index, 'isFailed'], false);
  }

  // This action is triggered by transformCardPreview saga
  case ActionTypes.UPDATE_TRANSFORM_CARD: {
    let cards = transformViewMapper.mapTransformRules({cards: [action.payload]}, actionType);
    cards = cards.cards || cards;
    const examplesList = cards[0] && cards[0].examplesList || [];
    const unmatchedCount = cards[0] && cards[0].unmatchedCount || 0;
    const matchedCount = cards[0] && cards[0].matchedCount || 0;
    const description = cards[0] && cards[0].description;

    const nextCards = state.getIn(['transform', transformType, method, 'cards']) || Immutable.List();
    const card = (nextCards.get(index) || Immutable.Map())
      .merge({
        description,
        examplesList,
        unmatchedCount,
        matchedCount,
        isInProgress: false,
        isFailed: false
      });

    return state.setIn(['transform', transformType, method, 'cards'], nextCards.set(index, card));
  }

  case ActionTypes.RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_START: {
    return state;
  }

  case ActionTypes.RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_SUCCESS: {
    const values = state.getIn(['transform', transformType, method, 'values']);
    if (!values || typeof values === 'function') {
      return state;
    }
    return state
      .setIn(['transform', transformType, method, 'values', 'matchedCount'], action.payload.matchedValues)
      .setIn(['transform', transformType, method, 'values', 'unmatchedCount'], action.payload.unmatchedValues);
  }

  case CleanActionTypes.LOAD_CLEAN_DATA_START: {
    return state.setIn(['cleanData', 'isInProgress'], true)
                .setIn(['cleanData', 'isFailed'], false);
  }

  case CleanActionTypes.LOAD_CLEAN_DATA_SUCCESS: {
    return state.setIn(['cleanData', 'isInProgress'], false)
                .setIn(['cleanData', 'isFailed'], false)
                .setIn(['cleanData', 'newFieldName'], action.payload.newFieldName)
                .setIn(['cleanData', 'newFieldNamePrefix'], action.payload.newFieldNamePrefix)
                .setIn(['cleanData', 'single'], Immutable.fromJS(
                  transforms.setDesiredTypes(action.payload.convertToSingles)
                ))
                .setIn(['cleanData', 'split', 'dataTypes'], action.payload.split)
                .setIn(['cleanData', 'split', 'availableValuesCount'], action.payload.availableValuesCount)
                .setIn(['cleanData', 'split', 'availableValues'], action.payload.availableValues);
  }

  case CleanActionTypes.LOAD_CLEAN_DATA_FAILURE: {
    return state.setIn(['cleanData', 'isInProgress'], false)
                .setIn(['cleanData', 'isFailed'], true)
                .setIn(['cleanData', 'single'], Immutable.List())
                .setIn(['cleanData', 'split', 'dataTypes'], Immutable.List())
                .setIn(['cleanData', 'split', 'availableValuesCount'], 0)
                .setIn(['cleanData', 'split', 'availableValues'], Immutable.List());
  }

  default:
    return state;
  }
}
