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
import invariant from 'invariant';
import exploreUtils from 'utils/explore/exploreUtils';
import { getExploreState } from '@app/selectors/explore';

export function getTransformCards(state, transform, defaultCard) {
  invariant(transform, 'Missed transformation state for getTransformCards selector');
  if (!transform) {
    return Immutable.List();
  }

  const hasSelection = exploreUtils.transformHasSelection(transform);

  const transformType = transform.get('transformType');
  const method = transform.get('method') || 'default';
  const recommended = getExploreState(state).recommended;

  const defaultTransform = recommended.getIn(['transform', 'default']);
  const transformWithCards = recommended.getIn(['transform', transformType, method]);
  const cards = transformWithCards && transformWithCards.get('cards');

  if (!hasSelection) {
    if (transformWithCards) {
      return cards.mergeIn([0], defaultCard).toList();
    }

    const defaultCards = defaultTransform.get('cards');
    return defaultCards.merge([defaultCard]);
  }

  return cards || Immutable.List();
}
