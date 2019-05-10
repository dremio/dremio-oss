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
import pinnedEntitiesReducer, { setEntityActiveState, isPinned } from './pinnedEntities';

describe('pinnedEntities reducer', () => {
  const defaultState = pinnedEntitiesReducer(undefined, { type: '__init' });

  it('sets isActive state to a state', () => {
    const newState = pinnedEntitiesReducer(defaultState, setEntityActiveState('1', true));
    expect(isPinned(newState, '1')).to.be.true;
  });

  it('removes current entity from state if isActive = false', () => {
    const newState = pinnedEntitiesReducer({ '1': true }, setEntityActiveState('1', false));
    expect(isPinned(newState, '1')).to.be.false;
    expect(newState).to.be.eql({});
  });
});
