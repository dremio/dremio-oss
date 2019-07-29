/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { RSAA } from 'redux-api-middleware';
import { createSampleSource } from './sources';

describe('createSampleSource', () => {
  const thunk = createSampleSource();
  const dispatch = sinon.stub();
  const getState = () => ({
    home: {
      pinnedEntities: {}
    },
    resources: {
      entities: Immutable.fromJS({
        source: {
          'source_id': {
            name: 'Samples'
          }
        },
        space: {
          'space_id': {
            path: ['Samples (1)'],
            containerType: 'SPACE'
          }
        }
      })
    }
  });
  it('generates a unique name for sample source', () => {
    thunk(dispatch, getState);
    const apiCallArg = dispatch.lastCall.args[0][RSAA];
    const [startAction] = apiCallArg.types;
    expect(startAction.meta.source.toJS().name).to.be.equal('Samples (2)');
  });
});
