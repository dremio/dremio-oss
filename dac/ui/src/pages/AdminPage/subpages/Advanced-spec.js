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
import { shallow } from 'enzyme';
import Immutable from 'immutable';
import { LABELS } from './settingsConfig';
import { Advanced } from './Advanced';

describe('Advanced', () => {

  let minimalProps;
  let commonProps;

  beforeEach(() => {
    minimalProps = { // todo: find a way to auto-gen this based on propTypes def
      getAllSettings: sinon.stub().returns(Promise.resolve()),
      resetSetting: sinon.stub().returns(Promise.resolve()),
      addNotification: sinon.stub().returns(Promise.resolve()),

      viewState: new Immutable.Map(),

      settings: new Immutable.Map(),
      setChildDirtyState: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      settings: Immutable.fromJS({
        '$a': {
          id: '$a',
          value: 1,
          type: 'INTEGER'
        }
      })
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<Advanced {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  it('should getAllSettings on mount', () => {
    shallow(<Advanced {...commonProps} />);
    expect(commonProps.getAllSettings).to.have.been.calledWith(Object.keys(LABELS), false, 'ADVANCED_SETTINGS_VIEW_ID');
  });
});
