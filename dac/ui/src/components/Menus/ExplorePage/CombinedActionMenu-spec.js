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
import {shallow} from 'enzyme';
import Immutable from 'immutable';
import {CombinedActionMenu} from './CombinedActionMenu';

describe('CombinedActionMenu', () => {
  let minimalProps;
  let wrapper;
  let instance;

  beforeEach(() => {
    minimalProps = {
      dataset: Immutable.fromJS({}),
      closeMenu: () => {
      },
      intl: {formatMessage: () => 'msg'}
    };
    wrapper = shallow(<CombinedActionMenu {...minimalProps}/>);
    instance = wrapper.instance();
  });

  it('should render download selection header w/o jobProgress', () => {
    expect(instance.renderDownloadSectionHeader()).to.be.defined;
  });
});
