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

import { QlikStateModal } from './QlikStateModal';

describe('QlikStateModal', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      retry: sinon.spy(),
      hide: sinon.spy(),
      dataset: Immutable.Map()
    };
    commonProps = {
      ...minimalProps,
      message: 'foo'
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<QlikStateModal {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render ConfirmCancelFooter', () => {
    const wrapper = shallow(<QlikStateModal {...commonProps}/>);
    expect(wrapper.find('ConfirmCancelFooter')).to.have.length(1);
  });

  it('should render Modal', () => {
    const wrapper = shallow(<QlikStateModal {...commonProps}/>);
    expect(wrapper.find('Modal')).to.have.length(1);
  });
});
