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
import { shallow } from 'enzyme';
import Immutable from 'immutable';
import SearchField from './SearchField';

describe('SearchField', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      value: '123',
      showCloseIcon: true,
      onChange: sinon.spy(),
      columns: Immutable.fromJS([
        { name: 'name' },
        { name: 'foo' },
        { name: 'bar' }
      ])
    };
    wrapper = shallow(<SearchField {...commonProps}/>);
  });
  it('should render with minimal props without exploding', () => {
    const wrapperMinProps = shallow(<SearchField {...minimalProps}/>);
    expect(wrapperMinProps).to.have.length(1);
  });
  it('should render 1 FontIcon components, input', () => {
    expect(wrapper.find('FontIcon')).to.have.length(1);
    expect(wrapper.find('input')).to.have.length(1);
  });
});
