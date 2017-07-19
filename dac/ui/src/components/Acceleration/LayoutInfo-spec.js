/*
 * Copyright (C) 2017 Dremio Corporation
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

import LayoutInfo from './LayoutInfo';

describe('LayoutInfo', () => {
  let minimalProps;
  let commonProps;

  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      layout: Immutable.fromJS({
        id:{id:'a'},
        // should be able to render without: currentByteSize
        totalByteSize: 1536,
        hasValidMaterialization: true,
        latestMaterializationState: 'NEW'
      })
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<LayoutInfo {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<LayoutInfo {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('shows expired as needed', () => {
    const props = {
      ...commonProps,
      layout: commonProps.layout.set('latestMaterializationState', 'DONE').set('hasValidMaterialization', false)
    };
    const wrapper = shallow(<LayoutInfo {...props}/>);
    expect(wrapper.find('FontIcon').props().type).to.equal('Warning-Solid');
  });
});
