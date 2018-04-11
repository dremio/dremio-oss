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

import SVG from './SVG';

describe('SVG-spec', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      src: 'NarwhalLogo.svg',
      'aria-label': 'some text'
    };
    commonProps = {
      ...minimalProps,
      title: 'tooltip'
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SVG {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<SVG {...commonProps} />);
    expect(wrapper).to.have.length(1);
  });

  it('should render with <img> by default', () => {
    const wrapper = shallow(<SVG {...commonProps} />);
    expect(wrapper.find('img')).to.have.length(1);
    expect(wrapper.find('img').props().title).to.equal('tooltip');
  });

  it('should render title=true with the aria-label', () => {
    const wrapper = shallow(<SVG {...commonProps} title/>);
    expect(wrapper.find('img').props().title).to.equal('some text');
  });

// todo: enable test when we have such an image
//   it('should render inline without exploding', () => {
//     const wrapper = shallow(<SVG {...commonProps} />);
//     expect(wrapper).to.have.length(1);
//   });
});
