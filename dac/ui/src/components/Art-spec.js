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

import Art from './Art';

describe('Art-spec', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      src: '_TestBitmap.png',
      alt: 'some text'
    };
    commonProps = {
      ...minimalProps,
      title: 'tooltip'
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<Art {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<Art {...commonProps} />);
    expect(wrapper).to.have.length(1);
  });

  it('should render with <img> by default', () => {
    const wrapper = shallow(<Art {...commonProps} />);
    expect(wrapper.find('img')).to.have.length(1);
    expect(wrapper.find('img').props().title).to.equal('tooltip');
  });

  it('should render with <SVG> as needed', () => {
    const wrapper = shallow(<Art {...commonProps} src='NarwhalLogo.svg' />);
    expect(wrapper.find('SVG')).to.have.length(1);
  });

  it('should render title=true with the alt', () => {
    const wrapper = shallow(<Art {...commonProps} title/>);
    expect(wrapper.find('img').props().title).to.equal('some text');
  });
});
