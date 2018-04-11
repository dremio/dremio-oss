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
import { mount } from 'enzyme';

import TextHighlight from 'components/TextHighlight';

describe('TextHighlight-spec', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      text: 'Some text here',
      inputValue: 'text'
    };
  });

  it('minimal props', () => {
    const wrapper = mount(<TextHighlight {...minimalProps} />);
    expect(wrapper.find('.TextHighlight')).have.length(1);
  });

  it('render elements', () => {
    const wrapper = mount(<TextHighlight {...commonProps} inputValue={null} />);
    expect(wrapper.find('.TextHighlight')).have.length(1);
    expect(wrapper.find('.TextHighlight').html().replace(/<!--.*?-->/g, ''))
      .equal('<span class="TextHighlight">Some text here</span>');
  });

  it('check selected', () => {
    const wrapper = mount(<TextHighlight {...commonProps} />);
    expect(wrapper.find('.TextHighlight').html().replace(/<!--.*?-->/g, ''))
      .equal('<span class="TextHighlight">Some <b>text</b> here</span>');
  });

});
