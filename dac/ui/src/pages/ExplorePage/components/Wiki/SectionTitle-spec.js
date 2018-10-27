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
import { SectionTitle } from './SectionTitle';
import {
  title as titleInnerCls
} from './SectionTitle.less';

describe('SectionTitle', () => {
  it('Title is rendered', () => {
    const title = 'this is a title';
    const wrapper = shallow(<SectionTitle title={title} />);
    const titleCnt = wrapper.find('.' + titleInnerCls);

    expect(titleCnt).to.have.length(1); //title element should be presented
    expect(titleCnt.text()).to.eql(title); // title should be displayed
  });

  it('No buttons should be rendered if buttons array is empty', () => {
    const wrapper = shallow(<SectionTitle />);

    expect(wrapper.find('button')).to.have.length(0); //no buttons should be rendered
  });

  it('Right number of buttons is rendered', () => {
    const clickHandler = sinon.stub();
    const buttons = [{
      text: 'button1',
      onClick: clickHandler,
      key: 'button1'
    },
    {
      text: 'button2',
      onClick: clickHandler,
      key: 'button2'
    }];
    const wrapper = shallow(<SectionTitle buttons={buttons} />);

    expect(wrapper.find('button')).to.have.length(buttons.length); //right number of buttons should be rendered
  });

  it('Button click handler is called', () => {
    const clickHandler = sinon.stub();
    const buttons = [{
      text: 'button1',
      onClick: clickHandler,
      key: 'button'
    }];
    const wrapper = shallow(<SectionTitle buttons={buttons} />);
    const button = wrapper.find('button').at(0);

    button.simulate('click');
    expect(clickHandler).to.be.called;
  });
});
