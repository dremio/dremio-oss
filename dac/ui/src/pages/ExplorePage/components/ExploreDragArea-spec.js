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

import DragTarget from 'components/DragComponents/DragTarget';

import ExploreDragArea from './ExploreDragArea';

describe('ExploreDragArea', () => {
  let minimalProps;
  let commonProps;

  beforeEach(() => {
    minimalProps = {
      handleDrop: sinon.spy(),
      getColumnsForDragArea: sinon.spy(),
      dragType: 'test',
      emptyDragAreaText: 'DragTarget TEXT',
      isDragged: false
    };
    commonProps = {
      ...minimalProps,
      children: [<div>foo</div>]
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ExploreDragArea {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });
  it('should render emptyDragAreaText if no children', () => {
    const wrapper = shallow(<ExploreDragArea {...minimalProps} />);
    expect(wrapper.find('.empty-text').text()).to.eql(minimalProps.emptyDragAreaText);
  });
  it('should not render emptyDragAreaText if there are children', () => {
    const wrapper = shallow(<ExploreDragArea {...commonProps} />);
    expect(wrapper.find('.empty-text')).to.have.length(0);
  });
  it('should render DragTarget component', () => {
    const wrapper = shallow(<ExploreDragArea {...commonProps} />);
    expect(wrapper.find(DragTarget)).to.have.length(1);
  });
});
