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
import ColumnMenuItem from  './ColumnMenuItem';
import DragSource from './DragSource';

describe('ColumnMenuItem', () => {

  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      item: Immutable.Map({name: 'theName'}),
      dragType: ''
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ColumnMenuItem {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render DragSource with preventDrag undefined, .draggable-icon if preventDrag is undefined', () => {
    const wrapper = shallow(<ColumnMenuItem {...minimalProps}/>);
    expect(wrapper.find(DragSource).prop('preventDrag')).to.be.undefined;
    expect(wrapper.find({ class: 'draggable-icon' })).to.have.length(1);
  });

  it('should render DragSource with preventDrag true and not render .draggable-icon if preventDrag is true', () => {
    const props = {
      ...minimalProps,
      preventDrag: true
    };
    const wrapper = shallow(<ColumnMenuItem {...props}/>);
    expect(wrapper.find(DragSource).prop('preventDrag')).to.be.true;
    expect(wrapper.find({ class: 'draggable-icon' })).to.have.length(0);
  });

  describe('checkThatDragAvailable', () => {
    let e;
    let wrapper;
    let instance;
    beforeEach(() => {
      e = {
        preventDefault: sinon.spy(),
        stopPropagation: sinon.spy()
      };
      wrapper = shallow(<ColumnMenuItem {...minimalProps}/>);
      instance = wrapper.instance();
    });
    it('should not call preventDefault, stopPropagation if preventDrag is undefined and item is not disabled', () => {
      instance.checkThatDragAvailable(e);
      expect(e.stopPropagation.called).to.be.false;
      expect(e.preventDefault.called).to.be.false;
    });
    it('should call preventDefault, stopPropagation if preventDrag is true', () => {
      wrapper.setProps({
        ...minimalProps,
        preventDrag: true
      });
      instance.checkThatDragAvailable(e);
      expect(e.preventDefault.called).to.be.true;
      expect(e.stopPropagation.called).to.be.true;
    });
    it('should call preventDefault, stopPropagation if item is disabled', () => {
      wrapper.setProps({
        ...minimalProps,
        item: Immutable.fromJS({
          name: 'theName'
        }),
        disabled: true
      });
      instance.checkThatDragAvailable(e);
      expect(e.preventDefault.called).to.be.true;
      expect(e.stopPropagation.called).to.be.true;
    });
  });
});
