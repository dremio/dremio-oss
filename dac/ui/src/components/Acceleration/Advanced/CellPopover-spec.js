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
import DragTarget from 'components/DragComponents/DragTarget';
import DragSource from 'components/DragComponents/DragSource';
import Divider from 'material-ui/Divider';
import CellPopover from './CellPopover';

describe('CellPopover', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  let context;
  beforeEach(() => {
    minimalProps = {
      onRequestClose: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      sortFieldList: []
    };
    commonProps.sortFieldList.swapFields = sinon.spy();
    commonProps.sortFieldList.addField = sinon.spy();
    commonProps.sortFieldList.removeField = sinon.spy();
    context = { router: { push: sinon.spy() } };
    wrapper = shallow(<CellPopover {...commonProps}/>, {context});
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<CellPopover {...minimalProps}/>, {context});
    expect(wrapper).to.have.length(1);
  });

  describe('renderContent', () => {
    beforeEach(() => {
      sinon.stub(instance, 'renderSortMenu');
    });

    it('should render Menu for Sort', () => {
      wrapper.setProps({ currentCell: 'sort' });
      expect(instance.renderSortMenu).to.have.been.called;
    });
  });

  describe('#componentWillMount', () => {
    it('should set appropriate drag field list to state', () => {
      const sortFieldList = [{name: {value: 'name'}}];
      const w = shallow(<CellPopover {...commonProps} sortFieldList={sortFieldList} />);
      expect(w.state().dragColumns.sortFieldList).to.be.eql([{name: 'name'}]);
    });
  });

  describe('#receiveProps', () => {
    it('should update fields in state when old props are empty', () => {
      const sortFieldList = [{name: {value: 'name'}}];
      instance.receiveProps({sortFieldList});
      expect(wrapper.state().dragColumns.sortFieldList).to.be.eql([{name: 'name'}]);
    });

    it('should update fields in state fields were changed', () => {
      const oldFields = [{name: {value: 'name'}}];
      const newFields = [{name: {value: 'name2'}}];
      instance.receiveProps({sortFieldList: newFields}, {sortFieldList: oldFields});
      expect(wrapper.state().dragColumns.sortFieldList).to.be.eql([{name: 'name2'}]);
    });
  });

  describe('#handleDragEnd', () => {
    it('should reorder columns for field - swap second and first items', () => {
      wrapper.setState({ dragIndex: 0, hoverIndex: 1 });
      instance.handleDragEnd('sortFieldList', {name: 'name'}, {index: 1});
      expect(instance.props.sortFieldList.removeField).to.be.calledWith(0);
      expect(instance.props.sortFieldList.addField)
        .to.be.calledWith({ name: 'name'}, 1);
      expect(wrapper.state('dragIndex')).to.equal(-1);
      expect(wrapper.state('hoverIndex')).to.equal(-1);
    });

    it('should append dropped item to list and remove it from old position', () => {
      wrapper.setState({ dragIndex: 3, hoverIndex: 1 });
      instance.handleDragEnd('sortFieldList', {name: 'name'}, { index: 1});
      expect(instance.props.sortFieldList.removeField).to.be.calledWith(3);
      expect(instance.props.sortFieldList.addField)
        .to.be.calledWith( { name: 'name'}, 1);
      expect(wrapper.state('dragIndex')).to.equal(-1);
      expect(wrapper.state('hoverIndex')).to.equal(-1);
    });
  });

  describe('#handleMoveColumn', () => {
    beforeEach(() => {
      wrapper.setState({
        dragColumns: {
          sortFieldList: [{name: '1'}, {name: '2'}, {name: '3'}]
        }
      });
    });

    it('should append second column at the first position when drag item moved', () => {
      instance.handleMoveColumn('sortFieldList', 1, 0);
      expect(wrapper.state().dragColumns.sortFieldList).to.be.eql([{name: '2'}, {name: '1'}, {name: '3'}]);
      expect(wrapper.state().hoverIndex).to.be.eql(0);
    });

    it('should append first column at the third position when drag item moved', () => {
      instance.handleMoveColumn('sortFieldList', 0, 2);
      expect(wrapper.state().dragColumns.sortFieldList).to.be.eql([{name: '2'}, {name: '3'}, {name: '1'}]);
      expect(wrapper.state().hoverIndex).to.be.eql(2);
    });
  });

  it('should render columns for drag area', () => {
    wrapper.setProps({
      sortFieldList: [
        { name: { value: 'A' } },
        { name: { value: 'B' } }
      ],
      currentCell: 'sort'
    });

    expect(wrapper.find(DragTarget)).to.have.length(2);
    expect(wrapper.find(DragSource)).to.have.length(2);
    expect(wrapper.find(Divider)).to.have.length(1);
  });
});
