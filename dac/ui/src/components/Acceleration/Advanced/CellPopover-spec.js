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
import CellPopover, { ColumnReorder } from './CellPopover';

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
      sortFields: []
    };
    commonProps.sortFields.swapFields = sinon.spy();
    commonProps.sortFields.addField = sinon.spy();
    commonProps.sortFields.removeField = sinon.spy();
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
      wrapper.setProps({ currentCell: {labelCell: 'sort'} });
      expect(instance.renderSortMenu).to.have.been.called;
    });
  });

  describe('#componentWillMount', () => {
    it('should set appropriate drag field list to state', () => {
      const sortFields = [{name: {value: 'name'}}];
      const w = shallow(<CellPopover {...commonProps} sortFields={sortFields} />);
      expect(w.state().dragColumns.sortFields).to.be.eql([{name: 'name'}]);
    });
  });

  describe('#receiveProps', () => {
    it('should update fields in state when old props are empty', () => {
      const sortFields = [{name: {value: 'name'}}];
      instance.receiveProps({sortFields});
      expect(wrapper.state().dragColumns.sortFields).to.be.eql([{name: 'name'}]);
    });

    it('should update fields in state fields were changed', () => {
      const oldFields = [{name: {value: 'name'}}];
      const newFields = [{name: {value: 'name2'}}];
      instance.receiveProps({sortFields: newFields}, {sortFields: oldFields});
      expect(wrapper.state().dragColumns.sortFields).to.be.eql([{name: 'name2'}]);
    });
  });

  describe('#handleDragEnd', () => {
    it('should reorder columns for field - swap second and first items', () => {
      wrapper.setState({ dragIndex: 0, hoverIndex: 1 });
      instance.handleDragEnd('sortFields', {name: 'name'}, {index: 1});
      expect(instance.props.sortFields.removeField).to.be.calledWith(0);
      expect(instance.props.sortFields.addField)
        .to.be.calledWith({ name: 'name'}, 1);
      expect(wrapper.state('dragIndex')).to.equal(-1);
      expect(wrapper.state('hoverIndex')).to.equal(-1);
    });

    it('should append dropped item to list and remove it from old position', () => {
      wrapper.setState({ dragIndex: 3, hoverIndex: 1 });
      instance.handleDragEnd('sortFields', {name: 'name'}, { index: 1});
      expect(instance.props.sortFields.removeField).to.be.calledWith(3);
      expect(instance.props.sortFields.addField)
        .to.be.calledWith( { name: 'name'}, 1);
      expect(wrapper.state('dragIndex')).to.equal(-1);
      expect(wrapper.state('hoverIndex')).to.equal(-1);
    });
  });

  describe('#handleMoveColumn', () => {
    beforeEach(() => {
      wrapper.setState({
        dragColumns: {
          sortFields: [{name: '1'}, {name: '2'}, {name: '3'}]
        }
      });
    });

    it('should append second column at the first position when drag item moved', () => {
      instance.handleMoveColumn('sortFields', 1, 0);
      expect(wrapper.state().dragColumns.sortFields).to.be.eql([{name: '2'}, {name: '1'}, {name: '3'}]);
      expect(wrapper.state().hoverIndex).to.be.eql(0);
    });

    it('should append first column at the third position when drag item moved', () => {
      instance.handleMoveColumn('sortFields', 0, 2);
      expect(wrapper.state().dragColumns.sortFields).to.be.eql([{name: '2'}, {name: '3'}, {name: '1'}]);
      expect(wrapper.state().hoverIndex).to.be.eql(2);
    });
  });

  it('should render ColumnReorder with columns', () => {
    wrapper.setProps({
      sortFields: [
        { name: { value: 'A' } },
        { name: { value: 'B' } }
      ],
      currentCell: {labelCell: 'sort'}
    });

    const cols = wrapper.find(ColumnReorder);

    expect(cols.props().columns).to.eql([
      { name: 'A' },
      { name: 'B' }
    ]);
  });
});
