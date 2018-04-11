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

import InnerJoin from './InnerJoin';

import { InnerJoinController } from './InnerJoinController';

describe('InnerJoinController', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      dataset: Immutable.fromJS({ displayFullPath: ['a', 'b'] }),
      leftColumns: Immutable.List(),
      rightColumns: Immutable.List(),
      fields: {
        activeDataset: { value: '' },
        joinType: { value: 'Left Outer' },
        columns: { onChange: sinon.spy() }
      }
    };
    commonProps = {
      ...minimalProps
    };
    wrapper = shallow(<InnerJoinController {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<InnerJoinController {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should renders InnerJoin', () => {
    expect(wrapper.find(InnerJoin)).to.have.length(1);
  });

  it('should set state.leftColumns and state.rightColumns on when receiving new columns prop', () => {
    expect(instance.state.leftColumns).to.eql(commonProps.leftColumns);
    expect(instance.state.rightColumns).to.eql(commonProps.rightColumns);
    const leftColumns = Immutable.fromJS([{name: 'column1'}, {name: 'column2'}]);
    const rightColumns = Immutable.fromJS([{name: 'column3'}, {name: 'column4'}]);
    wrapper.setProps({ leftColumns, rightColumns });
    expect(instance.state.leftColumns).to.eql(leftColumns);
    expect(instance.state.rightColumns).to.eql(rightColumns);
  });

  it('should set isDragInProgress=true and columnDragName=id when drag start', () => {
    instance.onDragStart('custom', {id: 'age'});
    expect(instance.state.isDragInProgress).to.eql(true);
    expect(instance.state.columnDragName).to.eql('age');
    expect(instance.state.dragColumntableType).to.eql('custom');
    expect(instance.state.type).to.eql('custom');
  });

  it('should on drop for custom column', () => {
    const newColumns = Immutable.fromJS([{name: 'a'}]);
    wrapper.setState({
      rightColumns: newColumns,
      type: 'custom'
    });
    instance.onDrop({id: 'a', type: 'custom'});
    expect(instance.state.isDragInProgress).to.eql(false);
    expect(instance.state.type).to.eql('');
    expect(instance.state.columnDragName).to.eql('');
    expect(commonProps.fields.columns.onChange.called).to.eql(true);
  });

  it('should drag stoped', () => {
    instance.stopDrag();
    expect(instance.state.isDragInProgress).to.eql(false);
    expect(instance.state.columnDragName).to.eql('');
    expect(instance.state.type).to.eql('');
  });

  it('should remove column from drag area', () => {
    wrapper.setState({
      columnsInDragArea: Immutable.fromJS([
        {
          id: 1,
          default: {name: 'column1'},
          custom: {name: 'column2'}
        },
        {
          id: 2,
          default: {name: 'column3'},
          custom: {name: 'column4'}
        }
      ])
    });
    instance.removeColumn(1);
    expect(commonProps.fields.columns.onChange.called).to.eql(true);
  });

  it('should add empty column', () => {
    instance.addEmptyColumnToInnerJoin();
    expect(commonProps.fields.columns.onChange.called).to.eql(true);
  });

  describe('getDefaultDragAreaColumns', () => {
    it('should return empty columns if leftColumn/rightColumn is empty array', () => {
      const props = {
        leftColumns: Immutable.List(),
        rightColumns: Immutable.List(),
        recommendation: {
          matchingKeys: {
            'name': 'name0'
          }
        }
      };

      let dragAreaColumns = InnerJoinController.getDefaultDragAreaColumns(props);
      dragAreaColumns = dragAreaColumns.deleteIn([0, 'id']);
      expect(dragAreaColumns.toJS()).to.eql([
        {
          custom: {
            empty: true
          },
          default: {
            empty: true
          }
        }
      ]);
    });

    it('should return recommended columns if set', () => {
      const props = {
        leftColumns: Immutable.fromJS([{name: 'name'}]),
        rightColumns: Immutable.fromJS([{name: 'name0'}]),
        recommendation: {
          matchingKeys: {
            'name': 'name0'
          }
        }
      };
      const dragAreaColumns = InnerJoinController.getDefaultDragAreaColumns(props);
      expect(dragAreaColumns.toJS()).to.have.length(1);
    });
  });
  describe('addColumnToInnerJoin', () => {
    it('should add column to columnsInDragArea', () => {
      const props = {
        ...minimalProps,
        leftColumns: Immutable.fromJS([{ name: 'name' }]),
        rightColumns: Immutable.fromJS([{ name: 'name0' }])
      };
      const wrap = shallow(<InnerJoinController {...props} />);
      const inst = wrap.instance();
      const column = {
        id: 'abc',
        'default': {
          name: 'name'
        },
        custom: {
          empty: true
        }
      };
      wrap.setState({
        columnsInDragArea: Immutable.fromJS([column])
      });
      inst.addColumnToInnerJoin({columnName: 'name0', dragColumnId: 'abc', dragColumnType: 'custom'});
      const expected = [{
        ...column,
        custom: {
          name: 'name0'
        },
        abc: 'namename0'
      }];
      expect(inst.state.columnsInDragArea.toJS()).to.eql(expected);
    });
  });
});
