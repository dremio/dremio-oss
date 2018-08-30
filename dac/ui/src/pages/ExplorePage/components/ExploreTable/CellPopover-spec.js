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
import CellPopover from './CellPopover';

describe('CellPopover', () => {
  let minimalProps;
  let commonProps;
  let context;
  let wrapper;
  let instance;

  beforeEach(() => {
    minimalProps = {
      data: '{"inner_str":"a string","inner_map":{"a":5},"inner_list":[1,2,3,4,5]}',
      cellPopover: Immutable.fromJS({ columnType: 'MAP', columnName: 'revenue' })
    };
    commonProps = {
      ...minimalProps,
      location: {
        query: {},
        pathname: 'space/Prod-Sample.ds1'
      }
    };
    context = { router: { push: sinon.spy() } };
    wrapper = shallow(<CellPopover {...commonProps}/>, { context });
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<CellPopover {...minimalProps}/>, {context});
    expect(wrapper).to.have.length(1);
    expect(wrapper.hasClass('cell-popover-wrap')).to.eql(true);
    expect(wrapper.find('JSONTree')).to.have.length(1);
    expect(wrapper.find('withLocation(SelectedTextPopoverView)')).to.have.length(1);
  });

  describe('getMapModel', () => {
    it('should return { columnName, mapPathList }', () => {
      expect(instance.getMapModel(['inner_map', 'inner_map_v2'])).to.eql(Immutable.fromJS({
        columnName: commonProps.cellPopover.get('columnName'),
        mapPathList: ['inner_map_v2', 'inner_map']
      }));
    });

    it('should reduce list of path and return [inner_map[0], inner_map_v2] if nested node is item of LIST', () => {
      expect(instance.getMapModel(['inner_map_v2', 0, 'inner_map'])).to.eql(Immutable.fromJS({
        columnName: commonProps.cellPopover.get('columnName'),
        mapPathList: ['inner_map[0]', 'inner_map_v2']
      }));
    });

    it('should reduce list of path and return [inner_map[0][1], inner_map_v2, inner_list[0]] if nested node is item of LIST', () => {
      expect(instance.getMapModel([0, 'inner_list', 'inner_map_v2', 1, 0, 'inner_map'])).to.eql(Immutable.fromJS({
        columnName: commonProps.cellPopover.get('columnName'),
        mapPathList: ['inner_map[0][1]', 'inner_map_v2', 'inner_list[0]']
      }));
    });
  });

  describe('getListModel', () => {
    it('should return inner_list', () => {
      const data = '["inner_list", ["the","day"]]';
      const cellPopover = Immutable.fromJS({ columnType: 'LIST', columnName: 'revenue' });
      const inst = shallow(<CellPopover data={data} cellPopover={cellPopover}/>, {context}).instance();
      expect(inst.getListModel([0])).to.eql(Immutable.fromJS({
        cellText: data,
        columnName: 'revenue',
        startIndex: 0,
        endIndex: 1,
        listLength: 2
      }));
    });

    it('should return length and offset for first item of list like "a"', () => {
      const data = '["a", ["the","day"]]';
      const cellPopover = Immutable.fromJS({ columnType: 'LIST', columnName: 'revenue' });
      const inst = shallow(<CellPopover data={data} cellPopover={cellPopover}/>, {context}).instance();
      expect(inst.getListModel([0])).to.eql(Immutable.fromJS({
        cellText: data,
        columnName: 'revenue',
        startIndex: 0,
        endIndex: 1,
        listLength: 2
      }));
    });

    it('should return length and offset for first item of list like 1', () => {
      const data = '[1, ["the","day"]]';
      const cellPopover = Immutable.fromJS({ columnType: 'LIST', columnName: 'revenue' });
      const inst = shallow(<CellPopover data={data} cellPopover={cellPopover}/>, {context}).instance();
      expect(inst.getListModel([0])).to.eql(Immutable.fromJS({
        cellText: data,
        columnName: 'revenue',
        startIndex: 0,
        endIndex: 1,
        listLength: 2
      }));
    });

    it('should return length and offset for last item of list like "c"', () => {
      const data = '["a", "b", "c"]';
      const cellPopover = Immutable.fromJS({ columnType: 'LIST', columnName: 'revenue' });
      const inst = shallow(<CellPopover data={data} cellPopover={cellPopover}/>, {context}).instance();
      expect(inst.getListModel([2])).to.eql(Immutable.fromJS({
        cellText: data,
        columnName: 'revenue',
        startIndex: 2,
        endIndex: 3,
        listLength: 3
      }));
    });

    it('should return length and offset for last item of list like ["ds123"]', () => {
      const data = '["a", "b", ["ds123"]]';
      const cellPopover = Immutable.fromJS({ columnType: 'LIST', columnName: 'revenue' });
      const inst = shallow(<CellPopover data={data} cellPopover={cellPopover}/>, {context}).instance();
      expect(inst.getListModel([2])).to.eql(Immutable.fromJS({
        cellText: data,
        columnName: 'revenue',
        startIndex: 2,
        endIndex: 3,
        listLength: 3
      }));
    });

    it('should return length and offset for second item of list like "b"', () => {
      const data = '["a", "b", ["ds123"]]';
      const cellPopover = Immutable.fromJS({ columnType: 'LIST', columnName: 'revenue' });
      const inst = shallow(<CellPopover data={data} cellPopover={cellPopover}/>, {context}).instance();
      expect(inst.getListModel([1])).to.eql(Immutable.fromJS({
        cellText: data,
        columnName: 'revenue',
        startIndex: 1,
        endIndex: 2,
        listLength: 3
      }));
    });

    it('should return length and offset for second item of list like 2', () => {
      const data = '[1, 2, ["ds123"]]';
      const cellPopover = Immutable.fromJS({ columnType: 'LIST', columnName: 'revenue' });
      const inst = shallow(<CellPopover data={data} cellPopover={cellPopover}/>, {context}).instance();
      expect(inst.getListModel([1])).to.eql(Immutable.fromJS({
        cellText: data,
        columnName: 'revenue',
        startIndex: 1,
        endIndex: 2,
        listLength: 3
      }));
    });
  });

  describe('getMapValueFromSelection', () => {
    it('should string key with value based on location.state.selection', () => {
      wrapper.setState({
        keyPath: ['inner_str']
      });
      expect(instance.getMapValueFromSelection()).to.eql(
        'inner_str:"a string"'
      );

      wrapper.setState({
        keyPath: ['inner_map']
      });
      expect(instance.getMapValueFromSelection()).to.eql(
        'inner_map:{"a":5}'
      );

      wrapper.setState({
        keyPath: ['inner_list', '0']
      });
      expect(instance.getMapValueFromSelection()).to.eql(
        '1'
      );
    });
  });

  describe('selectItem', () => {
    beforeEach(() => {
      sinon.stub(instance, 'getListModel').returns('getListModel');
      sinon.stub(instance, 'getMapModel').returns('getMapModel');
    });

    it('should not show overlay if tagName = DIV', () => {
      const event = {target: global.document.createElement('div')};
      instance.selectItem(event);
      expect(context.router.push.called).to.be.false;
    });

    it('should select item with correct type', () => {
      const event = { target: global.document.createElement('span') };

      instance.selectItem(event, ['inner_map']);
      expect(context.router.push.calledWith({
        state: { selection: 'getMapModel' },
        query: { column: 'revenue', columnType: 'MAP' },
        pathname: 'space/Prod-Sample.ds1'
      })).to.be.true;

      wrapper.setProps({ cellPopover: Immutable.fromJS({ columnType: 'LIST', columnName: 'age' }) });
      instance.selectItem(event, ['inner_list']);
      expect(context.router.push.calledWith({
        state: { selection: 'getListModel' },
        query: { column: 'age', columnType: 'LIST' },
        pathname: 'space/Prod-Sample.ds1'
      })).to.be.true;
    });

  });

});
