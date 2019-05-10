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

import exploreUtils from 'utils/explore/exploreUtils';
import { ExploreTableCellView as ExploreTableCell } from './ExploreTableCell';

describe('ExploreTableCell', () => {

  let minimalProps;
  let commonProps;
  let context;
  const getCellParentStub = () => ({
    className: 'cell-wrap',
    getAttribute: () => 'col1'
  });
  beforeEach(() => {
    minimalProps = {
      columnType: 'TEXT',
      onSelect: sinon.spy(),
      onShowMore: sinon.spy(),
      setRecommendationInfo: sinon.spy(),
      selectAll: sinon.spy(),
      selectItemsOfList: sinon.spy(),
      loadNextRows: sinon.spy(),
      rowIndex: 0,
      columnKey: 0,
      data: Immutable.fromJS([])
    };

    commonProps = {
      ...minimalProps,
      columnType: 'INTEGER',
      columnName: 'colName',
      columnStatus: 'ORIGINAL',
      data: Immutable.fromJS([{
        row: [{
          v: 23,
          type: 'INTEGER'
        }]
      }]),
      columns: Immutable.fromJS([{
        name: 'col1',
        status: 'ORIGINAL'
      }]),
      onCellTextSelect: sinon.spy(),
      preventNextRowsChunkLoad: false,
      isNextRowsChunkLoading: false
    };

    context = {
      router: { push: sinon.spy() }
    };
  });

  describe('#render', function() {

    it('should render with minimal props without exploding', () => {
      const wrapper = shallow(<ExploreTableCell {...minimalProps}/>);
      expect(wrapper).to.have.length(1);
    });

    it('should render cell data', () => {
      const wrapper = shallow(<ExploreTableCell {...commonProps}/>);
      expect(wrapper.find('.cell-wrap').text()).to.eql('23');
    });

    it('should render blank if no cell data', () => {
      const data = commonProps.data.deleteIn([0, 'row', 0, 'v']);
      const wrapper = shallow(<ExploreTableCell {...commonProps} data={data}/>);
      expect(wrapper.find('.cell-wrap').text()).to.eql('null');
    });

    it('should render properly if empty string data', () => {

      const emptyStringProps = {
        ...commonProps,
        columnType: 'TEXT',
        data: Immutable.fromJS([{
          row: [{
            v: '',
            type: 'TEXT'
          }]
        }])
      };

      const wrapper = shallow(<ExploreTableCell {...emptyStringProps}/>);
      expect(wrapper.find('.cell-wrap').text()).to.eql('empty text');
    });
  });

  describe('#shouldComponentUpdate', function() {
    let instance;
    beforeEach(() => {
      instance = shallow(<ExploreTableCell {...commonProps} />).instance();
    });
    describe('should return false', function() {
      it('with no actual prop changes', () => {
        expect(instance.shouldComponentUpdate(instance.props, instance.state)).to.be.false;
      });
      it('with same state and same rowIndex, columnKey, columnStatus, data', () => {
        expect(instance.shouldComponentUpdate(
          {
            rowIndex: instance.props.rowIndex,
            columnKey: instance.props.columnKey,
            columnStatus: instance.props.columnStatus,
            columnType: instance.props.columnType,
            data: instance.props.data
          },
          instance.state
        )).to.be.false;
      });
      it('with same state and same rowIndex, columnKey, columnStatus and equitable data', () => {
        expect(instance.shouldComponentUpdate(
          {
            rowIndex: instance.props.rowIndex,
            columnKey: instance.props.columnKey,
            columnStatus: instance.props.columnStatus,
            columnType: instance.props.columnType,
            data: Immutable.fromJS([{
              row: [{
                v: 23,
                type: 'INTEGER'
              }]
            }])
          },
          instance.state
        )).to.be.false;
      });
      it('with different state and same rowIndex, columnKey, columnStatus, data', () => {
        expect(instance.shouldComponentUpdate(
          instance.props,
          {
            ...instance.state
          }
        )).to.be.false;
      });
    });

    describe('should return true', function() {
      it('with different rowIndex', () => {
        expect(instance.shouldComponentUpdate(
          {
            ...instance.props,
            rowIndex: instance.props.rowIndex + 1
          },
          instance.state
        )).to.be.true;
      });
      it('with different columnKey', () => {
        expect(instance.shouldComponentUpdate(
          {
            ...instance.props,
            columnKey: 'foo'
          },
          instance.state
        )).to.be.true;
      });
      it('with different columnStatus', () => {
        expect(instance.shouldComponentUpdate(
          {
            ...instance.props,
            columnStatus: 'HIGHLIGHTED'
          },
          instance.state
        )).to.be.true;
      });
      it('with different data', () => {
        expect(instance.shouldComponentUpdate(
          {
            ...instance.props,
            data: new Immutable.List()
          },
          instance.state
        )).to.be.true;
      });
      it('with different state', () => {
        expect(instance.shouldComponentUpdate(
          instance.props,
          {
            ...instance.state,
            foo: 1
          }
        )).to.be.true;
      });
    });
  });

  describe('#prohibitSelection', function() {
    const selectionData = {
      text: 'text',
      oRange: {
        startContainer: {
          parentNode: getCellParentStub()
        }
      }
    };

    beforeEach(function() {
      global.$ = sinon.spy(() => {
        return {
          closest: sinon.spy(() => true)
        };
      });
    });

    it('should return true when isDumbTable = true', function() {
      commonProps.isDumbTable = true;
      commonProps.location = { query: {} };
      const instance = shallow(<ExploreTableCell {...commonProps}/>).instance();
      expect(instance.prohibitSelection({})).to.be.true;
    });

    it('should return true when selectionData = null', function() {
      commonProps.isDumbTable = false;
      commonProps.location = { query: {} };
      const instance = shallow(<ExploreTableCell {...commonProps}/>).instance();
      expect(instance.prohibitSelection(null)).to.be.true;
    });

    it('should return true when selectionData.text = null', function() {
      commonProps.isDumbTable = false;
      commonProps.location = { query: {} };
      const instance = shallow(<ExploreTableCell {...commonProps}/>).instance();
      expect(instance.prohibitSelection({ text: null })).to.be.true;
    });

    it('should return true when column does not exist', function() {
      commonProps.isDumbTable = false;
      commonProps.location = { query: {} };
      const instance = shallow(
        <ExploreTableCell {...commonProps} columns={Immutable.fromJS([])}/>
      ).instance();
      expect(instance.prohibitSelection(selectionData)).to.be.true;
    });

    it('should return false when query.type undefined', function() {
      commonProps.isDumbTable = false;
      commonProps.columns = Immutable.fromJS([{
        name: 'col1',
        status: 'ORIGINAL'
      }]);
      commonProps.location = { query: {} };
      const instance = shallow(<ExploreTableCell {...commonProps}/>).instance();
      expect(instance.prohibitSelection(selectionData)).to.be.false;
    });

    it('should return false when columnStatus is not "HIGHLIGHTED"', function() {
      commonProps.isDumbTable = false;
      commonProps.location = { query: { type: 'transform'} };
      const instance = shallow(<ExploreTableCell {...commonProps}/>).instance();
      expect(instance.prohibitSelection(selectionData)).to.be.false;
    });

    it('should return true when columnStatus is "HIGHLIGHTED"', function() {
      commonProps.isDumbTable = false;
      commonProps.columns = commonProps.columns.setIn([0, 'status'], 'HIGHLIGHTED');
      commonProps.location = { query: { type: 'transform'} };
      const instance = shallow(<ExploreTableCell {...commonProps}/>).instance();
      expect(instance.prohibitSelection(selectionData)).to.be.true;
    });
  });

  describe('#onMouseUp', () => {
    let wrapper;
    let instance;
    let selectionData;
    let props;
    beforeEach(() => {
      props = {
        ...commonProps,
        location: {state: {}},
        columns: Immutable.fromJS([{name: 'col1', type: 'LIST'}])
      };
      wrapper = shallow(<ExploreTableCell {...props}/>, {context});
      instance = wrapper.instance();
      selectionData = {
        oRange: {
          startContainer: {
            data: 'column_text',
            parentNode: getCellParentStub()
          }
        }
      };
      sinon.stub(instance, 'prohibitSelection').returns(false);
      sinon.stub(exploreUtils, 'getSelectionData').returns(selectionData);
      sinon.stub(exploreUtils, 'getSelection').returns({model: 'selectionModel'});
    });

    afterEach(() => {
      exploreUtils.getSelectionData.restore();
      exploreUtils.getSelection.restore();
    });

    it('should return null when isDumbTable', () => {
      wrapper.setProps({isDumbTable: true});
      expect(instance.onMouseUp()).to.be.null;
    });

    it('should return null when prohibitSelection returns true', () => {
      instance.prohibitSelection.returns(true);
      wrapper.setProps({
        location: {state: {}}
      });
      expect(instance.onMouseUp()).to.be.null;
    });

    it('should call selectItemsOfList from props when selected column type is LIST', () => {
      instance.onMouseUp();
      expect(props.selectItemsOfList).to.be.calledWith('column_text', 'col1', 'LIST', selectionData);
      expect(context.router.push).to.not.be.called;
    });

    it('should update location state with appropriate data when columnType is TEXT', () => {
      const expectedLocation = {
        ...props.location,
        state: {
          columnName: 'col1',
          columnType: 'TEXT',
          hasSelection: true,
          selection: 'selectionModel'
        }
      };
      wrapper.setProps({
        columns: props.columns.setIn([0, 'type'], 'TEXT')
      });
      instance.onMouseUp();
      expect(props.selectItemsOfList).to.not.be.called;
      expect(context.router.push).to.be.calledWith(expectedLocation);
      expect(props.onCellTextSelect).to.be.called;
    });

    it('should do nothing when columnType is MAP', () => {
      wrapper.setProps({
        columns: props.columns.setIn([0, 'type'], 'MAP')
      });
      instance.onMouseUp();
      expect(props.selectItemsOfList).to.not.be.called;
      expect(props.selectAll).to.not.be.called;
      expect(context.router.push).to.not.be.called;
    });

    it('should call selectAll for other column types (not TEXT, MAP, LIST)', () => {
      wrapper.setProps({
        columns: props.columns.setIn([0, 'type'], 'NUMBER')
      });
      instance.onMouseUp();
      expect(props.selectAll).to.be.calledWith(selectionData.parentElement, 'NUMBER', 'col1', 'column_text');
      expect(props.selectItemsOfList).to.not.be.called;
      expect(context.router.push).to.not.be.called;
    });
  });
});
