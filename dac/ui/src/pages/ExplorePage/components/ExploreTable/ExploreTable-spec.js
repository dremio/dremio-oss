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
import { Table } from 'fixed-data-table-2';
import ReactDOM from 'react-dom';

import json from '../../../../utils/mappers/mocks/gridMapper/expected.json';
import { MIN_COLUMN_WIDTH } from '../../../../uiTheme/radium/sizes';

import { ExploreTableView as ExploreTable, RIGHT_TREE_OFFSET } from './ExploreTable';

describe('ExploreTable', () => {

  let commonProps, context, wrapper, instance;
  beforeEach(() => {
    commonProps = {
      dragType: 'default',
      rows: Immutable.fromJS(json.rows),
      columns: Immutable.fromJS(json.columns),
      tableViewData: Immutable.Map(),
      paginationUrl: 'paginationUrl',
      exploreViewState: Immutable.fromJS({}),
      cardsViewState: Immutable.fromJS({}),
      toggleDropdown: () => {},
      updateColumnName: () => {},
      makeTransform: () => {},
      preconfirmTransform: () => {},
      handleColumnResizeEnd: sinon.spy(),
      openDetailsWizard: () => {},
      loadNextRows: sinon.spy(),
      dataset: Immutable.fromJS({
        datasetVersion: 1,
        fullPath: ['newSpace', 'newTable']
      }),
      location: {
        query: {
          version: '123456',
          mode: 'edit'
        }
      }
    };
    context = {
      router : {push: sinon.spy()},
      location: {
        query: {
          version: '123456',
          mode: 'edit'
        }
      }
    };

    wrapper = shallow(<ExploreTable {...commonProps}/>, {context});
    instance = wrapper.instance();
    sinon.stub(instance, 'debouncedUpdateSize');
  });

  it('should render .fixed-data-table', () => {
    expect(wrapper.hasClass('fixed-data-table')).to.equal(true);
  });

  it('we should see spinner when we dont have data and when we inProgress state', () => {
    const props = {
      ...commonProps,
      columns: Immutable.fromJS([]),
      rows: Immutable.fromJS([]),
      exploreViewState: Immutable.fromJS({isInProgress: true})
    };
    wrapper = shallow(<ExploreTable {...props}/>, {context});
    expect(wrapper.find('Connect(ViewStateWrapper)').props().viewState).to.eql(props.exploreViewState);
  });

  it('we should not see spinner when we have data and when we not have inProgress state', () => {
    const props = {
      ...commonProps,
      columns: Immutable.fromJS([]),
      rows: Immutable.fromJS([]),
      exploreViewState: Immutable.fromJS({isInProgress: false})
    };
    wrapper = shallow(<ExploreTable {...props}/>, {context});
    expect(wrapper.find('Connect(ViewStateWrapper)').props().viewState).to.eql(props.exploreViewState);
  });

  it.skip('we should not see spinner when we have data and when we have inProgress state', () => {
    const props = {
      ...commonProps,
      columns: Immutable.fromJS([{name: 'col', index: 0, type: 'TEXT'}]),
      rows: Immutable.fromJS([]),
      exploreViewState: Immutable.fromJS({isInProgress: true})
    };
    shallow(<ExploreTable {...props}/>, {context});
  });

  //todo: find a way to find Table inside an AutoSizer component to test it
  it.skip('should render columns', () => {
    wrapper = shallow(<ExploreTable {...commonProps}/>, {context});
    expect(wrapper.find(Table)).to.have.length(1);
    const children = wrapper.find(Table).children();
    const firstColumn = children.at(0).node.key;
    const secondColumn = children.at(1).node.key;
    const thirdColumn = children.at(2).node.key;
    const fourthColumn = children.at(3).node.key;
    expect(children.length).to.equal(4);
    expect(firstColumn.split('/')[0]).to.equal('l_orderkey');
    expect(secondColumn.split('/')[0]).to.equal('revenue');
    expect(thirdColumn.split('/')[0]).to.equal('o_orderdate');
    expect(fourthColumn.split('/')[0]).to.equal('o_shippriority');
  });

  describe('getCellStyle', () => {
    it('should return cell style', () => {
      expect(ExploreTable.getCellStyle({ color: 'COLOR' })).to.eql({
        width: '100%',
        display: 'inline-block',
        backgroundColor: 'COLOR'
      });
    });
  });

  describe('getCellWidth', () => {
    it('should return column width', () => {
      expect(ExploreTable.getCellWidth({ width: 100 }, 200)).to.eql(100);
    });
    it('should return default width', () => {
      expect(ExploreTable.getCellWidth({}, 200)).to.eql(200);
    });
  });

  describe('tableWidth', () => {
    it('should return width if defined', () => {
      expect(ExploreTable.tableWidth(100, {}, 2, false)).to.equal(200);
    });
    it('should return offsetWidth if width is not defined', () => {
      expect(ExploreTable.tableWidth(null, { offsetWidth: 200}, 1, false)).to.equal(200);
    });
    it('should calculate width if right tree is shown', () => {
      expect(ExploreTable.tableWidth(null, { offsetWidth: 300}, 1, true)).to.equal(300 - RIGHT_TREE_OFFSET);
    });
  });

  describe('tableHeight', () => {
    it('should return height if defined', () => {
      expect(ExploreTable.tableHeight(100, {}, null)).to.equal(100);
    });
    it('should calculate height', () => {
      expect(ExploreTable.tableHeight(null, { offsetHeight: 200}, 100)).to.equal(100);
    });
  });

  describe('getDefaultColumnWidth', () => {
    const widthThatWillBeSet = 200,
      columns = [{width: 10}, {width: 20}, {width: null}];

    it('should calculate column width', () => {
      expect(ExploreTable.getDefaultColumnWidth(300, columns)).to.equal(254);
    });

    it('should return MIN_COLUMN_WIDTH if calculated is less than MIN_COLUMN_WIDTH', () => {
      expect(ExploreTable.getDefaultColumnWidth(widthThatWillBeSet, columns)).to.equal(MIN_COLUMN_WIDTH);
    });
  });

  describe('getTableSize', () => {
    let res;
    const data = {
      widthScale: 2,
      width: 100,
      height: 200,
      maxHeight: 300,
      rightTreeVisible: false
    };
    const customWrappers = [ {}, {} ];
    const columns = [];
    const nodeOffsetTop = 10;

    beforeEach(() => {
      sinon.spy(ExploreTable, 'tableWidth');
      sinon.spy(ExploreTable, 'tableHeight');
      sinon.spy(ExploreTable, 'getDefaultColumnWidth');
      res = ExploreTable.getTableSize(data, customWrappers, columns, nodeOffsetTop);
    });

    afterEach(() => {
      ExploreTable.tableWidth.restore();
      ExploreTable.tableHeight.restore();
      ExploreTable.getDefaultColumnWidth.restore();
    });

    it('should run tableWidth with proper params', () => {
      expect(ExploreTable.tableWidth.calledWith(100, customWrappers[0], 2, false));
    });
    it('should run tableHeight with proper params', () => {
      expect(ExploreTable.tableHeight.calledWith(200, customWrappers[1], 10));
    });
    it('should run getDefaultColumnWidth with proper params', () => {
      expect(ExploreTable.getDefaultColumnWidth.calledWith(100, columns));
    });
    it('should return object with size properties', () => {
      expect(res).to.eql(Immutable.Map({ width: 200, height: 200, maxHeight: 300, defaultColumnWidth: 200 }));
    });
  });

  describe('constructor', () => {
    it('should set defaultColumnWidth to 0', () => {
      expect(wrapper.state('defaultColumnWidth')).to.equal(0);
    });

    it('should set size properties to 0', () => {
      expect(wrapper.state('size')).to.eql(Immutable.Map({ width: 0, height: 0, defaultColumnWidth: 0 }));
    });
  });

  describe('componentWillReceiveProps', () => {
    beforeEach(() => {
      sinon.stub(instance, 'updateColumns');
      sinon.stub(instance, 'needUpdateColumns').returns(false);
    });
    it('should not update columns when column data is not changed', () => {
      instance.componentWillReceiveProps(commonProps);
      expect(instance.updateColumns).to.have.not.been.called;
    });
    it('should update columns when columns changed', () => {
      instance.needUpdateColumns.restore();
      sinon.stub(instance, 'needUpdateColumns').returns(true);
      instance.componentWillReceiveProps(commonProps);
      expect(instance.updateColumns).to.have.been.called;
    });
  });

  describe('componentDidUpdate', () => {
    beforeEach(() => {
      sinon.stub(instance, 'updateSize');
    });

    afterEach(() => {
      instance.updateSize.restore();
    });

    it('should run updateSize id properties were changed', () => {
      const prevProps = { p: 1, dataset: Immutable.fromJS({
        fullPath: ['newSpace', 'newTable']
      })};
      instance.componentDidUpdate(prevProps);
      expect(instance.updateSize.calledOnce).to.be.true;
    });

    it('should reset lastLoaded', () => {
      const prevProps = { p: 1, dataset: Immutable.fromJS({
        fullPath: ['newSpace', 'newTable'],
        datasetVersion: 2
      })};
      instance.componentDidUpdate(prevProps);
      expect(instance.lastLoaded).to.equal(0);
    });
  });

  describe('loadNextRows', () => {
    beforeEach(() => {
      instance.lastLoaded = 0;
    });
    it('leave lastLoaded as is', () => {
      const offset = 0;
      const loadNew = false;
      instance.loadNextRows(offset, loadNew);
      expect(instance.lastLoaded).to.equal(0);
    });
    it('do not run loadNextRows', () => {
      const offset = 0;
      const loadNew = false;
      instance.loadNextRows(offset, loadNew);
      expect(instance.props.loadNextRows.called).to.be.false;
    });

    it('should update lastLoaded', () => {
      const offset = 1;
      const loadNew = false;
      instance.loadNextRows(offset, loadNew);
      expect(instance.lastLoaded).to.equal(1);
    });

    it('do not run loadNextRows', () => {
      const offset = 1;
      const loadNew = true;
      instance.loadNextRows(offset, loadNew);
      expect(instance.props.loadNextRows.calledOnce).to.be.true;
    });
  });

  describe('handleColumnResizeEnd', () => {
    beforeEach(() => {
      sinon.stub(instance, 'renderColumns');
      sinon.stub(instance, 'hasHorizontalScroll');
      sinon.stub(instance, 'getScrollToColumn');
      sinon.stub(instance, 'updateSize');
    });
    it('should update column with given width by index and update table size', () => {
      instance.handleColumnResizeEnd(100, 0);
      expect(wrapper.state('columns').getIn([0, 'width'])).to.be.eql(100);
      expect(instance.updateSize).to.have.been.called;
    });
  });

  describe('needUpdateColumns', () => {
    it('should return false when there are no columns at all', () => {
      expect(instance.needUpdateColumns({ columns: Immutable.fromJS([])})).to.be.false;
    });
    it('should return true when no columns in state and new columns recieved', () => {
      instance.setState({ columns: Immutable.List()});
      expect(instance.needUpdateColumns(commonProps)).to.be.true;
    });
    it('should return false when same columns recieved', () => {
      instance.setState({ columns: commonProps.columns});
      expect(instance.needUpdateColumns(commonProps)).to.be.false;
    });
    it('should return true when hidden value changes', () => {
      instance.setState({ columns: commonProps.columns.setIn([0, 'hidden'], true)});
      expect(instance.needUpdateColumns(commonProps)).to.be.true;
    });
    it('should return true when different columns recieved', () => {
      const newProps = {
        ...commonProps,
        columns: commonProps.columns.delete(0)
      };
      expect(instance.needUpdateColumns(newProps)).to.be.true;
    });
  });

  describe('shouldShowNoData', () => {
    it('should return false if viewState.isInProgress', () => {
      expect(instance.shouldShowNoData(Immutable.Map({isInProgress: true}))).to.be.false;
    });

    it('should return false if viewState.isFailed', () => {
      expect(instance.shouldShowNoData(Immutable.Map({isInFailed: true}))).to.be.false;
    });

    it('should return false if dataset has no version', () => {
      wrapper.setProps({dataset: commonProps.dataset.set('datasetVersion', null)});
      expect(instance.shouldShowNoData(Immutable.Map())).to.be.false;
    });

    it('should return true if no rows', () => {
      wrapper.setProps({
        columns: Immutable.fromJS([]),
        rows: Immutable.fromJS([])
      });
      expect(instance.shouldShowNoData(Immutable.Map())).to.be.true;
    });
  });

  describe('#updateSize', () => {
    beforeEach(() => {
      const fakeEl = document.createElement('div');
      fakeEl.getClientRects = () => []; //  a fix after jquery update to v3. $.offset fails when tryes to call getClientRects. Previously jquery does not use this method

      sinon.stub(ReactDOM, 'findDOMNode').returns(fakeEl);
    });
    afterEach(() => {
      ReactDOM.findDOMNode.restore();
    });

    it('should not change state if size did not actually change', () => {
      sinon.stub(instance.state.size, 'equals').returns({
        equals() {
          return true;
        }
      });

      sinon.spy(instance, 'componentDidUpdate');
      instance.updateSize();
      expect(instance.componentDidUpdate).to.have.not.been.called;
    });
    it('should change state if size did actually change', () => {
      sinon.stub(instance.state.size, 'equals').returns({
        equals() {
          return false;
        }
      });

      sinon.spy(instance, 'componentDidUpdate');
      instance.updateSize();
      expect(instance.componentDidUpdate).to.have.not.been.called;
    });
  });
});
