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

import json from 'utils/mappers/mocks/gridMapper/expected.json';

import exploreUtils from 'utils/explore/exploreUtils';
import { EXPLORE_VIEW_ID } from 'reducers/resources/view';
import { getContext } from 'containers/dremioLocation';
import { ExploreTableController } from './ExploreTableController';

const location = {
  pathname: 'foo',
  query: {
    version: '123456',
    mode: 'edit'
  }
};
const routeParams = {
  tableId: 'newTable',
  resourceId: 'newSpace',
  resources: 'space'
};

describe('ExploreTableController', () => {

  let commonProps;
  let context;
  let wrapper;
  let instance;
  beforeEach(() => {
    commonProps = {
      pageType: 'default',
      dragType: 'join',
      history: Immutable.fromJS([]),
      width: 500,
      height: 500,
      updateRightTreeVisibility: sinon.spy(),
      dataset: Immutable.fromJS({
        fullPath: ['dspath'],
        datasetVersion: '123456',
        displayFullPath: Immutable.List(['fullpath'])
      }),
      tableData: Immutable.fromJS(json),
      tableViewData: Immutable.Map(),
      fullCell: Immutable.Map(),
      exploreViewState: Immutable.fromJS({viewId: EXPLORE_VIEW_ID}),
      resetViewState: sinon.spy(),
      updateColumnName: sinon.spy(),
      transformHistoryCheck: sinon.spy(),
      performTransform: sinon.spy(),
      createNewDatasetAndTransformIfReview: sinon.spy(),
      hideTransformWarningModal: sinon.spy(),
      accessEntity: sinon.spy(),
      location,
      routeParams
    };
    context = {
      router : {
        push: sinon.spy()
      },
      routeParams
    };
    wrapper = shallow(<ExploreTableController {...commonProps}/>, {context});
    instance = wrapper.instance();
  });

  describe('ExploreTableController specs', () => {
    describe('makeTransform', () => {
      it('should dispatch transformHistoryCheck', () => {
        const data = {
          type: 'DROP',
          columnName: 'l_orderkey'
        };
        wrapper.instance().makeTransform(data);
        expect(commonProps.transformHistoryCheck.calledOnce).to.equal(true);
        // trigger callback
        commonProps.transformHistoryCheck.args[0][1]();

        expect(commonProps.performTransform.calledOnce).to.equal(true);
        const args = commonProps.performTransform.args[0][0];
        expect(args.currentSql).to.equal(commonProps.currentSql);
        expect(args.queryContext).to.equal(commonProps.queryContext);
        expect(args.viewId).to.equal(commonProps.exploreViewState.get('viewId'));
      });
    });
  });

  describe('constructor', () => {
    it('should set initial state activeTextSelect', () => {
      expect(wrapper.state('activeTextSelect')).to.be.null;
    });
    it('should set initial state openPopover', () => {
      expect(wrapper.state('openPopover')).to.equal(false);
    });
    it('should set initial state activeCell', () => {
      expect(wrapper.state('activeCell')).to.equal(null);
    });
  });

  describe('preconfirmTransform', () => {
    it('should resolve if already confirmed', () => {
      instance.transformPreconfirmed = true;
      const promise = instance.preconfirmTransform();
      expect(commonProps.transformHistoryCheck).to.not.be.called;
      return expect(promise).to.be.resolved;
    });

    it('should call transformHistoryCheck if not confirmed', () => {
      const confirmCallback = sinon.spy();
      const promise = instance.preconfirmTransform(confirmCallback);
      expect(commonProps.transformHistoryCheck).to.be.called;
      expect(promise).to.not.be.resolved;
    });

    it('should reset transformPreconfirmed when table version changes', () => {
      instance.transformPreconfirmed = true;
      instance.componentWillReceiveProps(commonProps);
      expect(instance.transformPreconfirmed).to.be.true;
      wrapper.setProps({tableData: commonProps.tableData.set('version', 'newVersion')});
      expect(instance.transformPreconfirmed).to.be.false;
    });
  });

  describe('isSqlChanged', () => {
    it('should return false due to empty nextProps dataset', () => {
      expect(wrapper.instance().isSqlChanged({nextProps: {dataset: null}})).to.be.false;
    });
    it('should return false due to empty props dataset', () => {
      wrapper = shallow(<ExploreTableController {...commonProps} dataset={null} />, {context});
      expect(wrapper.instance().isSqlChanged({nextProps: {dataset: {}}})).to.be.false;
    });
    it('should return false for equal next & current props', () => {
      const nextProps = {dataset: Immutable.fromJS({sql: 'sql'}), currentSql: 'sql'};
      expect(wrapper.instance().isSqlChanged(nextProps)).to.be.false;
    });
    it('should return true for different next & current props', () => {
      const nextProps = {dataset: Immutable.fromJS({sql: 'sql2'}), currentSql: 'sql1'};
      expect(wrapper.instance().isSqlChanged(nextProps)).to.be.true;
    });
  });

  describe('openDetailsWizard', () => {
    beforeEach(() => {
      instance.openDetailsWizard({columnName: 'revenue'});
    });

    it('should call transformHistoryCheck', () => {
      expect(commonProps.performTransform).to.be.calledOnce;

      //call callback
      commonProps.performTransform.args[0][0].callback();
      expect(context.router.push).to.be.calledWith({
        ...location,
        pathname: `${location.pathname}/details`,
        query: {...location.query, type: undefined},
        state: { columnName: 'revenue', columnType: 'FLOAT', previewVersion: '', toType: null, props: undefined }
      });
    });
  });

  describe('updateColumnName', () => {
    beforeEach(() => {
      sinon.stub(instance, 'makeTransform');
    });
    afterEach(() => {
      instance.makeTransform.restore();
    });
    it('makeTransform should not be called', () => {
      instance.updateColumnName('n1', {target: {value: 'n1'}});
      expect(instance.makeTransform.called).to.be.false;
    });
    it('makeTransform should be called', () => {
      instance.updateColumnName('n1', {target: {value: 'n2'}});
      expect(instance.makeTransform.calledOnce).to.be.true;
    });
  });

  describe('hideCellMore', () => {
    it('should remove activeCell state', () => {
      instance.setState({'activeCell': { anchor: document.createElement('span') }});
      instance.hideCellMore();
      expect(wrapper.state('activeCell')).to.be.null;
    });
  });

  describe('hideDrop', () => {
    it('should update state activeTextSelect', () => {
      instance.setState({'activeTextSelect': {}});
      instance.hideDrop();
      expect(wrapper.state('activeTextSelect')).to.be.null;
    });
    it('should update state openPopover', () => {
      instance.setState({'openPopover': true});
      instance.hideDrop();
      expect(wrapper.state('openPopover')).to.be.false;
    });
  });

  describe('componentWillReceiveProps', () => {
    beforeEach(() => {
      sinon.stub(instance, 'isContextChanged').returns(false);
    });
    it('should unset isGrayed when isSqlChanged = false', () => {
      instance.setState({'isGrayed': true});
      instance.componentWillReceiveProps(commonProps);
      expect(wrapper.state('isGrayed')).to.be.false;
    });
    it('should set isGrayed when isSqlChanged = true', () => {
      sinon.stub(instance, 'isSqlChanged').returns(true);

      instance.setState({'isGrayed': true});
      instance.componentWillReceiveProps(commonProps);
      expect(wrapper.state('isGrayed')).to.be.true;
    });
    it('should call props.accessEntity only when tableData version changed and is not undefined', () => {
      instance.componentWillReceiveProps(commonProps);
      expect(commonProps.accessEntity).to.not.be.called;

      instance.componentWillReceiveProps({...commonProps, tableData: commonProps.tableData.delete('version')});
      expect(commonProps.accessEntity).to.not.be.called;

      const newVersion = 'newVersion';
      instance.componentWillReceiveProps({
        ...commonProps,
        tableData: commonProps.tableData.set('version', newVersion)
      });
      expect(commonProps.accessEntity).to.not.be.calledWith(['tableData', newVersion]);
    });
  });

  describe('selectAll', () => {
    const position = { left: 1 };
    beforeEach(() => {
      sinon.stub(exploreUtils, 'selectAll').returns(position);
      sinon.spy(instance, 'handleCellTextSelect');
    });
    afterEach(() => {
      exploreUtils.selectAll.restore();
    });
    it('should properly handle selected content', () => {
      const elem = 'span';
      const columnType = 'INTEGER';
      const columnName = 'int_col';
      const cellText = '1';
      const cellValue = 1;
      const model = {
        cellText,
        offset: 0,
        columnName,
        length: cellText.length
      };

      instance.selectAll(elem, columnType, columnName, cellText, cellValue);
      expect(context.router.push).to.be.calledWith({
        ...location,
        state: { columnName, columnType, hasSelection: true, selection: Immutable.fromJS(model) }
      });
      expect(instance.handleCellTextSelect).calledWith({ ...position, columnType });
    });
    it('should properly handle selected content if content of a cell is null', () => {
      const elem = 'span';
      const columnType = 'BOOL';
      const columnName = 'bool_col';
      const cellText = 'null';
      const cellValue = null;
      const model = {
        cellText: null,
        offset: 0,
        columnName,
        length: 0
      };

      instance.selectAll(elem, columnType, columnName, cellText, cellValue);
      expect(context.router.push).to.be.calledWith({
        ...location,
        state: { columnName, columnType, hasSelection: true, selection: Immutable.fromJS(model) }
      });
      expect(instance.handleCellTextSelect).calledWith({ ...position, columnType });
    });
  });

  describe('handleCellTextSelect', () => {
    it('update activeTextSelect', () => {
      const activeTextSelect = {dropPositions: 12};
      instance.handleCellTextSelect(activeTextSelect);
      expect(wrapper.state('activeTextSelect')).to.equal(activeTextSelect);
    });
    it('state.openPopover should be true', () => {
      instance.setState({'openPopover': false});
      instance.handleCellTextSelect({dropPositions: 12});
      expect(wrapper.state('openPopover')).to.equal(true);
    });
  });

  describe('handleCellShowMore', () => {
    it('activeCell should be set', () => {
      const anchor = document.createElement('span');
      instance.handleCellShowMore('cellValue', anchor, 'columnType', 'columnName', 'valueUrl');
      expect(wrapper.state('activeCell')).to.eql({
        cellValue: 'cellValue',
        anchor,
        columnType: 'columnType',
        columnName: 'columnName',
        valueUrl: 'valueUrl',
        isTruncatedValue: false
      });
    });
  });

  describe('ExploreCellLargeOverlay', () => {
    it('should render ExploreCellLargeOverlay', () => {
      instance.setState({activeCell: {
        cellValue: '',
        anchor: document.createElement('span'),
        columnType: 't1',
        columnName: 'col1',
        valueUrl: ''
      }});
      const ExploreCellLargeOverlay = shallow(instance.renderExploreCellLargeOverlay(), { context: getContext({}) });
      expect(ExploreCellLargeOverlay).to.be.exist;
    });
  });
});
