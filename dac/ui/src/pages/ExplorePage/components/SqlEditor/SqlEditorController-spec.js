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
import sinon from 'sinon';
import Immutable from 'immutable';

import SqlToggle from 'pages/ExplorePage/components/SqlEditor/SqlToggle';
import SqlAutoComplete from 'pages/ExplorePage/components/SqlEditor/SqlAutoComplete';
import SimpleButton from 'components/Buttons/SimpleButton';

import { SqlEditorController } from './SqlEditorController';

const location = {
  query: {
    version: '1234'
  }
};

const routeParams = {
  tableId: 'newTable',
  resourceId: 'newSpace',
  resources: 'space'
};

describe('SqlEditorController', () => {
  let commonProps;
  let wrapper;
  let instance;
  let context;
  beforeEach(() => {
    commonProps = {
      dataset: Immutable.fromJS({context: ['SELECT'], sql: 'sql', canReapply: true}), // todo
      exploreViewState: Immutable.fromJS({}),
      updateSqlPartSize: sinon.spy(),
      sqlSize: 300,
      dragType: 'help-func',
      sqlState: true,
      setCurrentSql: sinon.spy(),
      setQueryContext: sinon.spy(),
      toggleSqlError: sinon.spy(),
      queryContext: Immutable.List(),
      getDatasetChangeDetails: () => ({}),
      location
    };
    context = {
      router : {
        push: sinon.spy()
      },
      location,
      routeParams
    };
    wrapper = shallow(<SqlEditorController {...commonProps}/>, {context});
    instance = wrapper.instance();
  });

  describe('SqlEditorController specs', () => {
    it('should render and wrapped in DragTarget', () => {
      expect(wrapper.find(SqlAutoComplete)).to.have.length(1);
    });

    describe('constructor', () => {
      const initialState = {
        funcHelpPanel: false,
        datasetsPanel: false
      };
      Object.keys(initialState).forEach((key) => {
        it(`should set initial state ${key}`, () => {
          expect(wrapper.state(key)).to.deep.equal(initialState[key]);
        });
      });
    });

    describe('#receiveProps', () => {
      beforeEach(() => {
        instance.refs = {
          editor: {
            resetValue: sinon.spy(),
            focus: sinon.spy()
          }
        };
      });
      it('should call editor.resetValue if currentSql changes to null', () => {

        instance.receiveProps({...commonProps, currentSql: 'some sql'}, {});
        expect(instance.refs.editor.resetValue).to.not.be.called;

        instance.receiveProps({...commonProps, currentSql: 'some sql'}, {...commonProps, currentSql: 'different sql'});
        expect(instance.refs.editor.resetValue).to.not.be.called;

        instance.receiveProps({...commonProps, currentSql: null}, {...commonProps, currentSql: 'some sql'});
        expect(instance.refs.editor.resetValue).to.be.called;
      });

      it('should call setQueryContext if old props are empty', () => {
        commonProps.setQueryContext.reset();
        instance.receiveProps(commonProps, {});
        expect(commonProps.setQueryContext).to.have.been.called;
      });

      it('should call setQueryContext if dataset is the same', () => {
        // this is called in constructor, so need to reset
        commonProps.setQueryContext.reset();
        instance.receiveProps(commonProps, commonProps);
        expect(commonProps.setQueryContext).to.not.have.been.called;
      });
    });

    describe('#componentDidUpdate()', () => {
      beforeEach(() => {
        instance.refs = {
          editor: {
            focus: sinon.spy(),
            resetValue: sinon.spy()
          }
        };
      });

      it('should focus editor when sql unchanged and dataset changed to isNewQuery', () => {
        instance.componentDidUpdate(commonProps);
        expect(instance.refs.editor.focus).to.not.be.called;

        wrapper.setProps({dataset: commonProps.dataset.set('isNewQuery', true), currentSql: 'foo'});
        instance.componentDidUpdate(commonProps);
        expect(instance.refs.editor.focus).to.not.be.called;

        wrapper.setProps({currentSql: null});
        instance.componentDidUpdate(commonProps);
        expect(instance.refs.editor.focus).to.be.called;
      });

      it('should focus editor when sql unchanged, isNewQuery and exploreViewState changed', () => {
        instance.componentDidUpdate(commonProps);
        expect(instance.refs.editor.focus).to.not.be.called;
        const newQueryDataset = commonProps.dataset.set('isNewQuery', true);

        wrapper.setProps({dataset: newQueryDataset, currentSql: 'foo'});
        instance.componentDidUpdate({...commonProps,
          dataset: newQueryDataset,
          exploreViewState: Immutable.Map({isFailed: true})});
        expect(instance.refs.editor.focus).to.not.be.called;

        wrapper.setProps({currentSql: null});
        instance.componentDidUpdate({...commonProps,
          dataset: newQueryDataset,
          exploreViewState: Immutable.Map({isFailed: true})});
        expect(instance.refs.editor.focus).to.be.called;
      });
    });

    describe('#shouldSqlBoxBeGrayedOut', () => {
      it('should return true if isInProgress', () => {
        expect(instance.shouldSqlBoxBeGrayedOut()).to.be.false;
        wrapper.setProps({exploreViewState: Immutable.Map({isInProgress: true})});
        expect(instance.shouldSqlBoxBeGrayedOut()).to.be.true;
      });

      it('should return true if isFailed and !datasetVersion and !isNewQuery', () => {
        expect(instance.shouldSqlBoxBeGrayedOut()).to.be.false;
        wrapper.setProps({exploreViewState: Immutable.Map({isFailed: true})});
        expect(instance.shouldSqlBoxBeGrayedOut()).to.be.true;
        wrapper.setProps({dataset: commonProps.dataset.set('isNewQuery', true)});
        expect(instance.shouldSqlBoxBeGrayedOut()).to.be.false;
        wrapper.setProps({dataset: commonProps.dataset.merge({datasetVersion: 'abc123', isNewQuery: false})});
        expect(instance.shouldSqlBoxBeGrayedOut()).to.be.false;
      });
    });

    describe('toggleDatasetPanel', () => {
      it('should toggle dataset panel state', () => {
        instance.toggleDatasetPanel();
        expect(instance.state.datasetsPanel).to.be.true;
        expect(instance.state.funcHelpPanel).to.be.false;
      });
    });

    describe('toggleFunctionsHelpPanel', () => {
      it('should toggle function help panel state', () => {
        instance.toggleFunctionsHelpPanel();
        expect(instance.state.datasetsPanel).to.be.false;
        expect(instance.state.funcHelpPanel).to.be.true;
      });
    });

    describe('SqlToggle', () => {
      it('should show SqlToggle when sql expanded or collapsed', () => {
        expect(wrapper.find(SqlToggle)).to.have.length(1);
      });
      it('should not hide SqlToggle when dataset panel is expanded and funcHelpPanel is collapsed', () => {
        wrapper.setState({datasetsPanel: true});
        expect(wrapper.find(SqlToggle)).to.have.length(1);
      });
      it('should not hide SqlToggle when funcHelpPanel is expanded and dataset panel is collapsed', () => {
        wrapper.setState({funcHelpPanel: true});
        expect(wrapper.find(SqlToggle)).to.have.length(1);
      });
    });

    describe('renderEditOriginalButton', () => {
      it('should hide edit button when sql is collapsed or dataset is not original', () => {
        expect(wrapper.find('.sql-control').childAt(0).find(SimpleButton)).to.have.length(1);
        wrapper.setProps({sqlState: false});
        expect(wrapper.find('.sql-control').childAt(0).find(SimpleButton)).to.have.length(0);
        wrapper.setProps({sqlState: true});
        expect(wrapper.find('.sql-control').childAt(0).find(SimpleButton)).to.have.length(1);
        wrapper.setProps({ dataset: commonProps.dataset.merge({canReapply: false})});
        expect(wrapper.find('.sql-control').childAt(0).find(SimpleButton)).to.have.length(0);
      });
    });

    describe('renderSqlBlocks', () => {
      it('should hide sql blocks when sql is collapsed', () => {
        expect(wrapper.find('.sql-btn')).to.have.length(1);
        wrapper.setProps({sqlState: false});
        expect(wrapper.find('.sql-btn')).to.have.length(0);
      });
    });
  });
});
