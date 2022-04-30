/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { PureComponent } from 'react';
import { connect } from 'react-redux';
import Immutable from 'immutable';
import Radium from 'radium';

import PropTypes from 'prop-types';

import { getExploreState } from '@app/selectors/explore';

import { editOriginalSql } from 'actions/explore/dataset/reapply';
import { setCurrentSql, setQueryContext } from 'actions/explore/view';

import { PALE_BLUE, EXPLORE_SQL_BUTTON_COLOR } from 'uiTheme/radium/colors.js';
import { constructFullPath } from 'utils/pathUtils';
import { replace } from 'react-router-redux';
import { showUnsavedChangesConfirmDialog } from '@app/actions/confirmation';

import { compose } from 'redux';
import { getActiveScript } from '@app/selectors/scripts';
import SqlAutoComplete from './SqlAutoComplete';
import FunctionsHelpPanel from './FunctionsHelpPanel';

const toolbarHeight = 42;
@Radium
export class SqlEditorController extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    datasetSummary: PropTypes.object,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    sqlSize: PropTypes.number,
    sqlState: PropTypes.bool,
    type: PropTypes.string,
    dragType: PropTypes.string,
    handleSidebarCollapse: PropTypes.func,
    sidebarCollapsed: PropTypes.bool,
    children: PropTypes.any,

    //connected by redux connect
    currentSql: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),
    focusKey: PropTypes.number,
    activeScript: PropTypes.object,
    //---------------------------

    // actions
    setCurrentSql: PropTypes.func,
    setQueryContext: PropTypes.func,
    editOriginalSql: PropTypes.func,
    replaceUrlAction: PropTypes.func,
    showUnsavedChangesConfirmDialog: PropTypes.func
  };

  sqlEditorControllerRef = null;

  constructor(props) {
    super(props);
    this.insertFunc = this.insertFunc.bind(this);
    this.insertFullPathAtCursor = this.insertFullPathAtCursor.bind(this);
    this.toggleFunctionsHelpPanel = this.toggleFunctionsHelpPanel.bind(this);
    this.state = {
      funcHelpPanel: false,
      datasetsPanel: !!(props.dataset && props.dataset.get('isNewQuery'))
    };
    this.receiveProps(this.props, {});
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  componentDidUpdate(prevProps) {
    const { activeScript } = this.props;
    const isNewQueryClick = this.props.dataset.get('isNewQuery')
      && this.props.currentSql === null
      && (!prevProps.dataset.get('isNewQuery') || this.props.exploreViewState !== prevProps.exploreViewState);
    // refocus the SQL editor if you click New Query again
    // but otherwise do not yank focus due to other changes
    if (isNewQueryClick || this.props.focusKey && prevProps.focusKey !== this.props.focusKey) { //focus input is focus key is changed
      this.sqlEditorControllerRef.focus();
    }
    // open right datasets panel after new query click
    if (isNewQueryClick) { //TODO: is there a cleaner way?
      this.setState({datasetsPanel: true}); // eslint-disable-line react/no-did-update-set-state
    }

    const controller = this.getMonacoEditor();
    if (activeScript.id && prevProps.activeScript.id !== activeScript.id && controller) {
      this.props.setCurrentSql({ sql: activeScript.content });
      controller.setValue(activeScript.content);

      this.props.setQueryContext({ context: Immutable.fromJS(activeScript.context) });
    }

    if (activeScript.id === undefined && prevProps.activeScript.id && controller) {
      setCurrentSql({ sql: '' });
      controller.setValue('');
    }

    if (prevProps.currentSql !== '' && this.props.currentSql === '' && controller) {
      // setValue('') makes the editor's lineNumber value null
      // when using SqlAutoComplete's insertAtRanges function, it breaks since lineNumber is null
      const range = controller.getModel().getFullModelRange();
      controller.setSelection(range);
      controller.executeEdits('dremio', [{
        identifier: 'dremio-reset',
        range: controller.getSelection(),
        text: ''
      }]);
    }
  }

  receiveProps(nextProps, oldProps) {
    const { dataset } = oldProps;
    const nextDataset = nextProps.dataset;

    // Sql editor needs to update sql on dataset load, or new query.
    // Normally this is picked up when defaultValue changes in CodeMirror.js. However there is an edge case for
    // new query => new query. In this case, defaultValue1 == defaultValue2 == '', So need to detect it here, when
    // currentSql is reset to null.
    if (nextProps.currentSql === null && oldProps.currentSql !== null && this.sqlEditorControllerRef) {
      this.sqlEditorControllerRef.resetValue();
    }
    if ((dataset && constructFullPath(dataset.get('context'))) !== constructFullPath(nextDataset.get('context'))) {
      nextProps.setQueryContext({ context: nextDataset.get('context') });
      //if context was changed, put cursor back to an editor.
      // This case has place also in case of new query
      if (this.sqlEditorControllerRef) {
        this.sqlEditorControllerRef.focus();
      }
    }
  }

  shouldSqlBoxBeGrayedOut() {
    const { exploreViewState, dataset } = this.props;
    return Boolean(exploreViewState.get('isInProgress') ||
      // disable when initial load failed
      (exploreViewState.get('isFailed') && !dataset.get('datasetVersion') && !dataset.get('isNewQuery')));
  }

  getMonacoEditor() {
    const editorRef = this.sqlEditorControllerRef;
    return editorRef && editorRef.getMonacoEditorInstance && editorRef.getMonacoEditorInstance();
  }

  insertFullPathAtCursor(id) {
    this.sqlEditorControllerRef.insertFullPath(id);
  }

  insertFunc(functionName, args) {
    this.sqlEditorControllerRef.insertFunction(functionName, args);
  }

  toggleDatasetPanel = () => this.setState({
    datasetsPanel: !this.state.datasetsPanel,
    funcHelpPanel: false
  });

  toggleFunctionsHelpPanel() {
    this.setState({
      funcHelpPanel: !this.state.funcHelpPanel,
      datasetsPanel: false
    });
  }

  handleSqlChange = (sql) => {
    this.props.setCurrentSql({ sql });
  }

  handleContextChange = (context) => {
    this.props.setQueryContext({ context });
  }

  onAutoCompleteEnabledChanged = () => {
    this.setState(state => {
      return {
        autoCompleteEnabled: !state.autoCompleteEnabled
      };
    });
  }

  renderSqlBlocks() {
    return (
      <div className='sql-btn' style={[styles.btn]}>
        <FunctionsHelpPanel
          height={this.props.sqlSize}
          isVisible={this.state.funcHelpPanel}
          dragType={this.props.dragType}
          handleSidebarCollapse={this.props.handleSidebarCollapse}
          addFuncToSqlEditor={this.insertFunc}/>
      </div>
    );
  }

  render() {
    let errors;
    if (this.props.exploreViewState.getIn(['error', 'message', 'code']) === 'INVALID_QUERY') {
      errors = this.props.exploreViewState.getIn(['error', 'message', 'details', 'errors']);
    }

    const sqlBlock = (
      <SqlAutoComplete
        dataset={this.props.dataset}
        type={this.props.type}
        isGrayed={this.shouldSqlBoxBeGrayedOut()}
        context={this.props.queryContext}
        changeQueryContext={this.handleContextChange}
        ref={(ref) => this.sqlEditorControllerRef = ref}
        onChange={this.handleSqlChange}
        onFunctionChange={this.toggleFunctionsHelpPanel.bind(this)}
        defaultValue={this.props.dataset.get('sql')}
        sqlSize={this.props.sqlSize - toolbarHeight}
        sidebarCollapsed={this.props.sidebarCollapsed}
        datasetsPanel={this.state.datasetsPanel}
        funcHelpPanel={this.state.funcHelpPanel}
        dragType={this.props.dragType}
        errors={errors}
        autoCompleteEnabled={false}
      >
        {this.props.children}
      </SqlAutoComplete>
    );
    return (
      <div style={{width: '100%'}}>
        <div
          className='sql-part'
          onClick={this.hideDropDown}
          style={[styles.base]}>
          <div className='sql-functions'>
            {this.renderSqlBlocks()}
          </div>
          <div>
            {sqlBlock}
          </div>
        </div>
      </div>
    );
  }
}

function mapStateToProps(state) {
  const explorePageState = getExploreState(state);
  return {
    currentSql: explorePageState.view.currentSql,
    queryContext: explorePageState.view.queryContext,
    focusKey: explorePageState.view.sqlEditorFocusKey,
    datasetSummary: state.resources.entities.get('datasetSummary'),
    activeScript: getActiveScript(state)
  };
}

export default compose(
  connect(mapStateToProps, {
    setCurrentSql,
    setQueryContext,
    editOriginalSql,
    replaceUrlAction: replace,
    showUnsavedChangesConfirmDialog
  }, null, { forwardRef: true })
)(SqlEditorController);

const styles = {
  base: {
    paddingBottom: 0,
    position: 'relative'
  },
  btn: {
    display: 'flex',
    alignItems: 'center',
    marginRight: 10,
    position: 'relative'
  },
  helpers: {
    width: 64,
    height: 24,
    border: 'none',
    borderRadius: 2,
    backgroundColor: 'transparent',
    margin: '0 5px',
    outline: 'none',
    ':hover': {
      backgroundColor: EXPLORE_SQL_BUTTON_COLOR
    }
  },
  helpersHovered: {
    backgroundColor: EXPLORE_SQL_BUTTON_COLOR
  },
  sqlControls: {
    height: toolbarHeight,
    padding: 0,
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    width: '100%',
    backgroundColor: PALE_BLUE
  }
};
