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
import { Component, PropTypes } from 'react';
import { connect }   from 'react-redux';
import Immutable from 'immutable';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import SimpleButton from 'components/Buttons/SimpleButton';
import DragTarget from 'components/DragComponents/DragTarget';

import { reapplyDataset } from 'actions/explore/dataset/reapply';
import { setCurrentSql, setQueryContext } from 'actions/explore/view';

import { PALE_BLUE, EXPLORE_SQL_BUTTON_COLOR } from 'uiTheme/radium/colors.js';
import {  MARGIN_PANEL } from 'uiTheme/radium/sizes.js';
import { bodySmall } from 'uiTheme/radium/typography';
import { constructFullPath } from 'utils/pathUtils';
import { sqlEditorButton } from 'uiTheme/radium/buttons';

import DatasetsPanel from './DatasetsPanel';
import SqlToggle from './SqlToggle';
import SqlAutoComplete from './SqlAutoComplete';
import FunctionsHelpPanel from './FunctionsHelpPanel';

@pureRender
@Radium
export class SqlEditorController extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    sqlSize: PropTypes.number,
    sqlState: PropTypes.bool,
    type: PropTypes.string,
    dragType: PropTypes.string,

    //connected
    currentSql: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),

    // actions
    setCurrentSql: PropTypes.func,
    setQueryContext: PropTypes.func,
    reapplyDataset: PropTypes.func
  };

  constructor(props) {
    super(props);
    this.insertFunc = this.insertFunc.bind(this);
    this.insertFullPathAtCursor = this.insertFullPathAtCursor.bind(this);
    this.onDrop = this.onDrop.bind(this);
    this.state = {
      sqlState: true,
      funcHelpPanel: false,
      datasetsPanel: false,
      dropDataset: '',
      dropFunc: '',
      isError: false
    };
    this.receiveProps(this.props, {});
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  componentDidUpdate(prevProps) {
    // refocus the SQL editor if you click New Query again
    // but otherwise do not yank focus due to other changes
    if (this.props.dataset.get('isNewQuery') &&
      this.props.currentSql === undefined &&
      (!prevProps.dataset.get('isNewQuery') || this.props.exploreViewState !== prevProps.exploreViewState)
    ) {
      this.refs.editor.focus();
    }
  }

  onDrop(dropEvent) {
    const { args, id } = dropEvent;
    if (args) {
      this.refs.editor.insertFunction(id, false, args);
    } else {
      this.refs.editor.insertFullPathAtDrop(id);
    }
  }

  receiveProps(nextProps, oldProps) {
    const { dataset } = oldProps;
    const nextDataset = nextProps.dataset;

    // Sql editor needs to update sql on dataset load, or new query.
    // Normally this is picked up when defaultValue changes in CodeMirror.js. However there is an edge case for
    // new query => new query. In this case, defaultValue1 == defaultValue2 == '', So need to detect it here, when
    // currentSql is reset to undefined.
    if (nextProps.currentSql === undefined && oldProps.currentSql !== undefined && this.refs.editor) {
      this.refs.editor.resetValue();
    }
    if ((dataset && constructFullPath(dataset.get('context'))) !== constructFullPath(nextDataset.get('context'))) {
      nextProps.setQueryContext({ context: nextDataset.get('context') });
    }
  }

  shouldSqlBoxBeGrayedOut() {
    const { exploreViewState, dataset } = this.props;
    return Boolean(exploreViewState.get('isInProgress') ||
      // disable when initial load failed
      (exploreViewState.get('isFailed') && !dataset.get('datasetVersion') && !dataset.get('isNewQuery')));
  }

  insertFullPathAtCursor(id) {
    this.refs.editor.insertFullPathAtCursor(id);
  }

  insertFunc(functionName) {
    this.refs.editor.insertFunction(functionName, true);
  }

  toggleDatasetPanel = () => this.setState({
    datasetsPanel: !this.state.datasetsPanel,
    funcHelpPanel: false
  });

  toggleFunctionsHelpPanel = () => this.setState({
    funcHelpPanel: !this.state.funcHelpPanel,
    datasetsPanel: false
  });

  handleSqlChange = (sql) => this.props.setCurrentSql({ sql });

  handleContextChange = (context) => this.props.setQueryContext({ context });

  handleEditOriginal = () => {
    this.props.reapplyDataset(this.props.dataset, null, null, true);
  };

  renderEditOriginalButton() {
    const {dataset, sqlState} = this.props;
    if (sqlState && dataset.get('canReapply')) {
      return (
        <SimpleButton
          type='button'
          buttonStyle='secondary'
          style={{...sqlEditorButton, lineHeight: '24px'}}
          onClick={this.handleEditOriginal}
        >
          {la('Edit Original SQL')}
        </SimpleButton>
      );
    }
  }

  renderSqlBlocks() {
    const isActiveFuncs = this.state.funcHelpPanel;
    const isActiveDatasets = this.state.datasetsPanel;
    if (this.props.sqlState) {
      return (
        <div className='sql-btn' style={[styles.btn]}>
          <button
            style={[styles.helpers, bodySmall, isActiveDatasets ? styles.helpersHovered : {}]}
            onClick={this.toggleDatasetPanel}
            key='datasets'>
            {la('Datasets')}
          </button>
          <DatasetsPanel
            dataset={this.props.dataset}
            height={this.props.sqlSize - MARGIN_PANEL}
            isVisible={this.state.datasetsPanel}
            dragType={this.props.dragType}
            viewState={this.props.exploreViewState}
            addFullPathToSqlEditor={this.insertFullPathAtCursor}/>
          <button
            style={[styles.helpers, bodySmall, isActiveFuncs ? styles.helpersHovered : {}]}
            onClick={this.toggleFunctionsHelpPanel}
            key='functions'>
            {la('Functions')}
          </button>
          <FunctionsHelpPanel
            height={this.props.sqlSize - MARGIN_PANEL}
            isVisible={this.state.funcHelpPanel}
            dragType={this.props.dragType}
            addFuncToSqlEditor={this.insertFunc}/>
        </div>
      );
    }
  }

  render() {
    // Keep SqlAutoComplete in the DOM even when hidden to maintain any SQL changes user has made
    const sqlStyle = this.props.sqlState ? {} : {height: 0, overflow: 'hidden'};

    const sqlBlock = (
      <div style={[styles.sqlAutoCompleteWrap, sqlStyle]}>
        <SqlAutoComplete
          dataset={this.props.dataset}
          type={this.props.type}
          isGrayed={this.shouldSqlBoxBeGrayedOut()}
          context={this.props.queryContext}
          changeQueryContext={this.handleContextChange}
          ref='editor'
          onChange={this.handleSqlChange}
          defaultValue={this.props.dataset.get('sql')}
          sqlSize={this.props.sqlSize}
          datasetsPanel={this.state.datasetsPanel}
          funcHelpPanel={this.state.funcHelpPanel}
        />
      </div>
    );
    const toggleButton = this.props.sqlState
      ? (
        <SqlToggle dataset={this.props.dataset} sqlState={this.props.sqlState}/>
      )
      : <div />;
    return (
      <div style={{width: '100%'}}>
        <div
          className='sql-part'
          onClick={this.hideDropDown}
          style={[styles.base, {height: this.props.sqlSize}]}>
          <div className='sql-control' style={[styles.sqlControls]}>
            <div style={{display: 'flex'}}>
              {toggleButton}
              {this.renderEditOriginalButton()}
            </div>
            {this.renderSqlBlocks()}
          </div>
          <DragTarget dragType={this.props.dragType} onDrop={this.onDrop}>
            {sqlBlock}
          </DragTarget>
        </div>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    currentSql: state.explore.view.get('currentSql'),
    queryContext: state.explore.view.get('queryContext')
  };
}

export default connect(mapStateToProps, {
  setCurrentSql,
  setQueryContext,
  reapplyDataset
})(SqlEditorController);

const styles = {
  base: {
    paddingBottom: 0,
    minHeight: 171,
    position: 'relative'
  },
  btn: {
    display: 'flex',
    alignItems: 'center',
    marginRight: 10
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
    height: 42,
    padding: 0,
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    width: '100%',
    backgroundColor: PALE_BLUE
  },
  sqlAutoCompleteWrap: {
    backgroundColor: PALE_BLUE,
    padding: '0 40px 0 0'
  }
};
