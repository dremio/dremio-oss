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
import { PureComponent } from "react";
import { connect } from "react-redux";
import Immutable from "immutable";

import PropTypes from "prop-types";

import { getExploreState } from "#oss/selectors/explore";

import {
  setCurrentSql,
  modifyCurrentSql,
  setQueryContext,
  setUpdateSqlFromHistory,
  resetQueryState,
  setEditorContents,
} from "#oss/actions/explore/view";

import { constructFullPath } from "utils/pathUtils";
import { replace } from "react-router-redux";
import { showUnsavedChangesConfirmDialog } from "#oss/actions/confirmation";
import { addNotification } from "#oss/actions/notification";

import { compose } from "redux";
import { INITIAL_CALL_VALUE } from "#oss/components/SQLScripts/sqlScriptsUtils";
import { fetchScripts, setActiveScript } from "#oss/actions/resources/scripts";
import { getActiveScript } from "#oss/selectors/scripts";
import SqlAutoComplete from "./SqlAutoComplete";
import SQLFunctionsPanel from "./SQLFunctionsPanel";
import { memoOne } from "#oss/utils/memoUtils";
import { extractSqlErrorFromResponse } from "./utils/errorUtils";
import { getLocation } from "#oss/selectors/routing";
import { intl } from "#oss/utils/intl";
import { getTracingContext } from "dremio-ui-common/contexts/TracingContext.js";
import {
  withExtraSQLEditorContent,
  renderExtraSQLPanelComponent,
  EXTRA_SQL_TRACKING_EVENT,
} from "@inject/utils/sql-editor-extra";
import { selectTab } from "dremio-ui-common/sonar/SqlRunnerSession/resources/SqlRunnerSessionResource.js";
import { isScriptUrl, isTabbableUrl } from "#oss/utils/explorePageTypeUtils";
import { withColorScheme } from "dremio-ui-common/utilities/themeUtils.js";
const toolbarHeight = 41;

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
    editorWidth: PropTypes.any,
    colorScheme: PropTypes.string,

    //connected by redux connect
    currentSql: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),
    focusKey: PropTypes.number,
    activeScript: PropTypes.object,
    queryStatuses: PropTypes.array,
    querySelections: PropTypes.array,
    updateSqlFromHistory: PropTypes.bool,
    isMultiQueryRunning: PropTypes.bool,
    previousMultiSql: PropTypes.string,
    editorContents: PropTypes.string,
    isOpenResults: PropTypes.bool,
    scriptId: PropTypes.string,
    isFromDataGraph: PropTypes.bool,
    isMultiTabEnabled: PropTypes.bool,
    location: PropTypes.object,
    //---------------------------

    // actions
    addNotification: PropTypes.func,
    fetchSQLScripts: PropTypes.func,
    setActiveScript: PropTypes.func,
    setCurrentSql: PropTypes.func,
    setQueryContext: PropTypes.func,
    setUpdateSqlFromHistory: PropTypes.func,
    resetQueryState: PropTypes.func,
    replaceUrlAction: PropTypes.func,
    setEditorContents: PropTypes.func,
    showUnsavedChangesConfirmDialog: PropTypes.func,
    hasExtraSQLPanelContent: PropTypes.bool,
  };

  sqlEditorControllerRef = null;
  NO_SQL_ERRORS = [];

  constructor(props) {
    super(props);
    this.insertFunc = this.insertFunc.bind(this);
    this.insertFullPathAtCursor = this.insertFullPathAtCursor.bind(this);
    this.toggleFunctionsHelpPanel = this.toggleFunctionsHelpPanel.bind(this);
    this.state = {
      extraSqlPanel: false,
      funcHelpPanel: false,
      datasetsPanel: !!(props.dataset && props.dataset.get("isNewQuery")),
      script: {},
      defaultValue: "",
      shouldLoadHistory: false,
    };
    this.receiveProps(this.props, {});
  }

  componentDidMount() {
    const { isViewingHistory } = this.props;

    if (isViewingHistory) {
      this.setState({ shouldLoadHistory: true });
    }

    this.setState({
      defaultValue: this.getDefaultValue(),
    });
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  componentDidUpdate(prevProps) {
    const {
      activeScript,
      dataset,
      isMultiQueryRunning,
      setEditorContents,
      colorScheme,
    } = this.props;
    const { shouldLoadHistory } = this.state;
    const isNewQueryClick =
      this.props.dataset.get("isNewQuery") &&
      this.props.currentSql === null &&
      (!prevProps.dataset.get("isNewQuery") ||
        this.props.exploreViewState !== prevProps.exploreViewState);
    // refocus the SQL editor if you click New Query again
    // but otherwise do not yank focus due to other changes
    const lostFocus =
      this.props.focusKey && prevProps.focusKey !== this.props.focusKey;
    if ((isNewQueryClick || lostFocus) && !isMultiQueryRunning) {
      //focus input is focus key is changed
      this.sqlEditorControllerRef.focus();
    }
    // open right datasets panel after new query click
    if (isNewQueryClick) {
      //TODO: is there a cleaner way?
      this.setState({ datasetsPanel: true });
    }

    const controller = this.getMonacoEditor();
    if (activeScript.id && prevProps.activeScript.id !== activeScript.id) {
      this.props.setCurrentSql({ sql: activeScript.content });

      if (controller) {
        controller.setValue(activeScript.content);
      }

      this.props.setQueryContext({
        context: Immutable.fromJS(activeScript.context),
      });
    }

    if (
      prevProps.currentSql !== "" &&
      this.props.currentSql === "" &&
      controller
    ) {
      // setValue('') makes the editor's lineNumber value null
      // when using SqlAutoComplete's insertAtRanges function, it breaks since lineNumber is null
      const range = controller.getModel().getFullModelRange();
      controller.setSelection(range);
      controller.executeEdits("dremio", [
        {
          identifier: "dremio-reset",
          range: controller.getSelection(),
          text: "",
        },
      ]);
    }

    // Loads the dataset sql into the editor when opening history in a new tab
    if (shouldLoadHistory && dataset.get("sql") && controller) {
      this.setState({ shouldLoadHistory: false });
      setEditorContents({ content: dataset.get("sql") });
    }

    this.updateEditorContents();
  }

  receiveProps(nextProps, oldProps) {
    const {
      dataset,
      setUpdateSqlFromHistory: changeUpdateSqlFromHistory,
      isFromDataGraph: oldIsFromDataGraph,
    } = oldProps;
    const {
      dataset: nextDataset,
      updateSqlFromHistory: nextUpdateSqlFromHistory,
      isOpenResults,
      isFromDataGraph,
      isMultiTabEnabled,
      location,
    } = nextProps;

    const isOnScriptTab = isMultiTabEnabled && isScriptUrl(location);
    if (
      !isOnScriptTab &&
      (dataset && constructFullPath(dataset.get("context"))) !==
        constructFullPath(nextDataset.get("context"))
    ) {
      nextProps.setQueryContext({ context: nextDataset.get("context") });
      //if context was changed, put cursor back to an editor.
      // This case has place also in case of new query
      if (this.sqlEditorControllerRef) {
        this.sqlEditorControllerRef.focus();
      }
    }

    const controller = this.getMonacoEditor();
    if (
      controller &&
      (!oldProps.currentSql || nextUpdateSqlFromHistory) &&
      dataset?.get("sql") !== nextDataset.get("sql") &&
      nextDataset.get("sql") !== oldProps.currentSql &&
      nextProps.queryStatuses.length < 2 &&
      !isOpenResults
    ) {
      if (nextUpdateSqlFromHistory) {
        changeUpdateSqlFromHistory({ updateSql: false });
      }

      controller.setValue(nextDataset.get("sql"));
    }

    if (
      oldIsFromDataGraph == null &&
      isFromDataGraph &&
      nextProps.currentSql === ""
    ) {
      this.handleSqlChange(nextDataset.get("sql"));
    }
  }

  shouldSqlBoxBeGrayedOut() {
    const { exploreViewState, dataset, isMultiQueryRunning } = this.props;

    return Boolean(
      isMultiQueryRunning ||
        exploreViewState.get("isInProgress") ||
        (exploreViewState.get("isFailed") &&
          !dataset.get("datasetVersion") &&
          !dataset.get("isNewQuery")),
    );
  }

  getMonacoEditor() {
    const editorRef = this.sqlEditorControllerRef;
    return (
      editorRef &&
      editorRef.getMonacoEditorInstance &&
      editorRef.getMonacoEditorInstance()
    );
  }

  getMonaco() {
    return this.sqlEditorControllerRef?.getMonaco?.();
  }

  insertFullPathAtCursor(id) {
    this.sqlEditorControllerRef.insertFullPath(id);
  }

  insertFunc(functionName, args) {
    this.sqlEditorControllerRef.insertFunction(functionName, args);
  }

  toggleFunctionsHelpPanel() {
    this.setState({
      funcHelpPanel: !this.state.funcHelpPanel,
      datasetsPanel: false,
      extraSqlPanel: false,
    });
  }

  toggleExtraSQLPanel = () => {
    if (!this.state.extraSqlPanel) {
      getTracingContext().appEvent(EXTRA_SQL_TRACKING_EVENT);
    }

    this.setState({
      extraSqlPanel: !this.state.extraSqlPanel,
      funcHelpPanel: false,
      datasetsPanel: false,
    });
  };

  handleSqlChange = (sql) => {
    this.props.modifyCurrentSql({ sql });
  };

  handleContextChange = (context) => {
    this.props.setQueryContext({ context });
  };

  renderFunctionsSQLPanel() {
    return (
      <div className="sql-btn" style={styles.btn}>
        {this.state.funcHelpPanel && this.props.sqlState && (
          <SQLFunctionsPanel
            height={this.props.sqlSize + 8}
            isVisible={this.state.funcHelpPanel}
            dragType={this.props.dragType}
            handleSidebarCollapse={this.props.handleSidebarCollapse}
            addFuncToSqlEditor={this.insertFunc}
          />
        )}
      </div>
    );
  }

  renderExtraSQLPanel() {
    return (
      <div className="sql-btn" style={styles.btn}>
        {this.state.extraSqlPanel &&
          this.props.sqlState &&
          renderExtraSQLPanelComponent({
            height: this.props.sqlSize + 8,
            isVisible: this.state.extraSqlPanel,
            handleSidebarCollapse: this.props.handleSidebarCollapse,
            disableInsertion: this.shouldSqlBoxBeGrayedOut(),
            editorRef: this.getMonacoEditor(),
            monaco: this.getMonaco(),
          })}
      </div>
    );
  }

  getServerSqlErrors() {
    const {
      currentSql,
      isMultiQueryRunning,
      previousMultiSql,
      querySelections,
      queryStatuses,
    } = this.props;
    return this.getServerSqlErrorsMemoize(
      currentSql,
      isMultiQueryRunning,
      previousMultiSql,
      querySelections,
      queryStatuses,
    );
  }

  getServerSqlErrorsMemoize = memoOne(
    (
      currentSql,
      isMultiQueryRunning,
      previousMultiSql,
      querySelections,
      queryStatuses,
    ) => {
      const isNotEdited = !!previousMultiSql && currentSql === previousMultiSql;

      if (isNotEdited && !isMultiQueryRunning && queryStatuses.length) {
        const sqlErrors = [];

        queryStatuses.forEach((status, index) => {
          const error = status.error;

          if (error && querySelections[index]) {
            let errorResponse;

            if (error.get?.("response")) {
              errorResponse = error.get("response")?.payload?.response;

              // as part of the new query flow, errors come in a different property
            } else if (error.get?.("message")) {
              errorResponse = { errorMessage: error.get("message") };
              if (error.get("range")) {
                errorResponse.range = error.get("range")?.toJS?.();
              }

              // when a job is canceled, an error is returned in an object instead of an Immutable Map
            } else if (error?.payload) {
              errorResponse = error.payload?.response;

              const errorMessage = errorResponse?.errorMessage;

              // do not show canceled jobs as errors
              if (errorMessage?.includes("Query cancelled by user")) {
                return;
              }
            }

            sqlErrors.push(
              extractSqlErrorFromResponse(
                errorResponse,
                querySelections[index],
              ),
            );
          }
        });

        return sqlErrors;
      } else {
        return this.NO_SQL_ERRORS; // Use a static array to prevent downstream re-rending from new prop reference
      }
    },
  );

  getScriptValue = memoOne(async () => {
    const {
      activeScript,
      addNotification,
      fetchSQLScripts,
      scriptId,
      setActiveScript,
      setCurrentSql,
      setQueryContext,
      dataset,
      location,
      resetQueryState,
      isMultiTabEnabled,
    } = this.props;

    let script = {};
    if (scriptId && !Object.keys(activeScript).length) {
      const response = await fetchSQLScripts({
        maxResults: INITIAL_CALL_VALUE,
        searchTerm: null,
        createdBy: null,
      });
      const scriptList = response.payload?.data || [];
      script = scriptList.find((script) => script.id === scriptId) ?? {};
      this.setState({ script });
      setActiveScript({ script });
      setCurrentSql({ sql: script.content });
      setQueryContext({ context: Immutable.fromJS(script.context) });

      // if script does not exist, show error banner
      if (!Object.keys(script).length) {
        addNotification(
          intl.formatMessage({ id: "Script.Invalid" }, { scriptId }),
          "error",
          10,
        );
      } else {
        // Open the script in the sql tab bar
        selectTab(script.id);
      }
    } else {
      // Need to set for view/table page
      // openResults check prevents the explore page from resetting when opening job results
      if (!isTabbableUrl(location) && location.query?.openResults !== "true") {
        if (Object.keys(activeScript).length && isMultiTabEnabled) {
          resetQueryState();
          setQueryContext({ context: dataset.get("context") });
        }
        setCurrentSql({ sql: dataset.get("sql") });
      }
    }
  });

  getDefaultValue = () => {
    const { previousMultiSql, dataset, isViewingHistory, location } =
      this.props;

    if (previousMultiSql && isTabbableUrl(location)) {
      return previousMultiSql;
    } else if (isViewingHistory) {
      // Don't overwrite editor content with the script if viewing history
      return dataset.get("sql");
    } else {
      // Default to script content if available
      return this.state.script.content || dataset.get("sql");
    }
  };

  // Expose Monaco's setValue method to programmatically update the editor contents
  updateEditorContents = () => {
    const { editorContents, setEditorContents } = this.props;

    if (editorContents != null) {
      setEditorContents({ content: null });
      this.getMonacoEditor()?.setValue(editorContents);
    }
  };

  render() {
    this.getScriptValue();

    const sqlStyle = this.props.sqlState
      ? {}
      : { height: 0, overflow: "hidden" };

    const sqlBlock = (
      <SqlAutoComplete
        dataset={this.props.dataset}
        type={this.props.type}
        isGrayed={this.shouldSqlBoxBeGrayedOut()}
        context={this.props.queryContext}
        changeQueryContext={this.handleContextChange}
        ref={(ref) => (this.sqlEditorControllerRef = ref)}
        onChange={this.handleSqlChange}
        onFunctionChange={this.toggleFunctionsHelpPanel.bind(this)}
        toggleExtraSQLPanel={this.toggleExtraSQLPanel}
        defaultValue={this.state.defaultValue}
        sqlSize={this.props.sqlSize - toolbarHeight}
        sidebarCollapsed={this.props.sidebarCollapsed}
        datasetsPanel={this.state.datasetsPanel}
        sidePanelEnabled={this.state.funcHelpPanel || this.state.extraSqlPanel}
        dragType={this.props.dragType}
        serverSqlErrors={this.getServerSqlErrors()}
        editorWidth={this.props.editorWidth}
        hasExtraSQLPanelContent={this.props.hasExtraSQLPanelContent}
        isDarkMode={this.props.colorScheme === "dark"}
      />
    );

    return (
      <div style={{ width: "100%" }}>
        <div
          className="sql-part"
          onClick={this.hideDropDown}
          style={styles.base}
        >
          <div className="sql-functions">{this.renderFunctionsSQLPanel()}</div>
          <div className="sql-extra-panel">{this.renderExtraSQLPanel()}</div>
          <div style={sqlStyle}>{sqlBlock}</div>
        </div>
      </div>
    );
  }
}

function mapStateToProps(state) {
  const explorePageState = getExploreState(state);
  const location = getLocation(state);
  const isMultiTabEnabled = state.supportFlags["sqlrunner.tabs_ui"];
  const query = location?.query;

  return {
    currentSql: explorePageState.view.currentSql,
    queryContext: explorePageState.view.queryContext,
    focusKey: explorePageState.view.sqlEditorFocusKey,
    datasetSummary: state.resources.entities.get("datasetSummary"),
    activeScript: getActiveScript(state),
    queryStatuses: explorePageState.view.queryStatuses,
    querySelections: explorePageState.view.querySelections,
    updateSqlFromHistory: explorePageState.view.updateSqlFromHistory,
    isMultiQueryRunning: explorePageState.view.isMultiQueryRunning,
    previousMultiSql: explorePageState.view.previousMultiSql,
    editorContents: explorePageState.view.editorContents,
    isOpenResults: query?.openResults,
    scriptId: query?.scriptId,
    isViewingHistory:
      query && query.scriptId && query.version !== query.tipVersion,
    isFromDataGraph: location?.state?.isFromDataGraph,
    isMultiTabEnabled,
    location,
  };
}

export default compose(
  withColorScheme,
  withExtraSQLEditorContent,
  connect(
    mapStateToProps,
    {
      addNotification,
      fetchSQLScripts: fetchScripts,
      setActiveScript,
      setCurrentSql,
      modifyCurrentSql,
      setQueryContext,
      resetQueryState,
      setUpdateSqlFromHistory,
      setEditorContents,
      replaceUrlAction: replace,
      showUnsavedChangesConfirmDialog,
    },
    null,
    { forwardRef: true },
  ),
)(SqlEditorController);

const styles = {
  base: {
    paddingBottom: 0,
    position: "relative",
  },
  btn: {
    display: "flex",
    alignItems: "center",
    marginRight: 10,
    position: "relative",
  },
};
