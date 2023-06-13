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
import { Component, createRef } from "react";
import { FormattedMessage } from "react-intl";

import PropTypes from "prop-types";
import Immutable from "immutable";
import deepEqual from "deep-equal";
import classNames from "clsx";

import { connect } from "react-redux";
import exploreUtils from "utils/explore/exploreUtils";
import { splitFullPath, constructFullPath } from "utils/pathUtils";

import Modal from "components/Modals/Modal";
import SQLEditor from "components/SQLEditor";

import DragTarget from "components/DragComponents/DragTarget";
import { Tooltip } from "components/Tooltip";
import { Tooltip as NewTooltip } from "dremio-ui-lib";

import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import { getSupportFlags } from "@app/selectors/supportFlags";
import { fetchSupportFlags } from "@app/actions/supportFlags";
import config from "@inject/utils/config";
import { isEnterprise, isCommunity } from "dyn-load/utils/versionUtils";
import SelectContextForm from "../forms/SelectContextForm";

import "./SqlAutoComplete.less";
import RefPicker from "./components/RefPicker";

const DEFAULT_CONTEXT = "<none>";

export class SqlAutoComplete extends Component {
  // todo: pull SQLEditor into this class (and rename)
  static propTypes = {
    onChange: PropTypes.func,
    onFunctionChange: PropTypes.func,
    pageType: PropTypes.oneOf(["details", "recent"]),
    defaultValue: PropTypes.string,
    isGrayed: PropTypes.bool,
    context: PropTypes.instanceOf(Immutable.List),
    errors: PropTypes.instanceOf(Immutable.List),
    name: PropTypes.string,
    sqlSize: PropTypes.number,
    funcHelpPanel: PropTypes.bool,
    changeQueryContext: PropTypes.func,
    style: PropTypes.object,
    dragType: PropTypes.string,
    children: PropTypes.any,
    fetchSupportFlags: PropTypes.func,
    supportFlags: PropTypes.object,
    sidebarCollapsed: PropTypes.bool,
    customDecorations: PropTypes.array,
    editorWidth: PropTypes.any,
  };

  static defaultProps = {
    sqlSize: 100,
  };

  static contextTypes = {
    location: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired,
  };

  monacoEditorComponent = null;
  sqlAutoCompleteRef = null;

  constructor(props) {
    super(props);

    this.handleClickEditContext = this.handleClickEditContext.bind(this);
    this.renderSelectContextModal = this.renderSelectContextModal.bind(this);
    this.hideSelectContextModal = this.hideSelectContextModal.bind(this);
    this.updateContext = this.updateContext.bind(this);
    this.onMouseEnter = this.onMouseEnter.bind(this);
    this.onMouseLeave = this.onMouseLeave.bind(this);
    this.addContextTooltip = this.addContextTooltip.bind(this);

    this.ref = {
      showContextTooltipRef: createRef(),
      targetRef: createRef(),
    };
    this.state = {
      showSelectContextModal: false,
      isContrast: localStorageUtils.getSqlThemeContrast(),
      isAutocomplete: localStorageUtils.getSqlAutocomplete(),
      funcHelpPanel: this.props.funcHelpPanel,
      manuallyEnableAutocomplete: false,
      tooltipHover: false,
      showContextTooltip: false,
      os: "windows",
    };
  }

  componentDidMount() {
    //  fetch supportFlags only if its not enterprise edition
    const isEnterpriseFlag = isEnterprise && isEnterprise();
    const isCommunityFlag = isCommunity && isCommunity();
    if (
      !(isEnterpriseFlag || isCommunityFlag) &&
      this.props.fetchSupportFlags
    ) {
      this.props.fetchSupportFlags("ui.autocomplete.allow");
      this.props.fetchSupportFlags("ui.formatter.allow");
    }
    this.getUserOperatingSystem();
  }

  shouldComponentUpdate(nextProps, nextState, nextContext) {
    return (
      nextProps.defaultValue !== this.props.defaultValue ||
      nextProps.context !== this.props.context ||
      nextContext.location.query.version !==
        this.context.location.query.version ||
      nextProps.funcHelpPanel !== this.props.funcHelpPanel ||
      nextProps.isGrayed !== this.props.isGrayed ||
      nextProps.sqlSize !== this.props.sqlSize ||
      nextProps.supportFlags !== this.props.supportFlags ||
      nextProps.sidebarCollapsed !== this.props.sidebarCollapsed ||
      nextProps.customDecorations !== this.props.customDecorations ||
      !deepEqual(nextState, this.state)
    );
  }

  componentWillReceiveProps(nextProps) {
    // reset this when it is rest at the project settings level
    const isEnterpriseFlag = isEnterprise && isEnterprise();
    const isCommunityFlag = isCommunity && isCommunity();

    if (
      !(isEnterpriseFlag || isCommunityFlag) &&
      nextProps.supportFlags &&
      nextProps.supportFlags["ui.autocomplete.allow"] !== undefined &&
      !nextProps.supportFlags["ui.autocomplete.allow"]
    ) {
      this.setState({
        isAutocomplete: false,
      });
    } else if (
      (isEnterpriseFlag || isCommunityFlag) &&
      !config.allowAutoComplete
    ) {
      // if enterprise or community edition then read allowAutoComplete flag from config
      this.setState({ isAutocomplete: false });
    }
  }

  isFormatterEnabled() {
    // reset this when it is rest at the project settings level
    const isEnterpriseFlag = isEnterprise && isEnterprise();
    const isCommunityFlag = isCommunity && isCommunity();

    let formatterEnabled = false;

    if (!(isEnterpriseFlag || isCommunityFlag) && this.props.supportFlags) {
      formatterEnabled = this.props.supportFlags["ui.formatter.allow"];
    } else if (isEnterpriseFlag || isCommunityFlag) {
      formatterEnabled = config.allowFormatting;
    }

    return formatterEnabled;
  }

  handleDrop = ({ id, args }) => {
    // because we move the cursor as we drag around, we can simply insert at the current position in the editor (default)

    // duck-type check pending drag-n-drop revamp
    if (args !== undefined) {
      this.insertFunction(id, args);
    } else if (typeof id === "string") {
      this.insertFieldName(id);
    } else {
      this.insertFullPath(id);
    }
  };

  getMonacoEditorInstance() {
    return this.sqlAutoCompleteRef?.monacoEditorComponent?.editor;
  }

  getMonaco() {
    return this.sqlAutoCompleteRef.monaco;
  }

  getKeyboardShortcuts() {
    const isFormatterEnabled = this.isFormatterEnabled();
    return this.state.os === "windows"
      ? {
          run: "CTRL + Shift + Enter",
          preview: "CTRL + Enter",
          comment: "CTRL + /",
          find: "CTRL + F",
          autocomplete: "CTRL + Space",
          ...(isFormatterEnabled ? { format: "CTRL + Shift + F" } : {}),
        }
      : {
          run: "⌘⇧↵",
          preview: "⌘↵",
          comment: "⌘/",
          find: "⌘F",
          autocomplete: "⌃ Space",
          ...(isFormatterEnabled ? { format: "⌘⇧F" } : {}),
        };
  }

  handleDragOver = (evt) => {
    const target = this.getMonacoEditorInstance().getTargetAtClientPoint(
      evt.clientX,
      evt.clientY
    );
    if (!target || !target.position) return; // no position if you drag over the rightmost part of the context UI
    this.getMonacoEditorInstance().setPosition(target.position);
    this.focus();
  };

  focus() {
    if (this.sqlAutoCompleteRef) {
      this.sqlAutoCompleteRef.focus();
    }
  }

  resetValue() {
    this.sqlAutoCompleteRef && this.sqlAutoCompleteRef.resetValue();
  }

  handleChange = () => {
    this.updateCode();
  };

  handleClickEditContext() {
    this.setState({ showSelectContextModal: true });
  }

  hideSelectContextModal() {
    this.setState({ showSelectContextModal: false });
  }

  updateContext(resource) {
    this.props.changeQueryContext(
      Immutable.fromJS(resource.context && splitFullPath(resource.context))
    );
    this.hideSelectContextModal();
  }

  insertFullPath(pathList, ranges) {
    const text = constructFullPath(pathList);
    this.insertAtRanges(text, ranges);
  }

  insertFieldName(name, ranges) {
    const text = exploreUtils.escapeFieldNameForSQL(name);
    this.insertAtRanges(text, ranges);
  }

  insertFunction(
    name,
    args,
    ranges = this.getMonacoEditorInstance().getSelections()
  ) {
    const hasArgs = args && args.length;
    const text = name;

    if (!hasArgs) {
      // simple insert/replace
      this.insertAtRanges(text, ranges);
      return;
    }

    this.getMonacoEditorInstance().getModel().pushStackElement();

    const Selection = this.getMonaco().Selection;
    const nonEmptySelections = [];
    let emptySelections = [];
    ranges.forEach((range) => {
      const selection = new Selection(
        range.startLineNumber,
        range.startColumn,
        range.endLineNumber,
        range.endColumn
      );
      if (!selection.isEmpty()) {
        nonEmptySelections.push(selection);
      } else {
        emptySelections.push(selection);
      }
    });

    if (nonEmptySelections.length) {
      const edits = [
        ...nonEmptySelections.map((sel) => ({
          identifier: "dremio-inject",
          range: sel.collapseToStart(),
          text: text + "(",
        })),
        ...nonEmptySelections.map((sel) => ({
          identifier: "dremio-inject",
          range: Selection.fromPositions(sel.getEndPosition()),
          text: ")",
        })),
      ];
      this.getMonacoEditorInstance().executeEdits("dremio", edits);

      // need to update emptySelections for the new insertions
      // assumes that function names are single line, and ranges don't overlap
      const nudge = text.length + 2;
      nonEmptySelections.forEach((nonEmptySel) => {
        emptySelections = emptySelections.map((otherSelection) => {
          let { startColumn, endColumn } = otherSelection;
          const { startLineNumber, endLineNumber } = otherSelection;
          if (startLineNumber === nonEmptySel.endLineNumber) {
            if (startColumn >= nonEmptySel.endColumn) {
              startColumn += nudge;
              if (endLineNumber === startLineNumber) {
                endColumn += nudge;
              }
            }
          }
          return new Selection(
            startLineNumber,
            startColumn,
            endLineNumber,
            endColumn
          );
        });
      });
    }

    // do snippet-style insertion last so that the selection ends up with token selection
    // args comes in correct format from BE
    if (emptySelections.length) {
      // insertSnippet only works with the current selection, so move the selection to the input range
      this.getMonacoEditorInstance().setSelections(emptySelections);
      this.sqlAutoCompleteRef.insertSnippet(
        text + args,
        undefined,
        undefined,
        false,
        false
      );
    }

    this.getMonacoEditorInstance().getModel().pushStackElement();
    this.focus();
  }

  insertAtRanges(
    text,
    ranges = this.getMonacoEditorInstance().getSelections()
  ) {
    // getSelections() falls back to cursor location automatically
    const edits = ranges.map((range) => ({
      identifier: "dremio-inject",
      range,
      text,
    }));

    // Remove highlighted string and place cursor to the end of the new `text`
    ranges[0].selectionStartColumn =
      ranges[0].selectionStartColumn + text.length;
    ranges[0].positionColumn = ranges[0].positionColumn + text.length;

    this.getMonacoEditorInstance().executeEdits("dremio", edits, ranges);
    this.getMonacoEditorInstance().pushUndoStop();
    this.focus();
  }

  updateCode() {
    if (this.props.onChange) {
      const value = this.getMonacoEditorInstance()?.getValue();
      this.props.onChange(value);
      this.getMonacoEditorInstance()?.pushUndoStop();
    }
  }

  renderSelectContextModal() {
    if (!this.state.showSelectContextModal) return null;

    const contextValue = constructFullPath(this.props.context);
    return (
      <Modal
        isOpen
        hide={this.hideSelectContextModal}
        size="small"
        title={la("Select Context")}
        modalHeight="600px"
      >
        <SelectContextForm
          onFormSubmit={this.updateContext}
          onCancel={this.hideSelectContextModal}
          initialValues={{ context: contextValue }}
        />
      </Modal>
    );
  }

  addContextTooltip() {
    const current = this.ref.showContextTooltipRef?.current;
    if (current.offsetWidth < current.scrollWidth) {
      this.setState({
        showContextTooltip: true,
      });
    }
  }

  renderContext() {
    const contextValue = this.props.context
      ? constructFullPath(this.props.context, true)
      : DEFAULT_CONTEXT;
    const showContext = this.props.pageType === "details";
    const emptyContext =
      typeof contextValue === "string" && contextValue.trim() === "";
    const isContrast = this.state.isContrast;
    const contextClassName = isContrast
      ? "sqlAutocomplete__contextText-dark"
      : "sqlAutocomplete__contextText-light";

    return (
      <div
        className="sqlAutocomplete__context"
        style={{ ...styles.context, ...(!!showContext && { display: "none" }) }}
      >
        {emptyContext ? (
          <span
            className={contextClassName}
            onClick={this.handleClickEditContext}
          >
            Context
          </span>
        ) : (
          <>
            Context:
            {this.state.showContextTooltip ? (
              <NewTooltip title={contextValue}>
                <span
                  className={contextClassName}
                  onClick={this.handleClickEditContext}
                >
                  {contextValue}
                </span>
              </NewTooltip>
            ) : (
              <span
                ref={this.ref.showContextTooltipRef}
                onMouseEnter={this.addContextTooltip}
                className={contextClassName}
                onClick={this.handleClickEditContext}
              >
                {contextValue}
              </span>
            )}
          </>
        )}
      </div>
    );
  }

  renderReferencePicker() {
    const isContrast = this.state.isContrast;
    const contextClassName = isContrast
      ? "sqlAutocomplete__contextText-dark"
      : "sqlAutocomplete__contextText-light";
    return (
      <RefPicker
        hide={this.props.pageType === "details"}
        contextClassName={contextClassName}
      />
    );
  }

  handleClick() {
    localStorageUtils.setSqlThemeContrast(!this.state.isContrast);
    this.setState((state) => {
      return { isContrast: !state.isContrast };
    });
  }

  handleAutocompleteClick() {
    localStorageUtils.setSqlAutocomplete(!this.state.isAutocomplete);
    this.setState((state) => {
      return { isAutocomplete: !state.isAutocomplete };
    });
  }

  showAutocomplete() {
    const { supportFlags } = this.props;

    //  if its enterprise read supportFlag from dremioConfig else read it from API
    const isEnterpriseFlag = isEnterprise && isEnterprise();
    const isCommunityFlag = isCommunity && isCommunity();
    const showAutoCompleteOption =
      isEnterpriseFlag || isCommunityFlag
        ? config.allowAutoComplete
        : supportFlags && supportFlags["ui.autocomplete.allow"];
    if (showAutoCompleteOption) {
      return (
        <span
          data-qa="toggle-autocomplete-icon"
          id="toggle--autocomplete-icon"
          className="sql__autocompleteIcon"
          onClick={this.handleAutocompleteClick.bind(this)}
        >
          <NewTooltip
            title={
              this.state.isAutocomplete
                ? "Autocomplete enabled"
                : "Autocomplete disabled"
            }
          >
            <dremio-icon
              name={
                this.state.isAutocomplete
                  ? "sql-editor/sqlAutoCompleteEnabled"
                  : "sql-editor/sqlAutoCompleteDisabled"
              }
              class={this.state.isContrast ? "sql__darkIcon" : "sql__lightIcon"}
            ></dremio-icon>
          </NewTooltip>
        </span>
      );
    }
  }

  onMouseEnter() {
    this.setState({ tooltipHover: true });
  }

  onMouseLeave() {
    this.setState({ tooltipHover: false });
  }

  getUserOperatingSystem() {
    if (navigator.userAgent.indexOf("Mac OS X") !== -1) {
      this.setState({
        os: "mac",
      });
    }
  }

  render() {
    const height = this.props.sqlSize;

    const {
      funcHelpPanel,
      isGrayed,
      errors,
      context,
      onFunctionChange,
      customDecorations,
    } = this.props;

    const { query } = this.context.location;

    const widthSqlEditor = funcHelpPanel
      ? styles.smallerSqlEditorWidth
      : this.props.editorWidth
      ? { width: this.props.editorWidth }
      : styles.SidebarEditorWidth;

    const keyboardShortcuts = this.getKeyboardShortcuts();
    const isFormatterEnabled = this.isFormatterEnabled();

    return (
      <DragTarget
        dragType={this.props.dragType}
        onDrop={this.handleDrop}
        onDragOver={this.handleDragOver}
      >
        <div
          className={classNames(
            "sqlAutocomplete",
            this.state.isContrast ? "vs-dark" : "vs"
          )}
          name={this.props.name}
          style={{
            ...styles.base,
            ...widthSqlEditor,
            ...(!!isGrayed && { opacity: 0.4, pointerEvents: "none" }),
            ...(this.props.style || {}),
          }}
        >
          <div className="sqlAutocomplete__actions text-sm">
            {this.renderSelectContextModal()}
            {query.type !== "transform" && this.renderContext()}
            {query.type !== "transform" && this.renderReferencePicker()}
            <span
              data-qa="toggle-icon"
              id="toggle-icon"
              className="function__toggleIcon"
              onClick={onFunctionChange}
            >
              <NewTooltip title={"Functions"}>
                <dremio-icon
                  name="sql-editor/function"
                  class={
                    this.state.isContrast ? "sql__darkIcon" : "sql__lightIcon"
                  }
                ></dremio-icon>
              </NewTooltip>
            </span>

            <span
              data-qa="toggle-icon"
              id="toggle-icon"
              className="sqlEditor__toggleIcon"
              onClick={this.handleClick.bind(this)}
            >
              <NewTooltip
                title={
                  this.state.isContrast
                    ? "Common.Theme.Dark"
                    : "Common.Theme.Light"
                }
              >
                <dremio-icon
                  name="sql-editor/sqlThemeSwitcher"
                  class={
                    this.state.isContrast ? "sql__darkIcon" : "sql__lightIcon"
                  }
                ></dremio-icon>
              </NewTooltip>
            </span>
            {this.showAutocomplete()}
            <span
              className="keyboard__shortcutsIcon"
              onMouseEnter={this.onMouseEnter}
              onMouseLeave={this.onMouseLeave}
              ref={this.ref.targetRef}
            >
              <dremio-icon
                name="sql-editor/keyboard"
                class={
                  this.state.isContrast ? "sql__darkIcon" : "sql__lightIcon"
                }
              ></dremio-icon>
              <Tooltip
                key="tooltip"
                type="info"
                placement="left-start"
                target={() =>
                  this.state.tooltipHover ? this.ref.targetRef.current : null
                }
                container={this}
                tooltipInnerClass="textWithHelp__tooltip --white keyboardShortcutTooltip"
                tooltipArrowClass="--white"
              >
                <p className="tooltip-content__heading">
                  <FormattedMessage id="KeyboardShortcuts.Shortcuts" />
                </p>
                <div className="divider" />
                <ul className="tooltip-content__list">
                  <li>
                    <FormattedMessage id="Common.Run" />
                    <span>{keyboardShortcuts.run}</span>
                  </li>
                  <li>
                    <FormattedMessage id="Common.Preview" />
                    <span>{keyboardShortcuts.preview}</span>
                  </li>
                  <li>
                    <FormattedMessage id="KeyboardShortcuts.Comment" />
                    <span>{keyboardShortcuts.comment}</span>
                  </li>
                  <li>
                    <FormattedMessage id="KeyboardShortcuts.Find" />
                    <span>{keyboardShortcuts.find}</span>
                  </li>
                  {this.state.isAutocomplete ? (
                    <li>
                      <FormattedMessage id="KeyboardShortcuts.Autocomplete" />
                      <span>{keyboardShortcuts.autocomplete}</span>
                    </li>
                  ) : (
                    <></>
                  )}
                  {isFormatterEnabled ? (
                    <li>
                      <FormattedMessage id="SQL.Format" />
                      <span>{keyboardShortcuts.format}</span>
                    </li>
                  ) : (
                    <></>
                  )}
                </ul>
              </Tooltip>
            </span>
          </div>

          <SQLEditor
            height={height - 2} // .sql-autocomplete has 1px top and bottom border. Have to substract border width
            ref={(ref) => (this.sqlAutoCompleteRef = ref)}
            defaultValue={this.props.defaultValue}
            onChange={this.handleChange}
            errors={errors}
            autoCompleteEnabled={this.state.isAutocomplete}
            formatterEnabled={isFormatterEnabled}
            sqlContext={context}
            customTheme
            theme={this.state.isContrast ? "vs-dark" : "vs"}
            background={this.state.isContrast ? "#333333" : "#FFFFFF"}
            selectionBackground={this.state.isContrast ? "#304D6D" : "#B5D5FB"}
            inactiveSelectionBackground={
              this.state.isContrast ? "#505862" : "#c6e9ef"
            }
            customDecorations={customDecorations}
          />
          {this.props.children}
        </div>
      </DragTarget>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    supportFlags: getSupportFlags(state),
  };
};

const mapDispatchToProps = {
  fetchSupportFlags,
};

export default connect(mapStateToProps, mapDispatchToProps, null, {
  forwardRef: true,
})(SqlAutoComplete);

const styles = {
  base: {
    position: "relative",
    width: "100%",
  },
  SidebarEditorWidth: {
    width: "calc(100% - 25px)",
  },
  smallerSqlEditorWidth: {
    width: "calc(60% - 7px)",
  },
  tooltip: {
    padding: "5px 10px 5px 10px",
    backgroundColor: "#f2f2f2",
  },
  messageStyle: {
    position: "absolute",
    bottom: "0px",
    zIndex: 1000,
  },
  editIcon: {
    Container: {
      height: 15,
      width: 20,
      position: "relative",
      display: "flex",
      alignItems: "center",
      cursor: "pointer",
    },
  },
  contextInput: {
    minWidth: 139,
    height: 19,
    outline: "none",
    paddingLeft: 3,
  },
};
