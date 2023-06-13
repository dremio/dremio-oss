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
import { formatQuery } from "dremio-ui-common/sql/formatter/sqlFormatter.js";
import { isNotSoftware } from "dyn-load/utils/versionUtils";
import { debounce } from "lodash";
import { forwardRef, PureComponent } from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import MonacoEditor from "react-monaco-editor";
import Immutable from "immutable";
import * as SQLLanguage from "monaco-editor/dev/vs/basic-languages/src/sql";

import { fetchSupportFlags } from "@app/actions/supportFlags";
import { MSG_CLEAR_DELAY_SEC } from "@app/constants/Constants";
import { AUTOCOMPLETE_UI_V2 as AUTOCOMPLETE_V2_SUPPORTFLAG } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { AUTOCOMPLETE_UI_V2 as AUTOCOMPLETE_V2_FEATUREFLAG } from "@app/exports/flags/AUTOCOMPLETE_UI_V2";
import { getFeatureFlag } from "@app/selectors/featureFlagsSelector";
import { getSupportFlags } from "@app/selectors/supportFlags";
import { intl } from "@app/utils/intl";
import { useSqlFunctions } from "@app/pages/ExplorePage/components/SqlEditor/hooks/useSqlFunctions";
import { fetchFeatureFlag } from "@inject/actions/featureFlag";

import { RESERVED_WORDS } from "utils/pathUtils";
import { runDatasetSql, previewDatasetSql } from "actions/explore/dataset/run";
import { addNotification } from "actions/notification";
import { SQLAutoCompleteProvider } from "./SQLAutoCompleteProvider";

import "./SQLEditor.less";

let haveLoaded = false;
let SnippetController;
const language = "dremio-sql";

const staticPropTypes = {
  height: PropTypes.number.isRequired, // pass-thru
  defaultValue: PropTypes.string, // pass-thru; do not update it via onChange, otherwise monaco will throw error.
  onChange: PropTypes.func,
  errors: PropTypes.instanceOf(Immutable.List),
  readOnly: PropTypes.bool,
  fitHeightToContent: PropTypes.bool,
  maxHeight: PropTypes.number, // is only applicable for fitHeightToContent case
  contextMenu: PropTypes.bool,
  autoCompleteEnabled: PropTypes.bool,
  formatterEnabled: PropTypes.bool,
  sqlContext: PropTypes.instanceOf(Immutable.List),
  customDecorations: PropTypes.array,
  runDatasetSql: PropTypes.func,
  previewDatasetSql: PropTypes.func,
  fetchFeatureFlag: PropTypes.func,
  fetchSupportFlags: PropTypes.func,
  autocompleteV2Enabled: PropTypes.bool,
  sqlFunctions: PropTypes.array,
};

const checkHeightAndFitHeightToContentFlags = (
  props,
  propName,
  componentName
) => {
  if (props.fitHeightToContent) {
    if (props.height !== undefined) {
      return new Error(
        "Height must not be provided if fitHeightToContent property set to true"
      );
    }
  } else {
    return PropTypes.checkPropTypes(
      staticPropTypes,
      props,
      propName,
      componentName
    ); // reuse standard prop types check
  }
};

const Observe = (sel, opt, cb) => {
  const Obs = new MutationObserver((m) => [...m].forEach(cb));
  document.querySelectorAll(sel).forEach((el) => Obs.observe(el, opt));
};

export class SQLEditor extends PureComponent {
  static propTypes = {
    ...staticPropTypes,
    height: checkHeightAndFitHeightToContentFlags, // pass-thru

    // all others pass thru
  };

  reseting = false;
  monacoEditorComponent = null;
  monaco = null;
  editor = null;
  previousDecorations = [];
  autoCompleteResources = []; // will store onDidTypeListener and auto completion provider for dispose purposes
  _focusOnMount = false;

  state = {
    language: "sql",
    theme: "vs",
    treeUpdated: false,
  };
  static defaultProps = {
    theme: null,
  };

  componentDidMount() {
    if (this.props.defaultValue !== undefined) {
      this.resetValue();
    }

    this.fitHeightToContent();

    if (this.props.autocompleteV2Enabled == undefined) {
      this.fetchAutocompleteV2Flag();
    }
  }

  // do this in componentDidUpdate so it only happens once mounted.
  componentDidUpdate(prevProps) {
    if (this.props.defaultValue !== prevProps.defaultValue) {
      this.resetValue();
    }
    if (
      this.props.errors !== prevProps.errors ||
      this.props.customDecorations !== prevProps.customDecorations
    ) {
      this.applyDecorations();
    }
    if (this.props.fitHeightToContent && this.props.value !== prevProps.value) {
      this.fitHeightToContent();
    }

    if (
      this.props.formatterEnabled &&
      !prevProps.formatterEnabled &&
      this.editor
    ) {
      this.addFormattingShortcut(this.editor);
    }

    if (this.props.autoCompleteEnabled !== prevProps.autoCompleteEnabled) {
      this.setAutocompletion(this.props.autoCompleteEnabled);
    }

    if (this.props.theme !== prevProps.theme) {
      this.setEditorTheme();
    }

    this.observeSuggestWidget();
  }

  componentWillUnmount() {
    this.removeAutoCompletion();
  }

  fetchAutocompleteV2Flag() {
    if (isNotSoftware?.()) {
      this.props.fetchFeatureFlag?.(AUTOCOMPLETE_V2_FEATUREFLAG);
    } else {
      this.props.fetchSupportFlags?.(AUTOCOMPLETE_V2_SUPPORTFLAG);
    }
  }

  observeSuggestWidget() {
    const suggestItemHeight = 35;

    if (
      !this.state.treeUpdated &&
      document.querySelectorAll(".editor-widget.suggest-widget").length > 0
    ) {
      Observe(
        ".editor-widget.suggest-widget .tree",
        {
          attributeOldValue: true,
          attributeFilter: ["style", "class"],
        },
        (m) => {
          const oldValue = m.oldValue;
          const newValue = m.target.getAttribute(m.attributeName);
          const suggestedItems = document.querySelectorAll(
            ".suggest-widget .monaco-list-rows .monaco-list-row"
          );

          if (oldValue !== newValue && suggestedItems.length > 0) {
            const suggestedFocusItem = document.getElementsByClassName(
              "monaco-list-row focused"
            )[0];
            const suggestedRows =
              document.getElementsByClassName("monaco-list-rows")[0];
            const treeHeight =
              (suggestedFocusItem &&
                suggestedFocusItem.offsetHeight - suggestItemHeight) +
              suggestedItems.length * suggestItemHeight;
            const monacoListRowsHeight = suggestedRows.style.height.split("px");
            const monacoListRowsHeightNew =
              parseInt(monacoListRowsHeight[0], 10) +
              (suggestedFocusItem && suggestedFocusItem.offsetHeight);

            // Resize suggested rows height to match widget window
            suggestedRows.style.height = `${monacoListRowsHeightNew}px`;

            // Calculate to resize widget window size
            document.querySelectorAll(
              ".editor-widget.suggest-widget .tree"
            )[0].style.height = `${treeHeight + 20}px`;
            document.querySelectorAll(
              ".editor-widget.suggest-widget .tree .scrollbar.vertical"
            )[0].style.height = `${treeHeight}px`;
          }
        }
      );

      this.setState({ treeUpdated: true });

      Observe(
        ".editor-widget.suggest-widget .monaco-list-rows",
        {
          attributes: true,
          attributeFilter: ["style"],
        },
        () => {
          // Sorting the unordered list when a user scrolls
          let suggestedItems =
            document.getElementsByClassName("monaco-list-row");
          suggestedItems = Array.prototype.slice.call(suggestedItems);

          suggestedItems.sort((a, b) => {
            const aIndex = parseInt(a.getAttribute("data-index"), 10);
            const bIndex = parseInt(b.getAttribute("data-index"), 10);

            if (aIndex > bIndex) {
              return 1;
            } else if (aIndex < bIndex) {
              return -1;
            } else if (aIndex === bIndex) {
              return 0;
            }
          });

          for (let i = 0, len = suggestedItems.length; i < len; i++) {
            // store the parent node so we can reatach the item
            const parent = suggestedItems[i].parentNode;
            // detach it from wherever it is in the DOM
            const detatchedItem = parent.removeChild(suggestedItems[i]);
            // reatach it.  This works because we are itterating
            // over the items in the same order as they were re-
            // turned from being sorted.
            parent.appendChild(detatchedItem);
          }
        }
      );
    }
  }

  handleChange = (...args) => {
    if (!this.reseting) {
      // if there are decorations, remove them
      if (this.monacoEditorComponent && this.monacoEditorComponent.editor) {
        this.previousDecorations =
          this.monacoEditorComponent.editor.deltaDecorations(
            this.previousDecorations,
            []
          );
      }

      // Debouncing helps with UI lag from autocomplete and undo/redo
      const debounced = debounce(() => this.props.onChange(...args), 50);
      debounced();
    }
  };

  resetValue() {
    if (!this.monacoEditorComponent.editor) return;
    this.reseting = true;
    try {
      const editor = this.monacoEditorComponent.editor;
      editor.executeEdits("dremio", [
        {
          identifier: "dremio-reset",
          range: editor.getModel()?.getFullModelRange(),
          text: this.props.defaultValue ?? "",
        },
      ]);
      const range = editor.getModel()?.getFullModelRange() ?? {};
      editor.setSelection({
        ...range,
        startColumn: range.endColumn,
        startLineNumber: range.endLineNumber,
      });
      this.applyDecorations();
      this.focus();
    } finally {
      this.reseting = false;
    }
  }

  focus() {
    const editor = this.monacoEditorComponent.editor;
    if (editor) {
      editor.focus();
      this._focusOnMount = false;
    } else {
      this._focusOnMount = true;
    }
  }

  applyDecorations() {
    const { customDecorations, errors } = this.props;

    if (!this.monacoEditorComponent || !this.monacoEditorComponent.editor)
      return;

    const monaco = this.monaco;
    if (!errors && !customDecorations) {
      this.previousDecorations =
        this.monacoEditorComponent.editor.deltaDecorations(
          this.previousDecorations,
          []
        );
      return;
    }

    // todo: filter errors to known types

    const decorations = errors
      ?.toJS()
      .filter((error) => error.range)
      .map((error) => {
        let range = new monaco.Range(
          error.range.startLine,
          error.range.startColumn,
          error.range.endLine,
          error.range.endColumn + 1
        );
        if (range.isEmpty()) {
          // note: Monaco seems fine with ranges that go out of bounds
          range = new monaco.Range(
            error.range.startLine,
            error.range.startColumn - 1,
            error.range.endLine,
            error.range.endColumn + 1
          );
        }

        // idea was taken from setModelMarkers (see https://github.com/Microsoft/monaco-editor/issues/255
        // and https://github.com/Microsoft/monaco-typescript/blob/master/src/languageFeatures.ts#L140)
        // however it is not possible to set stickiness: monaco.editor.TrackedRangeStickiness.NeverGrowsWhenTypingAtEdges,
        // for markers. That is why I copied styles, that are generated for markers decoration and applied them here
        // I marked copied styles by comment // [marker-decoration-source]
        return {
          range,
          options: {
            hoverMessage: error.message, // todo: loc
            linesDecorationsClassName: "dremio-error-line",
            stickiness:
              monaco.editor.TrackedRangeStickiness.NeverGrowsWhenTypingAtEdges,
            className: "redsquiggly", // [marker-decoration-source]
            overviewRuler: {
              // [marker-decoration-source]
              color: "rgba(255,18,18,0.7)", // [marker-decoration-source] // if change this value, also change @monaco-error variable in color-schema.scss
              darkColor: "rgba(255,18,18,0.7)", // [marker-decoration-source]
              hcColor: "rgba(255,50,50,1)", // [marker-decoration-source]
              position: monaco.editor.OverviewRulerLane.Right, // [marker-decoration-source]
            },
          },
        };
      });

    this.previousDecorations =
      this.monacoEditorComponent.editor.deltaDecorations(
        this.previousDecorations,
        customDecorations ? customDecorations : decorations
      );
  }

  fitHeightToContent() {
    const { fitHeightToContent, maxHeight } = this.props;

    if (!fitHeightToContent) return;

    const editor =
      this.monacoEditorComponent && this.monacoEditorComponent.editor;

    if (!editor) return;

    let height = this._getContentHeight(editor);

    if (maxHeight && maxHeight < height) {
      height = maxHeight;
    }

    editor.layout({ height });
  }

  _getContentHeight(editor) {
    const configuration = editor.getConfiguration();

    const lineHeight = configuration.lineHeight;
    // we need take in account row wrapping. The solution was found here
    // https://github.com/Microsoft/monaco-editor/issues/947#issuecomment-403756024
    const lineCount = editor.viewModel.getLineCount();
    const contentHeight = lineHeight * lineCount;

    const horizontalScrollbarHeight =
      configuration.layoutInfo.horizontalScrollbarHeight;

    const editorHeight = contentHeight + horizontalScrollbarHeight;
    const defaultHeight =
      lineHeight * (this.minHeight || 0) + horizontalScrollbarHeight;
    return Math.max(defaultHeight, editorHeight);
  }

  setAutocompletion(enabled) {
    this.removeAutoCompletion(enabled);
    const editor = this.editor;

    if (!enabled || !editor) return;

    this.autoCompleteResources.push(
      this.monaco.languages.registerCompletionItemProvider(
        language,
        SQLAutoCompleteProvider(this.monaco, this.getSqlContext)
      )
    );

    this.autoCompleteResources.push(
      this.editor.onDidType((text) => {
        if (text === "\n" || text === ";") {
          // Do not trigger autocomplete
          editor.trigger("", "hideSuggestWidget", null);
          return;
        }

        editor.trigger(
          "dremio autocomplete request",
          "editor.action.triggerSuggest",
          {}
        );
      })
    );

    // Listner to remove autocomplete's 'No Suggetions' widget
    this.autoCompleteResources.push(
      this.editor.contentWidgets[
        "editor.widget.suggestWidget"
      ].widget.onDidShow(({ messageElement }) => {
        if (messageElement.innerText === "No suggestions.") {
          messageElement.hidden = true;
        }
      })
    );
  }

  getSqlContext = () =>
    this.props.sqlContext ? this.props.sqlContext.toJS() : [];

  removeAutoCompletion(enabled) {
    if (this.autoCompleteResources) {
      this.autoCompleteResources.forEach((resource) => {
        resource.dispose();
      });
    }
    this.autoCompleteResources = [];

    // Monaco Editor was still showing the suggestion window even if Autocomplete was turned off.
    // It's not built to toggle the autocomplete on/off. Therefore, this fix will hide the widget
    // when the state is false and show when it's true.
    const suggestWidget = document.getElementsByClassName(
      "editor-widget suggest-widget"
    )[0];
    if (suggestWidget !== undefined) {
      if (enabled) {
        suggestWidget.style.display = "block";
      } else {
        suggestWidget.style.display = "none";
      }
    }
  }
  setEditorTheme = () => {
    const {
      background,
      customTheme,
      inactiveSelectionBackground,
      selectionBackground,
      theme,
    } = this.props;

    if (customTheme) {
      this.monaco.editor.defineTheme("sqlEditorTheme", {
        base: theme,
        inherit: true,
        rules: [],
        colors: {
          "editor.background": background,
          "editor.selectionBackground": selectionBackground,
          "editor.inactiveSelectionBackground": inactiveSelectionBackground,
        },
      });
      this.monaco.editor.setTheme("sqlEditorTheme");
      this.setState({
        theme: "sqlEditorTheme",
      });
    }
  };

  registerFormattingProvider(language) {
    const actions = window.require("vs/platform/actions/common/actions");
    const editorContextId = actions.MenuId.EditorContext.id;
    // Remove the built-in format document context menu item since it does not reflect our custom shortcut
    actions.MenuRegistry._menuItems[editorContextId] =
      actions.MenuRegistry._menuItems[editorContextId].filter(
        (menu) => menu.command.id != "editor.action.formatDocument"
      );
    this.monaco.languages.registerDocumentFormattingEditProvider(language, {
      provideDocumentFormattingEdits: (model) => [
        {
          range: model.getFullModelRange(),
          text: this.onFormatQuery(model.getValue()),
        },
      ],
    });
  }

  editorDidMount = (editor, monaco) => {
    this.monaco = monaco;
    this.setEditorTheme();
    editor.getDomNode()._monacoEditor = editor; // for e2e tests

    // if this is our first time using monaco it will lazy load
    // only once it's loaded can we set up languages, etc
    if (!haveLoaded) {
      const { language: tokenProvider, conf } = SQLLanguage;
      tokenProvider.builtinVariables = [];
      tokenProvider.keywords = [...RESERVED_WORDS];
      // currently mixed into .keywords due to RESERVED_WORDS; todo: factor out
      tokenProvider.operators = [];
      tokenProvider.builtinFunctions = [];
      tokenProvider.builtinVariables = [];
      tokenProvider.pseudoColumns = [];

      // todo:
      // limit operators to /[*+\-<>!=&|/~]/
      tokenProvider.tokenizer.comments.push([/\/\/+.*/, "comment"]);

      monaco.languages.register({ id: language });
      monaco.languages.setMonarchTokensProvider(language, tokenProvider);
      monaco.languages.setLanguageConfiguration(language, conf);

      this.registerFormattingProvider(language);

      SnippetController = window.require(
        "vs/editor/contrib/snippet/browser/snippetController2"
      ).SnippetController2;

      haveLoaded = true;
    }

    // these are just for debugging, so it's okay if there's a change they override other active instances
    window.dremioEditor = editor;
    window.monaco = monaco;

    this.editor = editor;
    this.applyDecorations();
    this.setAutocompletion(this.props.autoCompleteEnabled);

    this.fitHeightToContent();

    this.setState({ language });

    if (this._focusOnMount) {
      this.focus();
    }
    this.addKeyboardShortcuts(editor);
  };

  onKbdPreview = () => {
    if (this.getSelectedSql() !== "") {
      this.props.previewDatasetSql({ selectedSql: this.getSelectedSql() });
    } else {
      this.props.previewDatasetSql();
    }
  };

  getMonacoEditorInstance = () => {
    return this?.monacoEditorComponent?.editor;
  };

  getSelectedSql = () => {
    if (this.getMonacoEditorInstance() === undefined) {
      return "";
    }

    const selection = this.getMonacoEditorInstance().getSelection();
    const range = {
      endColumn: selection.endColumn,
      endLineNumber: selection.endLineNumber,
      startColumn: selection.startColumn,
      startLineNumber: selection.startLineNumber,
    };
    return this.getMonacoEditorInstance().getModel().getValueInRange(range);
  };

  onKbdRun = () => {
    if (this.getSelectedSql() !== "") {
      this.props.runDatasetSql({ selectedSql: this.getSelectedSql() });
    } else {
      this.props.runDatasetSql();
    }
  };

  addKeyboardShortcuts = (editor) => {
    const monaco = this.monaco;
    editor.addAction({
      id: "keys-preview",
      label: "Preview",
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter], // eslint-disable-line no-bitwise
      precondition: null,
      keybindingContext: null,
      run: this.onKbdPreview,
    });
    editor.addAction({
      id: "keys-run",
      label: "Run",
      keybindings: [
        monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.Enter,
      ], // eslint-disable-line no-bitwise
      precondition: null,
      keybindingContext: null,
      run: this.onKbdRun,
    });
    if (this.props.formatterEnabled) {
      this.addFormattingShortcut(editor);
    }
    // trigger autocomplete suggestWidget
    editor.addCommand(monaco.KeyMod.WinCtrl | monaco.KeyCode.Space, () =>
      editor.trigger("", "editor.action.triggerSuggest")
    );
  };

  addFormattingShortcut = (editor) => {
    editor.addAction({
      id: "keys-format",
      label: intl.formatMessage({ id: "SQL.Format" }),
      keybindings: [
        monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KEY_F,
      ], // eslint-disable-line no-bitwise
      precondition: null,
      keybindingContext: null,
      run: () => {
        editor.getAction("editor.action.formatDocument").run();
      },
      contextMenuGroupId: "1_modification", // Show a context menu item for the action
      contextMenuOrder: 2, // After "Change All Occurrences"
    });
  };

  onFormatQuery = (query) => {
    if (!query) {
      return query;
    }

    try {
      return formatQuery(query);
    } catch {
      this.props.addNotification(
        intl.formatMessage({ id: "SQL.Format.Error" }),
        "error",
        MSG_CLEAR_DELAY_SEC
      );
      return query;
    }
  };

  insertSnippet() {
    SnippetController.get(this.monacoEditorComponent.editor).insert(
      ...arguments
    );
  }

  render() {
    const {
      readOnly,
      contextMenu,
      // monaco does not support fitHeightToContent, so exclude it from monacoProps
      ...monacoProps
    } = this.props;

    return (
      // div wrapper is required for FF and IE. Without it a editor has uncontrolled grow on jobs page.
      <div>
        <MonacoEditor
          {...monacoProps}
          onChange={this.handleChange}
          editorDidMount={this.editorDidMount}
          ref={(ref) => (this.monacoEditorComponent = ref)}
          width="100%"
          language={this.state.language}
          theme={this.state.theme}
          className="dremio-typography-monospace"
          options={{
            wordWrap: "on",
            lineNumbersMinChars: 3,
            scrollBeyondLastLine: false,
            scrollbar: { vertical: "visible", useShadows: false },
            automaticLayout: true,
            lineDecorationsWidth: 12,
            fontFamily: "Consolas, Fira Code",
            fontSize: 14,
            fixedOverflowWidgets: true,
            minimap: {
              enabled: false,
            },
            suggestLineHeight: 25,
            suggestFontSize: 12,
            readOnly,
            hideCursorInOverviewRuler: true,
            contextmenu: contextMenu, // a case is important here
            ...(this.props.customOptions && this.props.customOptions),
          }}
          requireConfig={{ url: "/vs/loader.js", paths: { vs: "/vs" } }}
        />
      </div>
    );
  }
}

const SqlEditorWithHooksProps = forwardRef((props, ref) => {
  const [sqlFunctions] = useSqlFunctions();
  return <SQLEditor sqlFunctions={sqlFunctions} {...props} ref={ref} />;
});
SqlEditorWithHooksProps.displayName = "SqlEditorWithHooksProps";

const isAutocompleteV2Enabled = (state) => {
  if (isNotSoftware?.()) {
    const flagVal = getFeatureFlag(state, AUTOCOMPLETE_V2_FEATUREFLAG);
    return flagVal ? flagVal == "ENABLED" : undefined;
  } else {
    return getSupportFlags(state)[AUTOCOMPLETE_V2_SUPPORTFLAG];
  }
};

const mapStateToProps = (state) => ({
  autocompleteV2Enabled: isAutocompleteV2Enabled(state),
});

export default connect(
  mapStateToProps,
  {
    runDatasetSql,
    previewDatasetSql,
    addNotification,
    fetchFeatureFlag,
    fetchSupportFlags,
  },
  null,
  { forwardRef: true }
)(SqlEditorWithHooksProps);
