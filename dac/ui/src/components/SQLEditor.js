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

import { debounce } from "lodash";
import { forwardRef, PureComponent } from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import MonacoEditor from "react-monaco-editor";
import Immutable from "immutable";
import * as SQLLanguage from "monaco-editor/dev/vs/basic-languages/src/sql";

import { setActionState } from "actions/explore/view";
import { ExploreHeaderActions } from "@app/pages/ExplorePage/components/ExploreHeaderUtils";
import { formatQuery } from "dremio-ui-common/sql/formatter/sqlFormatter.js";
import { SQLEditorExtension } from "dremio-ui-common/sql/worker/client/SQLParsingWorkerClient.js";
import { fetchSupportFlags } from "@app/actions/supportFlags";
import { MSG_CLEAR_DELAY_SEC } from "@app/constants/Constants";
import { APIV2Call } from "@app/core/APICall";
import { LIVE_SYNTAX_ERROR_DETECTION } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { getSupportFlags } from "@app/selectors/supportFlags";
import { intl } from "@app/utils/intl";
import { useSqlFunctions } from "@app/pages/ExplorePage/components/SqlEditor/hooks/useSqlFunctions";
import { fetchFeatureFlag } from "@inject/actions/featureFlag";
import localStorageUtils from "@inject/utils/storageUtils/localStorageUtils";

import {
  EXCLUDE_FROM_FUNCTIONS,
  NULL_VALUE,
  RESERVED_TYPES,
  RESERVED_WORDS,
} from "utils/pathUtils";
import { runDatasetSql, previewDatasetSql } from "actions/explore/dataset/run";
import { addNotification } from "actions/notification";
import {
  getSQLEditorThemeRules,
  getSQLTokenNotationMap,
  SQL_LIGHT_THEME,
  TOKEN_NOTATION_REGEX,
} from "@app/utils/sql-editor";
import { renderExtraSQLKeyboardShortcuts } from "@inject/utils/sql-editor-extra";

import "./SQLEditor.less";

let haveLoaded = false;
let SnippetController;
const language = "dremio-sql";

const staticPropTypes = {
  height: PropTypes.number.isRequired, // pass-thru
  defaultValue: PropTypes.string, // pass-thru; do not update it via onChange, otherwise monaco will throw error.
  onChange: PropTypes.func,
  readOnly: PropTypes.bool,
  fitHeightToContent: PropTypes.bool,
  maxHeight: PropTypes.number, // is only applicable for fitHeightToContent case
  contextMenu: PropTypes.bool,
  autoCompleteEnabled: PropTypes.bool,
  formatterEnabled: PropTypes.bool,
  sqlContext: PropTypes.instanceOf(Immutable.List),
  serverSqlErrors: PropTypes.array,
  runDatasetSql: PropTypes.func,
  previewDatasetSql: PropTypes.func,
  fetchFeatureFlag: PropTypes.func,
  fetchSupportFlags: PropTypes.func,
  liveSyntaxErrorDetectionFlagEnabled: PropTypes.bool,
  liveSyntaxErrorDetectionDisabled: PropTypes.bool,
  sqlFunctions: PropTypes.array,
  setActionState: PropTypes.func,
  hasExtraSQLPanelContent: PropTypes.bool,
  toggleExtraSQLPanel: PropTypes.func,
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
  autoCompleteDisposers = [];
  liveErrorDetectionDisposers = [];
  _focusOnMount = false;
  sqlEditorExtension = null;

  state = {
    language: "sql",
    theme: SQL_LIGHT_THEME,
    treeUpdated: false,
    liveErrors: [],
  };
  static defaultProps = {
    theme: null,
  };

  componentDidMount() {
    if (this.props.defaultValue !== undefined) {
      this.resetValue();
    }

    this.fitHeightToContent();

    if (this.props.liveSyntaxErrorDetectionFlagEnabled == undefined) {
      this.props.fetchSupportFlags?.(LIVE_SYNTAX_ERROR_DETECTION);
    }
  }

  // do this in componentDidUpdate so it only happens once mounted.
  componentDidUpdate(prevProps, prevState) {
    if (this.props.defaultValue !== prevProps.defaultValue) {
      this.resetValue();
    }
    if (
      this.props.serverSqlErrors !== prevProps.serverSqlErrors ||
      this.state.liveErrors !== prevState?.liveErrors
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

    if (
      this.props.autoCompleteEnabled !== prevProps.autoCompleteEnabled ||
      this.props.liveSyntaxErrorDetectionFlagEnabled !==
        prevProps.liveSyntaxErrorDetectionFlagEnabled ||
      this.props.sqlFunctions !== prevProps.sqlFunctions
    ) {
      this.setupSQLEditorExtension(
        this.props.sqlFunctions,
        this.props.autoCompleteEnabled,
        this.props.liveSyntaxErrorDetectionFlagEnabled &&
          !this.props.liveSyntaxErrorDetectionDisabled
      );
    }

    if (this.props.theme !== prevProps.theme) {
      this.setEditorTheme();
    }

    if (this.props.sqlFunctions !== prevProps.sqlFunctions && this.monaco) {
      this.applySQLFunctionsToReservedKeywords();
    }

    this.observeSuggestWidget();
  }

  componentWillUnmount() {
    this.removeLiveErrorDetection();
    this.removeAutoCompletion(false);
  }

  getSqlFunctionLink = (fName) => {
    if (fName === undefined) return undefined;

    const { sqlFunctions = [] } = this.props;
    for (const sqlFunction of sqlFunctions) {
      if (sqlFunction.name === fName) return sqlFunction.link;
    }
    return undefined;
  };

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
            for (const row of suggestedRows.getElementsByClassName(
              "monaco-list-row"
            )) {
              const isFunctionSuggestion =
                row.getElementsByClassName("icon function")?.length === 1;
              // If it's a function suggestion, append doc link to the function description
              if (isFunctionSuggestion) {
                let fName = row.attributes.getNamedItem("aria-label")?.value;
                fName = fName?.substring(0, fName?.indexOf("("));
                const link = this.getSqlFunctionLink(fName);
                if (link !== undefined) {
                  const label = row.getElementsByClassName("type-label")[0];
                  // Skip appending doc link if there is no description for the function.
                  // Meanwhile avoid appending the link more than once.
                  if (label.textContent && label.childNodes.length < 2) {
                    // Fix context and format
                    label.textContent = label.textContent.trimEnd();
                    if (!label.textContent.endsWith(".")) {
                      label.textContent += ".";
                    }
                    label.textContent += " ";

                    // Add doc link node
                    const docLink = document.createElement("span");
                    docLink.appendChild(document.createTextNode("Docs"));
                    docLink.title = "Docs";
                    docLink.addEventListener("mousedown", (e) => {
                      window.open(link, "_blank");
                      e.stopPropagation();
                    });
                    label.appendChild(docLink);
                  }
                }
              }
            }
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
    const { serverSqlErrors } = this.props;
    const { liveErrors } = this.state;

    if (!this.monacoEditorComponent || !this.monacoEditorComponent.editor)
      return;

    const monaco = this.monaco;
    if (serverSqlErrors?.length == 0 && liveErrors.length == 0) {
      this.previousDecorations =
        this.monacoEditorComponent.editor.deltaDecorations(
          this.previousDecorations,
          []
        );
      return;
    }

    const errorToDecoration = (error) => {
      // idea was taken from setModelMarkers (see https://github.com/Microsoft/monaco-editor/issues/255
      // and https://github.com/Microsoft/monaco-typescript/blob/master/src/languageFeatures.ts#L140)
      // however it is not possible to set stickiness: monaco.editor.TrackedRangeStickiness.NeverGrowsWhenTypingAtEdges,
      // for markers. That is why I copied styles, that are generated for markers decoration and applied them here
      // I marked copied styles by comment // [marker-decoration-source]
      return {
        range: error.range,
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
    };

    this.previousDecorations =
      this.monacoEditorComponent.editor.deltaDecorations(
        this.previousDecorations,
        serverSqlErrors?.length > 0
          ? serverSqlErrors.map(errorToDecoration)
          : liveErrors.map(errorToDecoration)
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

  setupSQLEditorExtension(
    sqlFunctions,
    autocompleteEnabled,
    liveErrorDetectionEnabled
  ) {
    const getSuggestionsURL = new APIV2Call()
      .paths("sql/autocomplete")
      .toString();
    const authToken = localStorageUtils?.getAuthToken() || "";
    this.sqlEditorExtension = new SQLEditorExtension(
      this.getSqlContext,
      authToken,
      getSuggestionsURL,
      sqlFunctions
    );
    this.setupAutocompletion(autocompleteEnabled);
    this.setupLiveErrorDetection(liveErrorDetectionEnabled);
  }

  setupAutocompletion(enabled) {
    this.removeAutoCompletion(enabled);
    const editor = this.editor;

    if (!enabled || !editor) return;

    this.autoCompleteDisposers.push(
      this.monaco.languages.registerCompletionItemProvider(
        language,
        this.sqlEditorExtension.completionItemProvider
      )
    );

    this.autoCompleteDisposers.push(
      this.editor.onDidType((text) => {
        if (text === "\n" || text === ";" || text === " ") {
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
  }

  setupLiveErrorDetection(enabled) {
    this.removeLiveErrorDetection(enabled);
    const editor = this.editor;

    if (!enabled || !editor) return;

    this.liveErrorDetectionDisposers.push(
      this.editor.onDidChangeModelContent(async () => {
        if (this.state.liveErrors.length > 0) {
          // Clear any existing live errors before starting the async request to resynchronize them
          this.setState({ liveErrors: [] });
        }
        const getModelVersion = () => this.editor?.getModel()?.getVersionId();
        const modelVersion = getModelVersion();
        const liveErrors =
          await this.sqlEditorExtension.errorDetectionProvider.getLiveErrors(
            this.editor.getModel(),
            modelVersion
          );
        if (modelVersion === getModelVersion()) {
          if (liveErrors.length === 0 && this.state.liveErrors.length === 0) {
            return;
          }
          // Update only if we are responding to the current model's live errors request
          this.setState({ liveErrors });
        }
      })
    );
  }

  getSqlContext = () =>
    this.props.sqlContext ? this.props.sqlContext.toJS() : [];

  removeAutoCompletion(enabled) {
    if (this.autoCompleteDisposers) {
      this.autoCompleteDisposers.forEach((resource) => {
        resource.dispose();
      });
    }
    this.autoCompleteDisposers = [];

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

  removeLiveErrorDetection() {
    if (this.liveErrorDetectionDisposers) {
      this.liveErrorDetectionDisposers.forEach((resource) => {
        resource.dispose();
      });
    }
    this.liveErrorDetectionDisposers = [];
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
        rules: getSQLEditorThemeRules(theme),

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

  applySQLFunctionsToReservedKeywords = () => {
    // This will only run if functions endpoint completes after the editor lazy-load has completed
    const { sqlFunctions } = this.props;
    if (!sqlFunctions) return;

    const sqlFunctionNames = sqlFunctions.map((fn) => fn.name);
    const { language: tokenProvider } = SQLLanguage;
    tokenProvider.functions = [...sqlFunctionNames];
    this.monaco.languages.setMonarchTokensProvider(language, tokenProvider);
  };

  getSQLKeywordsAndFunctions = () => {
    const { sqlFunctions } = this.props;
    if (!sqlFunctions) {
      return [
        [],
        [...RESERVED_WORDS].filter(
          (word) => ![...RESERVED_TYPES].includes(word.toUpperCase())
        ),
      ];
    }

    const sqlFunctionNames = sqlFunctions
      .map((fn) => fn.name.toUpperCase())
      .filter((fn) => !EXCLUDE_FROM_FUNCTIONS.includes(fn)); // Assign LEFT as a keyword. Wait until semantic syntax to implement this correctly
    const sqlKeywords = [...RESERVED_WORDS].filter(
      (word) =>
        !sqlFunctionNames.includes(word.toUpperCase()) &&
        ![...RESERVED_TYPES].includes(word.toUpperCase())
    );
    return [sqlFunctionNames, sqlKeywords];
  };

  editorDidMount = (editor, monaco) => {
    this.monaco = monaco;
    this.setEditorTheme();
    editor.getDomNode()._monacoEditor = editor; // for e2e tests

    // if this is our first time using monaco it will lazy load
    // only once it's loaded can we set up languages, etc
    if (!haveLoaded) {
      const [sqlFunctionNames, sqlKeywords] = this.getSQLKeywordsAndFunctions();
      const { language: tokenProvider, conf } = SQLLanguage;
      tokenProvider.builtinVariables = [];
      tokenProvider.builtinFunctions = [];
      // TODO: limit operators to /[*+\-<>!=&|/~]/
      tokenProvider.operators = [];
      tokenProvider.keywords = sqlKeywords;
      tokenProvider.functions = sqlFunctionNames;
      tokenProvider.datatypes = [...RESERVED_TYPES];
      tokenProvider.nullValue = [NULL_VALUE];
      tokenProvider.pseudoColumns = [];

      tokenProvider.tokenizer.comments.push([/\/\/+.*/, "comment"]);
      const notationRule = tokenProvider.tokenizer.root.find(
        (rules) => String(rules[0]) === String(TOKEN_NOTATION_REGEX) // rule for regex notation cases
      );
      if (!notationRule) {
        tokenProvider.tokenizer.root.push([
          TOKEN_NOTATION_REGEX,
          getSQLTokenNotationMap(),
        ]);
      } else {
        notationRule[1] = getSQLTokenNotationMap();
      }

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
    this.setupSQLEditorExtension(
      this.props.sqlFunctions,
      this.props.autoCompleteEnabled,
      this.props.liveSyntaxErrorDetectionFlagEnabled &&
        !this.props.liveSyntaxErrorDetectionDisabled
    );

    this.fitHeightToContent();

    this.setState({ language });

    if (this._focusOnMount) {
      this.focus();
    }
    this.addKeyboardShortcuts(editor);
  };

  onKbdPreview = () => {
    this.props.setActionState({ actionState: ExploreHeaderActions.PREVIEW });
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
    this.props.setActionState({ actionState: ExploreHeaderActions.RUN });
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
    renderExtraSQLKeyboardShortcuts({
      editor,
      monaco,
      toggleExtraSQLPanel: this.props.toggleExtraSQLPanel,
      hasExtraSQLPanelContent: this.props.hasExtraSQLPanelContent,
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
    const monaco = this.monaco;
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

const isLiveSyntaxErrorDetectionFlagEnabled = (state) =>
  getSupportFlags(state)[LIVE_SYNTAX_ERROR_DETECTION];

const mapStateToProps = (state) => ({
  liveSyntaxErrorDetectionFlagEnabled:
    isLiveSyntaxErrorDetectionFlagEnabled(state),
});

export default connect(
  mapStateToProps,
  {
    runDatasetSql,
    previewDatasetSql,
    addNotification,
    fetchFeatureFlag,
    fetchSupportFlags,
    setActionState,
  },
  null,
  { forwardRef: true }
)(SqlEditorWithHooksProps);
