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
import PropTypes from "prop-types";
import { connect } from "react-redux";
import MonacoEditor from "react-monaco-editor";
import Immutable from "immutable";
import * as SQLLanguage from "monaco-editor/dev/vs/basic-languages/src/sql";

import { RESERVED_WORDS } from "utils/pathUtils";
import { runDatasetSql, previewDatasetSql } from "actions/explore/dataset/run";
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
  sqlContext: PropTypes.instanceOf(Immutable.List),
  customDecorations: PropTypes.array,
  runDatasetSql: PropTypes.func,
  previewDatasetSql: PropTypes.func,
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
      this.props.onChange(...args);
    }
  };

  resetValue() {
    if (!this.monacoEditorComponent.editor) return;
    this.reseting = true;
    try {
      this.monacoEditorComponent.editor.setValue(this.props.defaultValue || "");
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
    if (this.props.customTheme) {
      this.monaco.editor.defineTheme("sqlEditorTheme", {
        base: this.props.theme,
        inherit: true,
        rules: [],
        colors: {
          "editor.background": this.props.background,
        },
      });
      this.monaco.editor.setTheme("sqlEditorTheme");
      this.setState({
        theme: "sqlEditorTheme",
      });
    }
  };
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
    this.props.previewDatasetSql();
  };

  onKbdRun = () => {
    this.props.runDatasetSql();
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
    // Disable Autocomplete details slide out
    editor.addCommand(monaco.KeyMod.WinCtrl | monaco.KeyCode.Space, () => null);
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
          options={{
            wordWrap: "on",
            lineNumbersMinChars: 3,
            scrollBeyondLastLine: false,
            scrollbar: { vertical: "visible", useShadows: false },
            automaticLayout: true,
            lineDecorationsWidth: 12,
            fontSize: 14,
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

export default connect(
  null,
  {
    runDatasetSql,
    previewDatasetSql,
  },
  null,
  { forwardRef: true }
)(SQLEditor);
