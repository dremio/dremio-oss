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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import MonacoEditor from 'react-monaco-editor';
import Immutable from 'immutable';
import * as SQLLanguage from 'monaco-editor/dev/vs/basic-languages/src/sql';

import {RESERVED_WORDS} from 'utils/pathUtils';
import { runDatasetSql, previewDatasetSql } from 'actions/explore/dataset/run';
import { SQLAutoCompleteProvider } from './SQLAutoCompleteProvider';
import './SQLEditor.less';

let haveLoaded = false;
let SnippetController;
const language = 'dremio-sql';


const staticPropTypes = {
  height: PropTypes.number.isRequired, // pass-thru
  defaultValue: PropTypes.string, // pass-thru
  onChange: PropTypes.func,
  errors: PropTypes.instanceOf(Immutable.List),
  readOnly: PropTypes.bool,
  fitHeightToContent: PropTypes.bool,
  maxHeight: PropTypes.number, // is only applicable for fitHeightToContent case
  contextMenu: PropTypes.bool,
  autoCompleteEnabled: PropTypes.bool,
  sqlContext: PropTypes.instanceOf(Immutable.List),
  runDatasetSql: PropTypes.func,
  previewDatasetSql: PropTypes.func
};

const checkHeightAndFitHeightToContentFlags = (props, propName, componentName) => {
  if (props.fitHeightToContent) {
    if (props.height !== undefined) {
      return new Error('Height must not be provided if fitHeightToContent property set to true');
    }
  } else {
    return PropTypes.checkPropTypes(staticPropTypes, props, propName, componentName); // reuse standard prop types check
  }
};

export class SQLEditor extends PureComponent {
  static propTypes = {
    ...staticPropTypes,
    height: checkHeightAndFitHeightToContentFlags // pass-thru

    // all others pass thru
  }

  reseting = false;
  monacoEditorComponent = null;
  monaco = null;
  editor = null;
  previousDecorations = [];
  autoCompleteResources = []; // will store onDidTypeListener and auto completion provider for dispose purposes
  _focusOnMount = false;

  state = {
    language: 'sql'
  }

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
    if (this.props.errors !== prevProps.errors) {
      this.applyDecorations();
    }
    if (this.props.fitHeightToContent && this.props.value !== prevProps.value) {
      this.fitHeightToContent();
    }

    if (this.props.autoCompleteEnabled !== prevProps.autoCompleteEnabled) {
      this.setAutocompletion(this.props.autoCompleteEnabled);
    }
  }

  componentWillUnmount() {
    this.removeAutoCompletion();
  }

  handleChange = (...args) => {
    if (!this.reseting) {
      // if there are decorations, remove them
      if (this.monacoEditorComponent && this.monacoEditorComponent.editor) {
        this.previousDecorations = this.monacoEditorComponent.editor.deltaDecorations(this.previousDecorations, []);
      }
      this.props.onChange(...args);
    }
  }

  resetValue() {
    if (!this.monacoEditorComponent.editor) return;
    this.reseting = true;
    try {
      this.monacoEditorComponent.editor.setValue(this.props.defaultValue || '');
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
    if (!this.monacoEditorComponent || !this.monacoEditorComponent.editor) return;

    const monaco = this.monaco;
    const errors = this.props.errors;
    if (!errors) {
      this.previousDecorations = this.monacoEditorComponent.editor.deltaDecorations(this.previousDecorations, []);
      return;
    }

    // todo: filter errors to known types

    const decorations = errors.toJS().filter(error => error.range).map(error => {
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
          linesDecorationsClassName: 'dremio-error-line',
          stickiness: monaco.editor.TrackedRangeStickiness.NeverGrowsWhenTypingAtEdges,
          className: 'redsquiggly', // [marker-decoration-source]
          overviewRuler: { // [marker-decoration-source]
            color: 'rgba(255,18,18,0.7)', // [marker-decoration-source] // if change this value, also change @monaco-error variable in color-schema.scss
            darkColor: 'rgba(255,18,18,0.7)', // [marker-decoration-source]
            hcColor: 'rgba(255,50,50,1)', // [marker-decoration-source]
            position: monaco.editor.OverviewRulerLane.Right // [marker-decoration-source]
          }
        }
      };
    });

    this.previousDecorations = this.monacoEditorComponent.editor.deltaDecorations(this.previousDecorations, decorations);
  }

  fitHeightToContent() {
    const {
      fitHeightToContent,
      maxHeight
    } = this.props;

    if (!fitHeightToContent) return;

    const editor = this.monacoEditorComponent && this.monacoEditorComponent.editor;

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

    const horizontalScrollbarHeight = configuration.layoutInfo.horizontalScrollbarHeight;

    const editorHeight = contentHeight + horizontalScrollbarHeight;
    const defaultHeight = lineHeight * (this.minHeight || 0) + horizontalScrollbarHeight;
    return Math.max(defaultHeight, editorHeight);
  }

  setAutocompletion(enabled) {
    this.removeAutoCompletion();
    const editor = this.editor;

    if (!enabled || !editor) return;

    this.autoCompleteResources.push(this.monaco.languages
      .registerCompletionItemProvider(language, SQLAutoCompleteProvider(this.monaco, this.getSqlContext)));

    this.autoCompleteResources.push(this.editor.onDidType(text => {
      if (!/\s/.test(text)) { // call autocomplete only for not whitespace string
        editor.trigger('dremio autocomplete request', 'editor.action.triggerSuggest', {});
      }
    }));
  }

  getSqlContext = () => this.props.sqlContext ? this.props.sqlContext.toJS() : [];

  removeAutoCompletion() {
    if (this.autoCompleteResources) {
      this.autoCompleteResources.forEach(resource => {
        resource.dispose();
      });
    }
    this.autoCompleteResources = [];
  }

  editorDidMount = (editor, monaco) => {
    this.monaco = monaco;
    editor.getDomNode()._monacoEditor = editor; // for e2e tests

    // if this is our first time using monaco it will lazy load
    // only once it's loaded can we set up languages, etc
    if (!haveLoaded) {
      const {language: tokenProvider, conf} = SQLLanguage;
      tokenProvider.builtinVariables = [];
      tokenProvider.keywords = [...RESERVED_WORDS];

      // currently mixed into .keywords due to RESERVED_WORDS; todo: factor out
      tokenProvider.operators = [];
      tokenProvider.builtinFunctions = [];
      tokenProvider.builtinVariables = [];
      tokenProvider.pseudoColumns = [];

      // todo:
      // limit operators to /[*+\-<>!=&|/~]/
      tokenProvider.tokenizer.comments.push([/\/\/+.*/, 'comment']);

      monaco.languages.register({ id: language });
      monaco.languages.setMonarchTokensProvider(language, tokenProvider);
      monaco.languages.setLanguageConfiguration(language, conf);

      SnippetController = window.require('vs/editor/contrib/snippet/browser/snippetController2').SnippetController2;

      haveLoaded = true;
    }

    // these are just for debugging, so it's okay if there's a change they override other active instances
    window.dremioEditor = editor;
    window.monaco = monaco;

    this.editor = editor;
    this.applyDecorations();
    this.setAutocompletion(this.props.autoCompleteEnabled);

    this.fitHeightToContent();

    this.setState({language});

    if (this._focusOnMount) {
      this.focus();
    }
    this.addKeyboardShortcuts(editor);
  };

  onKbdPreview = (editor) => {
    this.props.previewDatasetSql();
  };

  onKbdRun = (editor) => {
    this.props.runDatasetSql();
  };

  addKeyboardShortcuts = (editor) => {
    const monaco = this.monaco;
    editor.addAction({
      id: 'keys-preview',
      label: 'Preview',
      keybindings: [ monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter ], // eslint-disable-line no-bitwise
      precondition: null,
      keybindingContext: null,
      run: this.onKbdPreview
    });
    editor.addAction({
      id: 'keys-run',
      label: 'Run',
      keybindings: [ monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.Enter ], // eslint-disable-line no-bitwise
      precondition: null,
      keybindingContext: null,
      run: this.onKbdRun
    });

  };

  insertSnippet() {
    SnippetController.get(this.monacoEditorComponent.editor).insert(...arguments);
  }

  render() {
    const {
      onChange,
      errors,
      readOnly,
      contextMenu,
      fitHeightToContent, // here to not pass it in monaco editor, as it does not support it
       ...monacoProps} = this.props;

    return (
      // div wrapper is required for FF and IE. Without it a editor has uncontrolled grow on jobs page.
      <div>
        <MonacoEditor
          {...monacoProps}
          onChange={this.handleChange}
          editorDidMount={this.editorDidMount}
          ref={(ref) => this.monacoEditorComponent = ref}
          width='100%'
          language={this.state.language}
          theme='vs'
          options={{
            wordWrap: 'on',
            lineNumbersMinChars: 3,
            scrollBeyondLastLine: false,
            scrollbar: {vertical: 'visible', useShadows: false},
            automaticLayout: true,
            lineDecorationsWidth: 12,
            minimap: {
              enabled: false
            },
            suggestLineHeight: 25,
            readOnly,
            contextmenu: contextMenu // a case is important here
          }}
          requireConfig={{url: '/vs/loader.js', paths: {vs: '/vs'}}}
        />
      </div>
    );
  }
}

export default connect(null, {
  runDatasetSql,
  previewDatasetSql
}, null, {forwardRef: true})(SQLEditor);
