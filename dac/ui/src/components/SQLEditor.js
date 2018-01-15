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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import MonacoEditor from 'react-monaco-editor';
import * as SQLLanguage from 'monaco-editor/dev/vs/basic-languages/src/sql';

import {RESERVED_WORDS} from 'utils/pathUtils';

import './SQLEditor.css';

let haveLoaded = false;
let SnippetController;

export default class SQLEditor extends PureComponent {
  static propTypes = {
    height: PropTypes.number.isRequired,
    defaultValue: PropTypes.string,
    onChange: PropTypes.func

    // all others pass thru
  }

  reseting = false;
  monacoEditorComponent = null;
  monaco = null;

  state = {
    language: 'sql'
  }

  componentDidMount() {
    if (this.props.defaultValue !== undefined) {
      this.resetValue();
    }
  }

  // do this in componentDidUpdate so it only happens once mounted.
  componentDidUpdate(prevProps) {
    if (this.props.defaultValue !== prevProps.defaultValue) {
      this.resetValue();
    }
  }

  handleChange = (...args) => {
    if (!this.reseting) {
      this.props.onChange(...args);
    }
  }

  resetValue() {
    if (!this.monacoEditorComponent.editor) return;
    this.reseting = true;
    try {
      this.monacoEditorComponent.editor.setValue(this.props.defaultValue || '');
    } finally {
      this.reseting = false;
    }
  }

  editorDidMount = (editor, monaco) => {
    this.monaco = monaco;

    const language = 'dremio-sql';

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

      monaco.languages.register({ id: language });
      monaco.languages.setMonarchTokensProvider(language, tokenProvider);
      monaco.languages.setLanguageConfiguration(language, conf);

      SnippetController = window.require('vs/editor/contrib/snippet/browser/snippetController2').SnippetController2;

      haveLoaded = true;
    }

    this.setState({language});
  }

  insertSnippet() {
    SnippetController.get(this.monacoEditorComponent.editor).insert(...arguments);
  }

  render() {
    return <MonacoEditor
      {...this.props}
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
        minimap: {
          enabled: false
        }
      }}
      requireConfig={{url: '/vs/loader.js', paths: {vs: '/vs'}}}
    />;
  }
}
