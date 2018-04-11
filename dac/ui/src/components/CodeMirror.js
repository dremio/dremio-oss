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
import invariant from 'invariant';

import codeMirror from 'codemirror';
import './CodeMirror-sqlMode';

const CM_EVENTS = [
  'change', 'changes', 'beforeChange', 'cursorActivity', 'keyHandled', 'inputRead', 'electricInput',
  'beforeSelectionChange', 'viewportChange', 'swapDoc', 'gutterClick', 'gutterContextMenu', 'focus', 'blur', 'scroll',
  'refresh', 'optionChange', 'scrollCursorIntoView', 'update', 'renderLine', 'mousedown', 'dblclick', 'touchstart',
  'contextmenu', 'keydown', 'keypress', 'keyup', 'cut', 'copy', 'paste', 'dragstart', 'dragenter', 'dragover',
  'dragleave', 'drop'
];

const PROP_NAME_TO_CM_EVENT = CM_EVENTS.reduce((prev, event) => {
  prev[`on${event[0].toUpperCase()}${event.slice(1)}`] = event;
  return prev;
}, {});

const CM_EVENT_PROP_TYPES = Object.keys(PROP_NAME_TO_CM_EVENT).reduce((prev, propName) => {
  prev[propName] = PropTypes.func;
  return prev;
}, {});

const DEFAULT_OPTIONS = {
  mode: 'text/x-dremiosql',
  theme: 'mdn-like'
};

/**
 * React wrapper for codemirror code editor.
 *
 * Note: previously we used react-codemirror but it's incomplete, unmaintained and not that complicated,
 * so we use a custom wrapper instead.
 *
 * One change is calling setValue in componentDidUpdate instead of receiveProps. This prevents ignoring updates that
 * happen before componentDidMount.
 */
export default class CodeMirror extends PureComponent {

  static propTypes = {
    defaultValue: PropTypes.string,
    options: PropTypes.object,
    ...CM_EVENT_PROP_TYPES,
    codeMirror: PropTypes.func // for testing
  }

  static defaultProps = {
    options: {},
    codeMirror
  }

  editor = null;
  codeMirrorEl = null;

  componentDidMount() {
    const { options } = this.props;
    this.editor = this.props.codeMirror(this.codeMirrorEl, {...DEFAULT_OPTIONS, ...options});

    Object.keys(PROP_NAME_TO_CM_EVENT).forEach((propName) => {
      if (this.props[propName]) {
        // special case onChange to prevent change event when reseting
        if (propName === 'onChange') {
          this.editor.on('change', this.handleChange);
        } else {
          this.editor.on(PROP_NAME_TO_CM_EVENT[propName], this.props[propName]);
        }
      }
    });

    if (this.props.defaultValue !== undefined) {
      this.resetValue();
    }
  }

  // do this in componentDidUpdate so it only happens once mounted.
  componentDidUpdate(prevProps) {
    if (this.props.defaultValue !== prevProps.defaultValue) {
      this.resetValue();
    }
    invariant(this.props.options === prevProps.options, 'changing options is not implemented');
  }

  componentWillUnmount() {
    if (this.editor && this.editor.toTextArea) {
      this.editor.toTextArea(); // free cm from memory
      this.editor = null;
    }
  }

  handleChange = (cm, event) => {
    if (!this.reseting) {
      this.props.onChange(cm, event);
    }
  }

  resetValue() {
    this.reseting = true;
    try {
      this.editor.setValue(this.props.defaultValue || '');
    } finally {
      this.reseting = false;
    }
  }

  render() {
    return (
      <div className='ReactCodeMirror' ref={(ref) => this.codeMirrorEl = ref}/>
    );
  }
}
