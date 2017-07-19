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

import ReactDOM from 'react-dom';
import $ from 'jquery';
import Radium from 'radium';
import Immutable from 'immutable';
import deepEqual from 'deep-equal';
import { debounce } from 'lodash/function';

import 'codemirror/mode/sql/sql';
import 'codemirror/lib/codemirror.css';
import 'codemirror/addon/hint/show-hint.css';
import 'codemirror/theme/mdn-like.css';

import exploreUtils from 'utils/explore/exploreUtils';
import codeMirrorUtils from 'utils/CodeMirrorUtils';
import { splitFullPath, constructFullPath } from 'utils/pathUtils';

import FontIcon from 'components/Icon/FontIcon';
import Modal from 'components/Modals/Modal';
import CodeMirror from 'components/CodeMirror';

import { body } from 'uiTheme/radium/typography';
import { MARGIN_SQL_EDITOR } from 'uiTheme/radium/sizes.js';

import SelectContextForm from '../forms/SelectContextForm';

import './SqlAutoComplete.less';

const DEFAULT_HEIGHT = 150;
const UPDATE_HEIGHT_DELAY = 100;
const DEBOUNCE_UPDATE_CODE = 250;
const DEFAULT_CONTEXT = '<none>';

const CODE_MIRROR_OPTIONS = {
  lineWrapping: true
};

@Radium
export default class SqlAutoComplete extends Component {
  static propTypes = {
    onChange: PropTypes.func,
    pageType: PropTypes.oneOf(['details', 'recent']),
    defaultValue: PropTypes.string,
    isGrayed: PropTypes.bool,
    onFocus: PropTypes.func,
    context: PropTypes.instanceOf(Immutable.List),
    name: PropTypes.string,
    sqlSize: PropTypes.number,
    datasetsPanel: PropTypes.bool,
    funcHelpPanel: PropTypes.bool,
    changeQueryContext: PropTypes.func,
    style: PropTypes.object
  };

  static contextTypes = {
    location: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);

    this.getWord = this.getWord.bind(this);
    this.handleClickEditContext = this.handleClickEditContext.bind(this);
    this.renderSelectContextModal = this.renderSelectContextModal.bind(this);
    this.hideSelectContextModal = this.hideSelectContextModal.bind(this);
    this.updateContext = this.updateContext.bind(this);
    this.updateCode = debounce(this.updateCode, DEBOUNCE_UPDATE_CODE);

    this.state = {
      showSelectContextModal: false
    };
  }

  componentDidMount() {
    this.setHeightOfEditor(this.props.sqlSize - MARGIN_SQL_EDITOR);
    this.editor = this.refs.editor.editor;
    this.posForDrop = this.editor.coordsChar({left: 0, top: 0});
  }

  componentWillReceiveProps(nextProps, nextContext) {
    if (nextProps.sqlSize !== this.props.sqlSize && this.refs.editor &&
       nextProps.pageType === undefined) {
      clearTimeout(this.timerForheight);
      this.timerForheight = setTimeout(() => {
        this.setHeightOfEditor(nextProps.sqlSize - MARGIN_SQL_EDITOR);
      }, UPDATE_HEIGHT_DELAY);
    }
  }

  shouldComponentUpdate(nextProps, nextState, nextContext) {
    return (
      nextProps.defaultValue !== this.props.defaultValue ||
      nextProps.context !== this.props.context ||
      nextContext.location.query.version !== this.context.location.query.version ||
      nextProps.funcHelpPanel !== this.props.funcHelpPanel ||
      nextProps.datasetsPanel !== this.props.datasetsPanel ||
      nextProps.isGrayed !== this.props.isGrayed ||
      !deepEqual(nextState, this.state));
  }

  componentWillUpdate() {
    const height = this.props.sqlSize === null ? DEFAULT_HEIGHT : this.props.sqlSize - MARGIN_SQL_EDITOR;
    this.cursor = this.editor && this.editor.getCursor();
    this.setHeightOfEditor(height);
  }

  componentWillUnmount() {
    if (this.updateCode.cancel) {
      this.updateCode.cancel();
    }
  }

  getWord(offset) {
    let word = this.editor.getRange(offset.anchor, offset.head);
    word = word.trim();
    return word;
  }

  setHeightOfEditor(height) {
    $(ReactDOM.findDOMNode(this.refs.editor)).find('.cm-s-default').height(height);
    $(ReactDOM.findDOMNode(this.refs.editor)).find('.CodeMirror').height(height);
  }

  focus() {
    this.editor.focus();
  }

  resetValue() {
    this.refs.editor.resetValue();
  }

  handleChange = () => {
    this.updateCode();
  }

  handleFocus = () => {
    if (this.props.onFocus) {
      this.props.onFocus(this.editor.doc.getValue());
    }
  }

  handleBlur = () => {
    this.updateCode.flush();
  }

  handleDragover = (editor, e) => {
    const pos = editor.coordsChar({left: e.x, top: e.pageY});
    this.posForDrop = pos;
  }

  handleClickEditContext() {
    this.setState({ showSelectContextModal: true });
  }

  hideSelectContextModal() {
    this.setState({ showSelectContextModal: false });
  }

  updateContext(resource) {
    this.props.changeQueryContext(Immutable.fromJS(resource.context && splitFullPath(resource.context)));
    this.hideSelectContextModal();
  }

  insertFullPathAtDrop(nameOrPathList) {
    this.insertFullPathAtPosition(nameOrPathList, this.posForDrop);
  }

  insertFullPathAtCursor(nameOrPathList) {
    this.insertFullPathAtPosition(nameOrPathList, this.editor.getCursor());
  }

  insertFullPathAtPosition(nameOrPathList, position) {
    // fullPath should be escaped with constructedFullPath
    const finalText = typeof nameOrPathList === 'string' ?
      exploreUtils.escapeFieldNameForSQL(nameOrPathList) : constructFullPath(nameOrPathList);

    this.editor.doc.setValue(
      codeMirrorUtils.insertTextAtPos(this.editor, finalText, position)
    );
    this.setState({
      code: this.editor.doc.getValue()
    });
    this.props.onChange(this.editor.doc.getValue());
  }

  insertFunction(code, insertToEnd, args) {
    if (insertToEnd) {
      this.editor.replaceRange(` ${code}()`, {line: Infinity});
    } else {
      const selectedRange = { from: this.editor.getCursor(true), to: this.editor.getCursor(false) };
      if (selectedRange.to.ch - selectedRange.from.ch) {
        const newcode = exploreUtils.describeSqlFuncs(code, args);
        this.editor.replaceRange(newcode, selectedRange.from, selectedRange.to);
      } else {
        const word = this.editor.findWordAt(this.posForDrop);
        const newcode = `${exploreUtils.describeSqlFuncs(code, args)}${this.getWord(word)} `;
        this.editor.replaceRange(newcode, word.anchor, word.head);
      }
    }
    this.setState({
      code: this.editor.doc.getValue()
    });
    this.props.onChange(this.editor.doc.getValue());
  }

  updateCode() {
    if (this.props.onChange) {
      this.props.onChange(this.editor.doc.getValue());
    }
  }

  renderSelectContextModal() {
    if (this.state.showSelectContextModal) {
      const contextValue = constructFullPath(this.props.context);
      return <Modal
        isOpen
        hide={this.hideSelectContextModal}
        size='small'
        title={'Select Context'}>
        <SelectContextForm
          onFormSubmit={this.updateContext}
          onCancel={this.hideSelectContextModal}
          initialValues={{context: contextValue}}
        />
      </Modal>;
    }
  }

  renderContext() {
    const contextValue = this.props.context ? constructFullPath(this.props.context, true) : DEFAULT_CONTEXT;
    const showContext = this.props.pageType === 'details';
    return (
      <div style={[styles.context, showContext && {display: 'none'}]} onClick={this.handleClickEditContext}>
        <span className='context' style={[body, styles.contextInner]}>Context: {contextValue}</span>
        <FontIcon type='Edit' theme={styles.editIcon} hoverType='EditActive'/>
      </div>
    );
  }

  render() {
    const { datasetsPanel, funcHelpPanel, isGrayed } = this.props;
    const { query } = this.context.location;
    const widthSqlEditor = funcHelpPanel || datasetsPanel ? styles.smallerSqlEditor : {};
    return (
      <div className='sql-autocomplete'
        name={this.props.name}
        style={[body, styles.base, widthSqlEditor,
          isGrayed && {opacity: 0.4, pointerEvents: 'none'}, this.props.style]}>
        {this.renderSelectContextModal()}
        <CodeMirror
          ref='editor'
          onChange={this.handleChange}
          onFocus={this.handleFocus}
          onBlur={this.handleBlur}
          onDragover={this.handleDragover}
          defaultValue={this.props.defaultValue}
          options={CODE_MIRROR_OPTIONS}/>
        { query.type !== 'transform' &&  this.renderContext() }
      </div>
    );
  }
}

const styles = {
  base: {
    position: 'relative',
    width: '100%',
    transition: 'all .3s',
    backgroundColor: '#fff',
    cursor: 'text'
  },
  smallerSqlEditor: {
    width: 'calc(50% - 7px)', // 7px - indents from the edge of the screen
    borderRight: 'none'
  },
  tooltip: {
    padding: '5px 10px 5px 10px',
    backgroundColor: '#f2f2f2'
  },
  messageStyle: {
    position: 'absolute',
    bottom: '0px',
    zIndex: 1000
  },
  editIcon: {
    Container: {
      height: 15,
      width: 20,
      position: 'relative',
      display: 'flex',
      alignItems: 'center',
      cursor: 'pointer'
    }
  },
  context: {
    cursor: 'pointer',
    position: 'absolute',
    backgroundColor: 'rgba(216,218,222,0.50)',
    border: '1px solid #D8DADE',
    height: 20,
    display: 'flex',
    alignItems: 'center',
    padding: 3,
    bottom: 0,
    right: 0
  },
  contextInner: {
    color: '#B5BAC1',
    fontSize: '10px'
  },
  contextInput: {
    minWidth: 139,
    height: 19,
    outline: 'none',
    paddingLeft: 3
  }
};
