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

import { loadHelpGridData } from 'actions/explore/sqlActions';
import DragSource from 'components/DragComponents/DragSource';
import { SearchField } from 'components/Fields';

import { fixedWidthSmall, formDescription, fixedWidthBold } from 'uiTheme/radium/typography';

import './HelpFunctions.less';

@Radium
@pureRender
export class HelpFunctions extends Component {
  static propTypes = {
    dragType: PropTypes.string.isRequired,
    addFuncToSqlEditor: PropTypes.func.isRequired,
    loadHelpGridData: PropTypes.func.isRequired,
    gridHelpData: PropTypes.object.isRequired,
    heightPanel: PropTypes.number
  };

  state = {
    expandId: '',
    filter: ''
  };

  componentWillMount() {
    this.loadData(this.state.filter);
  }

  componentDidUpdate() {
    if (this.refs.activeItem) {
      this.scrollTop(this.refs.activeItem);
    }
  }

  loadData = (value) => {
    this.setState({ filter: value });
    this.props.loadHelpGridData(value, 'helpfunc');
  }

  scrollTop = (activeItem) => {
    const currentTop = $(activeItem).position().top;
    const scrollTop = $(this.refs.scroll).scrollTop() + currentTop;
    $(this.refs.scroll).animate({ scrollTop }, 'fast');
  }

  expandFuncInfo = (id) => {
    if (this.state.expandId === id) {
      this.setState({ expandId: '' });
    } else {
      this.setState({ expandId: id });
    }
  }

  renderFunctionRow(item) {
    const isActiveItem = this.state.expandId === item.get('id');
    const activeDes = isActiveItem ? styles.insideTextActive : {};
    const activeFuncStyle = isActiveItem ? styles.funcActive : {};
    const example = item.get('example')
      ? <div className='example' style={[styles.insideDescription, styles.example]}>{item.get('example')}</div>
      : null;
    const nameToInsert = item.get('name').toUpperCase();
    return (
      <div
        className='func_for_sql_editor' style={[styles.func, activeFuncStyle]}
        key={`helpFunction-${item.get('id')}`}
        ref={isActiveItem ? 'activeItem' : undefined}
        onMouseUp={e => e.preventDefault()}
        onDoubleClick={this.props.addFuncToSqlEditor.bind(this, nameToInsert, item.get('args'))}
        onClick={this.expandFuncInfo.bind(this, item.get('id'))}>
        <DragSource
          dragType={this.props.dragType}
          id={nameToInsert}
          args={item.get('args')}
          key={item.get('id')}>
          <div style={{display: 'flex', alignItems: 'baseline'}}>
            <div style={fixedWidthSmall}>{nameToInsert}</div>
            <div style={[fixedWidthSmall, {marginLeft: 5}]}
              dangerouslySetInnerHTML={{__html: `${item.get('args')}`}}/>
            <div style={styles.arrow}>></div>
            <div style={fixedWidthBold}>{item.get('returnType')}</div>
          </div>
        </DragSource>
        <div className='inside-text' style={[styles.insideText, activeDes]}>
          <div style={styles.insideDescription} dangerouslySetInnerHTML={{__html: item.get('description')}}></div>
          {example}
        </div>
      </div>
    );
  }

  renderFunctionsList() {
    const funcs = this.props.gridHelpData.getIn(['items', 'funcs']) || Immutable.List();

    return (
      funcs.map((item) => {
        return this.renderFunctionRow(item);
      })
    );
  }

  render() {
    return (
      <div className='content-tabs-wrap'>
        <SearchField
          style={styles.searchWrap}
          inputStyle={styles.searchInput}
          searchIconTheme={styles.searchIcon}
          placeholder={la('Search functions...')}
          onChange={this.loadData}
          value={this.state.filter}
        />
        <div className='content-wrap' style={styles.contentWrap}>
          <div style={{height: this.props.heightPanel, overflowY: 'scroll'}} ref='scroll'>
            {this.renderFunctionsList()}
          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  searchWrap: {
    margin: 2
  },
  searchInput: {
    padding: '5px 10px'
  },
  searchIcon: {
    Icon: {
      width: 22,
      height: 22
    },
    Container: {
      cursor: 'pointer',
      position: 'absolute',
      right: 3,
      top: 0,
      bottom: 0,
      margin: 'auto',
      width: 22,
      height: 22
    }
  },
  contentWrap: {
    position: 'absolute',
    top: 30,
    bottom: 0,
    left: 0,
    right: 0,
    overflowY: 'hidden'
  },
  func: {
    fontFamily: 'Roboto, sans-serif',
    padding: '3px 8px',
    clear: 'both',
    position: 'relative',
    height: 'auto',
    cursor: 'move'
  },
  insideText: {
    padding: '1px 0',
    overflow: 'hidden',
    maxHeight: 0
  },
  insideTextActive: {
    maxHeight: 150
  },
  funcActive: {
    background: '#fff5dc'
  },
  insideDescription: {
    ...formDescription,
    marginTop: 5,
    marginLeft: 5
  },
  example: {
    ...fixedWidthSmall,
    whiteSpace: 'pre-line'
  },
  arrow: {
    margin: '-1px 10px',
    fontWeight: 200,
    fontSize: 13
  }
};

function mapStateToProps(state) {
  return {
    gridHelpData: state.explore.sqlActions.get('gridHelpData')
  };
}

export default connect(mapStateToProps, {
  loadHelpGridData
})(HelpFunctions);
