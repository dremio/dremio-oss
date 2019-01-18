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
import { Component } from 'react';
import { connect }   from 'react-redux';
import Immutable from 'immutable';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { injectIntl } from 'react-intl';
import {safeHtml} from 'common-tags';

import { getExploreState } from '@app/selectors/explore';
import { loadHelpGridData } from 'actions/explore/sqlActions';
import DragSource from 'components/DragComponents/DragSource';
import { SearchField } from 'components/Fields';
import FontIcon from 'components/Icon/FontIcon';

import { fixedWidthSmall, formDescription, fixedWidthBold } from 'uiTheme/radium/typography';

import './HelpFunctions.less';

@injectIntl
@Radium
@pureRender
export class HelpFunctions extends Component {
  static propTypes = {
    dragType: PropTypes.string.isRequired,
    addFuncToSqlEditor: PropTypes.func.isRequired,
    loadHelpGridData: PropTypes.func.isRequired,
    gridHelpData: PropTypes.object.isRequired,
    heightPanel: PropTypes.number,
    intl: PropTypes.object.isRequired
  };

  state = {
    expandId: '',
    filter: ''
  };

  componentWillMount() {
    this.loadData(this.state.filter);
  }

  loadData = (value) => {
    this.setState({ filter: value });
    this.props.loadHelpGridData(value);
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

    const argsHTML = !item.get('args') ? '' : safeHtml`${item.get('args')}`.replace(/{|}|\[|\]/g, (symbol) => {
      if (symbol === '[') {
        return '';
      } else if (symbol === ']') {
        return '';
      } else if (symbol === '{') {
        return '<i>';
      } else if (symbol === '}') {
        return '</i>';
      }
    });

    const descriptionHTML = !item.get('description') ? '' : safeHtml`${item.get('description')}`.replace(/{{|}}/g, (match) => {
      if (match === '{{') {
        return '<i>';
      } else if (match === '}}') {
        return '</i>';
      }
    });

    const insert = (evt) => {
      evt.stopPropagation();
      this.props.addFuncToSqlEditor(nameToInsert, item.get('args'));
    };

    return (
      <DragSource
        dragType={this.props.dragType}
        id={nameToInsert}
        args={item.get('args')}
        key={item.get('id')}>
        <div
          className='func_for_sql_editor' style={[styles.func, activeFuncStyle]}
          key={`helpFunction-${item.get('id')}`}
          ref={isActiveItem ? 'activeItem' : undefined}
          onMouseUp={e => e.preventDefault()}
          onClick={this.expandFuncInfo.bind(this, item.get('id'))}>
          <div style={{display: 'flex', alignItems: 'flex-start', paddingTop: 1}}>
            <FontIcon
              style={{ height: 10, marginTop: -6, cursor: 'pointer', marginRight: 3}} // fudge factor makes it look v-aligned better
              type='Add'
              hoverType='AddHover'
              theme={styles.addIcon}
              onClick={insert}/>
            <div style={fixedWidthSmall}>{nameToInsert}</div>
            <div style={[fixedWidthSmall, {marginLeft: 5}]}
              dangerouslySetInnerHTML={{__html: argsHTML}}/>
            <div style={styles.arrow}>{'>'}</div>
            <div style={fixedWidthBold}>{item.get('returnType')}</div>
          </div>
          <div className='inside-text' style={[styles.insideText, activeDes]}>
            <div style={styles.insideDescription} dangerouslySetInnerHTML={{__html: descriptionHTML}}></div>
            {example}
          </div>
        </div>
      </DragSource>
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
          placeholder={this.props.intl.formatMessage({ id: 'Dataset.SearchFunctions' })}
          onChange={this.loadData}
          value={this.state.filter}
        />
        <div className='content-wrap' style={styles.contentWrap}>
          <div style={{height: this.props.heightPanel, overflowY: 'scroll', paddingTop: 2}} ref='scroll'>
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
    padding: '5px 8px',
    clear: 'both',
    position: 'relative',
    height: 'auto',
    cursor: 'pointer'
  },
  insideText: {
    padding: '2px 0',
    display: 'none'
  },
  insideTextActive: {
    display: 'block'
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
    gridHelpData: getExploreState(state).sqlActions.get('gridHelpData')
  };
}

export default connect(mapStateToProps, {
  loadHelpGridData
})(HelpFunctions);
