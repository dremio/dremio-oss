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
import Radium from 'radium';
import PropTypes from 'prop-types';
import domHelpers  from 'dom-helpers';
import FontIcon from 'components/Icon/FontIcon';

import { setResizeProgressState, updateSqlPartSize, toggleExploreSql } from './../../../actions/explore/ui';

const MIN_SQL_HEIGHT = 80;
const BOTTOM_OFFSET = 175; // a space reserved for table part

@Radium
export class TopSplitterContent extends Component {
  static propTypes = {
    isRawMode: PropTypes.bool,
    startDrag: PropTypes.func,
    toggleExploreSql: PropTypes.func,
    setResizeProgressState: PropTypes.func,
    updateSqlPartSize: PropTypes.func,
    locationType: PropTypes.string,
    transformType: PropTypes.string,
    children: PropTypes.oneOfType([PropTypes.node, PropTypes.array]),
    resizeLineTop: PropTypes.number,
    sqlState: PropTypes.bool.isRequired,
    sqlSize: PropTypes.number.isRequired
  };

  constructor(props) {
    super(props);
    this.startDrag = this.startDrag.bind(this);
    this.stopDrag = this.stopDrag.bind(this);
    this.doDrag = this.doDrag.bind(this);
    this.state = {
      resizeLineTop: props.sqlSize,
      isDragInProgress: false
    };
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.sqlSize !== this.props.sqlSize) {
      this.setState({
        resizeLineTop: nextProps.sqlSize
      });
    }
  }

  componentDidUpdate(props, state) {
    if (this.state.isDragInProgress && !state.isDragInProgress) {
      document.addEventListener('mousemove', this.doDrag);
      document.addEventListener('mouseup', this.stopDrag);
    } else if (!this.state.isDragInProgress && state.isDragInProgress) {
      document.removeEventListener('mousemove', this.doDrag);
      document.removeEventListener('mouseup', this.stopDrag);
    }
  }

  getHeight(sqlState, sqlSize) {
    if (!sqlState || sqlSize <= 0) {
      return 0;
    }
    return sqlSize;
  }

  doDrag(e) {
    if (!this.state.isDragInProgress) {
      return true;
    }
    e.preventDefault();
    e.stopPropagation();
    const prevSqlHeight = this.startHeight;
    const moveInPx = e.pageY - this.startTop;
    const nextHeight = prevSqlHeight + moveInPx;

    this.setState({
      resizeLineTop: Math.max(nextHeight, MIN_SQL_HEIGHT)
    });
  }

  stopDrag() {
    const documentHeight = domHelpers.ownerWindow().innerHeight;
    const maxHeight = (documentHeight - BOTTOM_OFFSET);
    let height = this.state.resizeLineTop;

    if (this.state.resizeLineTop > maxHeight) {
      height = maxHeight;
    }
    if (!this.state.isDragInProgress) {
      return;
    }
    this.setState({
      isDragInProgress: false,
      // see https://dremio.atlassian.net/browse/DX-7038
      resizeLineTop: height <= MIN_SQL_HEIGHT ? this.props.sqlSize : height
    });

    if (height <= MIN_SQL_HEIGHT) {
      this.props.toggleExploreSql();
    } else {
      this.props.updateSqlPartSize(height);
    }
    this.props.setResizeProgressState(false);
  }

  startDrag(e) {
    this.props.setResizeProgressState(true);
    this.setState({
      isDragInProgress: true
    });
    this.startTop = e.pageY;
    this.startHeight = this.props.sqlSize;
  }

  render() {
    const { sqlState, sqlSize } = this.props;
    const { isDragInProgress, resizeLineTop } = this.state;
    const height = this.getHeight(sqlState, sqlSize);
    const hideDragBar = !sqlState || isDragInProgress;

    return (
      <div className='top-splitter-content' ref='topSplitter'
        style={[ styles.base, {
          height,
          overflow: sqlState ? null : 'hidden' // needed to not let resizer flow out of the component
        }]}>
        {this.props.children}
        <div style={[styles.separatorLine, {display: isDragInProgress ? 'block' : 'none', top: resizeLineTop + 12}]}></div>
        <div style={[styles.separatorBase, {visibility: hideDragBar ? 'hidden' : 'visible', top: resizeLineTop}]}
          onMouseDown={this.startDrag}>
          <FontIcon type='Bars' theme={styles.separator}/>
        </div>
      </div>
    );
  }
}

export default connect(null, {
  updateSqlPartSize,
  setResizeProgressState,
  toggleExploreSql
})(TopSplitterContent);

const styles = {
  base: {
    position: 'relative',
    minHeight: 0,
    backgroundColor: '#F5FCFF',
    flexShrink: 0 // do not allow to reduce a height
  },
  separatorBase: {
    position: 'absolute',
    left: '50%',
    zIndex: 10,
    width: 18,
    height: 18,
    display: 'inline-block'
  },
  separatorLine: {
    position: 'absolute',
    height: 10,
    background: '#ccc',
    cursor: 'row-resize',
    zIndex: 10,
    left: 0,
    width: '100%',
    opacity: '.6',
    display: 'block'
  },
  separator: {
    Icon: {
      fontSize: 17,
      cursor: 'row-resize',
      color: '#CACACA'
    }
  }
};
