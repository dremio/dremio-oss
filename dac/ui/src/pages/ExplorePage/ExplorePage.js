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
import { Component } from 'react';
import Immutable from 'immutable';
import ReactDOM from 'react-dom';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import MainHeader from 'components/MainHeader';
import { hashHeightTopSplitter } from 'constants/explorePage/heightTopSplitter.js';

import './ExplorePage.less';
import HistoryLineController from './components/Timeline/HistoryLineController';
import ExplorePageContentWrapper from './subpages/ExplorePageContentWrapper';

const GRID_TABLE_MARGIN = 15;

@pureRender
class ExplorePage extends Component {
  static propTypes = {
    pageType: PropTypes.string,
    dataset: PropTypes.instanceOf(Immutable.Map),
    history: PropTypes.instanceOf(Immutable.Map),
    setResizeProgressState: PropTypes.func,
    location: PropTypes.object.isRequired,
    rightTreeVisible: PropTypes.bool.isRequired,
    sqlState: PropTypes.bool,
    sqlSize: PropTypes.number,
    updateSqlPartSize: PropTypes.func.isRequired,
    toggleRightTree: PropTypes.func.isRequired,
    toggleAccessModal: PropTypes.func,
    showEditColumnsModal: PropTypes.func,
    updateGridSizes: PropTypes.func.isRequired,
    isResizeInProgress: PropTypes.bool,
    exploreViewState: PropTypes.instanceOf(Immutable.Map),
    style: PropTypes.object
  };

  state = {
    isError: false,
    errorData: new Immutable.Map()
  };

  componentWillMount() {
    this.initSqlEditor(this.props);
  }

  componentDidMount() {
    this.resize();
    $(window).on('resize', this.resize);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.pageType !== this.props.pageType) {
      this.initSqlEditor(nextProps);
    }
  }

  componentWillUnmount() {
    $(window).off('resize', this.resize);
  }

  initSqlEditor(props) {
    const { pageType, location } = props;
    const key = !pageType || pageType === 'graph' ? 'default' : pageType;
    if (key !== 'default') {
      return;
    }

    const newSize = hashHeightTopSplitter[location.query.type || key];
    props.updateSqlPartSize(newSize);
  }

  startDrag() {}

  resize = () => {
    const gridNode = ReactDOM.findDOMNode(this.refs.ExplorePageView);
    const historyNode = ReactDOM.findDOMNode(this.refs.historyLine);
    const tableNode = ReactDOM.findDOMNode(this.refs.gridTableWrap);
    const ExplorePageSizes = new Immutable.Map({
      ExplorePageWidth: gridNode.offsetWidth,
      ExplorePageHeight: gridNode.offsetHeight,
      gridTableMargin: GRID_TABLE_MARGIN,
      gridTableHeight: gridNode.offsetHeight - (tableNode && tableNode.offsetTop || 0),
      historyLineWidth: historyNode.offsetWidth
    });
    this.props.updateGridSizes(ExplorePageSizes);
  }

  render() {
    const { dataset, history, isResizeInProgress } = this.props;
    const selectState = isResizeInProgress ? 'text' : 'none';
    const cursor = isResizeInProgress ? 'row-resize' : 'initial';
    const dragStyle = {
      MozUserSelect: selectState,
      WebkitUserSelect: selectState,
      MsUserSelect: selectState
    };

    // Note the DocumentTitle for this page lives in ExploreInfoHeader

    return (
      <div id='grid-page'
        ref='ExplorePageView'
        style={{...this.props.style, dragStyle, cursor}}>
        <MainHeader />
        <div className='grid-wrap'>
          <ExplorePageContentWrapper
            pageType={this.props.pageType}
            dataset={dataset}
            location={this.props.location}
            startDrag={this.startDrag}
            rightTreeVisible={this.props.rightTreeVisible}
            sqlSize={this.props.sqlSize}
            sqlState={this.props.sqlState}
            isError={this.state.isError}
            errorData={this.state.errorData}
            updateSqlPartSize={this.props.updateSqlPartSize}
            toggleRightTree={this.props.toggleRightTree}
            toggleAccessModal={this.props.toggleAccessModal}
            showEditColumnsModal={this.props.showEditColumnsModal}
            exploreViewState={this.props.exploreViewState}
          />
          <HistoryLineController
            dataset={dataset}
            history={history}
            location={this.props.location}
            ref='historyLine'/>
        </div>
      </div>
    );
  }
}

export default ExplorePage;
