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
import Immutable from 'immutable';
import ReactDOM from 'react-dom';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';
import { connect } from 'react-redux';

import MainHeader from 'components/MainHeader';
import { hashHeightTopSplitter } from 'constants/explorePage/heightTopSplitter.js';
import { PageTypes, pageTypesProp } from '@app/pages/ExplorePage/pageTypes';
import { clearEntities } from '@app/actions/resources/entities';

import './ExplorePage.less';
import HistoryLineController from './components/Timeline/HistoryLineController';
import ExplorePageContentWrapper from './subpages/ExplorePageContentWrapper';

const GRID_TABLE_MARGIN = 15;
const EXPLORE_PAGE_MIN_HEIGHT = 700;

@pureRender
export class ExplorePageView extends Component {
  static propTypes = {
    pageType: pageTypesProp,
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
    style: PropTypes.object,
    onUnmount: PropTypes.func.isRequired
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
    // init editor if changing page type or clicked on new query from non-newQuery view
    if (nextProps.pageType !== this.props.pageType || this.clickedNewQuery(nextProps)) {
      this.initSqlEditor(nextProps);
    }
  }

  clickedNewQuery = (nextProps) => {
    return this.locationIncludesNewQuery(nextProps.location) && !this.locationIncludesNewQuery(this.props.location);
  };

  locationIncludesNewQuery = (location) => location && location.pathname && location.pathname.includes('new_query');

  componentWillUnmount() {
    $(window).off('resize', this.resize);
    this.props.onUnmount();
  }

  initSqlEditor(props) {
    const { pageType, location } = props;

    switch (pageType) {
    case PageTypes.details:
      return;
    case PageTypes.wiki:
    case PageTypes.default:
    case PageTypes.graph: {
      const newSize = hashHeightTopSplitter[location.query.type] ||
        (this.locationIncludesNewQuery(location)) ?
        hashHeightTopSplitter.getNewQueryDefaultSqlHeight() :
        hashHeightTopSplitter.getDefaultSqlHeight();
      props.updateSqlPartSize(newSize);
      break;
    }
    default:
      throw new Error(`Not supported page type: '${pageType}'`);
    }
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
    const minHeightOverride = {
      minHeight: EXPLORE_PAGE_MIN_HEIGHT
    };

    // Note the DocumentTitle for this page lives in ExploreInfoHeader

    return (
      <div id='grid-page'
        ref='ExplorePageView'
        style={{...this.props.style, dragStyle, cursor, ...minHeightOverride}}>
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

const clearExploreEntities = () => clearEntities(['history', 'historyItem', 'dataset', 'fullDataset', 'datasetUI', 'tableData']);
export default connect(null, {
  onUnmount: clearExploreEntities
})(ExplorePageView);
