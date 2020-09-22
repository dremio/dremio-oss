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
import { Component, CSSProperties } from 'react';
import Immutable from 'immutable';
import classNames from 'classnames';
import pureRender from 'pure-render-decorator';
import { connect } from 'react-redux';
import { hashHeightTopSplitter } from '@app/constants/explorePage/heightTopSplitter.js';
import { PageTypes } from '@app/pages/ExplorePage/pageTypes';
import { clearEntities } from '@app/actions/resources/entities';
import { flexElementAuto } from '@app/uiTheme/less/layout.less';

import './ExplorePage.less';
import HistoryLineController from './components/Timeline/HistoryLineController';
import ExplorePageContentWrapper from './subpages/ExplorePageContentWrapper';

const EXPLORE_PAGE_MIN_HEIGHT = 700;

type ExplorePageViewProps = {
  pageType?: PageTypes,
  dataset?: any,
  location: any,
  rightTreeVisible: boolean,
  sqlState?: boolean,
  sqlSize?: number,
  updateSqlPartSize: (newSize: number) => void,
  toggleRightTree: () => void,
  isResizeInProgress?: boolean,
  onUnmount: () => void
};
type ExplorePageViewState = {
  isError: boolean,
  errorData: Immutable.Map<any, any>
};
@pureRender
export class ExplorePageView extends Component<ExplorePageViewProps, ExplorePageViewState> {
  state = {
    isError: false,
    errorData: Immutable.Map<any, any>()
  };
  componentWillMount() {
    this.initSqlEditor(this.props);
  }
  componentWillReceiveProps(nextProps: ExplorePageViewProps) {
    // init editor if changing page type or clicked on new query from non-newQuery view
    if (nextProps.pageType !== this.props.pageType || this.clickedNewQuery(nextProps)) {
      this.initSqlEditor(nextProps);
    }
  }
  clickedNewQuery = (nextProps: ExplorePageViewProps) => {
    return this.locationIncludesNewQuery(nextProps.location) && !this.locationIncludesNewQuery(this.props.location);
  };
  locationIncludesNewQuery = (location: any) => location && location.pathname && location.pathname.includes('new_query');
  componentWillUnmount() {
    this.props.onUnmount();
  }
  initSqlEditor(props: ExplorePageViewProps) {
    const { pageType, location } = props;
    switch (pageType) {
      case PageTypes.details:
        return;
      case PageTypes.wiki:
      case PageTypes.reflections:
      case PageTypes.default:
      case PageTypes.graph: {
        const newSize =
          (hashHeightTopSplitter as any)[location.query.type] || this.locationIncludesNewQuery(location)
            ? hashHeightTopSplitter.getNewQueryDefaultSqlHeight()
            : hashHeightTopSplitter.getDefaultSqlHeight();
        props.updateSqlPartSize(newSize);
        break;
      }
      default:
        throw new Error(`Not supported page type: '${pageType}'`);
    }
  }
  startDrag() {}
  render() {
    const { dataset, isResizeInProgress } = this.props;
    const selectState = isResizeInProgress ? 'text' : 'none';
    const cursor = isResizeInProgress ? 'row-resize' : 'initial';
    const dragStyle: CSSProperties = {
      MozUserSelect: selectState,
      WebkitUserSelect: selectState,
      msUserSelect: selectState
    };
    const minHeightOverride = {
      minHeight: EXPLORE_PAGE_MIN_HEIGHT
    };
    // Note the DocumentTitle for this page lives in ExploreInfoHeader
    return (
      <div id='grid-page'
        className={classNames('grid-wrap', flexElementAuto)}
        style={{ ...dragStyle, cursor, ...minHeightOverride }}
      >
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
        />
        <HistoryLineController
          dataset={dataset}
          location={this.props.location}
          ref='historyLine'
        />
      </div>
    );
  }
}
const clearExploreEntities = () => clearEntities(['history', 'historyItem', 'dataset', 'fullDataset', 'datasetUI', 'tableData']);
export default connect(null, {
  onUnmount: clearExploreEntities
})(ExplorePageView);
