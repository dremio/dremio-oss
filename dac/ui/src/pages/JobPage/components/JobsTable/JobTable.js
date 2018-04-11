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
import Immutable, { Map }  from 'immutable';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import FontIcon from 'components/Icon/FontIcon';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import { AutoSizer, List } from 'react-virtualized';
import { getIconByEntityType } from 'utils/iconUtils';
import jobsUtils from 'utils/jobsUtils';
import 'react-virtualized/styles.css';

import { formLabel, h5 } from 'uiTheme/radium/typography';

import JobTr from './JobTr';
import { SEPARATOR_WIDTH, MIN_LEFT_PANEL_WIDTH } from './../JobsContent';

const MIN_JOBS_LIST_HEIGHT = 500;
// calculate min width for tr, сonsidering width of separator
const MIN_JOB_TR_WIDTH = MIN_LEFT_PANEL_WIDTH - SEPARATOR_WIDTH;
const JOBS_OFFSET = 30;
const OFFSETHEIGHT_HEIGHT_DIFF = 25;

@Radium
@PureRender
export default class JobTable extends Component {

  static propTypes = {
    jobs: PropTypes.instanceOf(Immutable.List).isRequired,
    next: PropTypes.string,
    setActiveJob: PropTypes.func,
    loadNextJobs: PropTypes.func,
    jobId: PropTypes.string,
    containsTextValue: PropTypes.string,
    // if we have not jobId, we use 48% for width. Other cases, it's number
    width: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    viewState: PropTypes.instanceOf(Immutable.Map),
    isResizing: PropTypes.bool,
    isNextJobsInProgress: PropTypes.bool
  };

  static contextTypes = {
    router: PropTypes.object,
    location: PropTypes.object
  }

  constructor(props) {
    super(props);
    this.renderJobTr = this.renderJobTr.bind(this);
    this.handleResize = this.handleResize.bind(this);

    this.state = {
      activeColumn: Map({
        direction: '',
        name: ''
      }),
      columnsHeader: Map({ // todo: loc
        ds: Map({
          label: 'Dataset'
        }),
        usr: Map({
          label: 'User'
        }),
        st: Map({
          label: 'Start Time'
        }),
        dur: Map({
          label: 'Duration'
        }),
        et: Map({
          label: 'End Time'
        })
      })
    };
  }

  state = {
    previousJobId: null
  };

  componentDidMount() {
    $(window).on('resize', this.handleResize);
    this.handleResize();
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.jobs !== this.props.jobs ||
        nextProps.isResizing !== this.props.isResizing ||
        nextProps.jobId !== this.props.jobId) {
      setTimeout(this.handleResize); // we require this to prevent wrong size when we get all data before data for selected job
    }

    if (nextProps.jobId !== this.props.jobId) {
      this.setState({
        previousJobId: this.props.jobId
      });
    }
    if (this.virtualList
      && (nextProps.jobId !== this.props.jobId || nextProps.jobs !== this.props.jobs)) {
      this.virtualList.forceUpdateGrid();
    }
  }

  componentWillUnmount() {
    $(window).off('resize', this.handleResize);
  }

  getActiveJobIndex() {
    const { jobId, jobs } = this.props;
    if (this.state.previousJobId === jobId) {
      return -1;
    }
    return jobs.findIndex(item => item.get('id') === jobId);
  }

  sortJobsByColumn(name) {
    const currentSortDirection = this.context.location.query.order;
    const isCurrentColumn = name === this.context.location.query.sort;
    const direction = (currentSortDirection === 'ASCENDING' || !isCurrentColumn) ? 'DESCENDING' : 'ASCENDING';
    const { location } = this.context;
    this.context.router.push({
      ...location, query: {...location.query, sort: name, order: direction}
    });
  }

  handleJobClick = (item) => this.props.setActiveJob(item);

  handleResize() {
    const width  = this.refs.table && this.refs.table.offsetWidth || 0;
    this.setState({
      width,
      height: this.refs.table && this.refs.table.offsetHeight - OFFSETHEIGHT_HEIGHT_DIFF
    });
  }

  renderHeaderBlock() {
    return (
      <div className='job-table-header' style={[style.jobTableHeader, formLabel]}>
        {
          this.state.columnsHeader.keySeq().map(key => (
            <div
              key={key}
              style={[style.th, { marginLeft: key === 'usr' ? 50 : 0 }]}
              onClick={this.sortJobsByColumn.bind(this, key)}>
              <span style={h5}>{this.state.columnsHeader.getIn([key, 'label'])}</span>
              {this.renderIconForColumnHeader(key)}
            </div>
          ))
        }
      </div>
    );
  }

  renderIconForColumnHeader(colName) {
    const direction = this.context.location.query.order;
    if (direction && colName === this.context.location.query.sort) {
      const type = direction === 'DESCENDING'
        ? 'fa-caret-down'
        : 'fa-caret-up';
      return <FontIcon type={type} theme={style.caretTheme}/>;
    }
  }

  renderJobTr({ index, key, style }) {
    if (index + JOBS_OFFSET > this.props.jobs.size &&
        this.props.next && !this.props.isNextJobsInProgress &&
        this.lastLoaded !== this.props.next) {
      this.props.loadNextJobs(this.props.next);
      this.lastLoaded = this.props.next;
    }

    const item = this.props.jobs.get(index);
    const { jobId, containsTextValue } = this.props;
    const active = item.get('id') === jobId;
    const even = !(index % 2);
    const dataset = item.getIn(['datasetPathList', -1]); // use dataset name on job info
    const datasetType = item.get('datasetType');
    const fullPath = item.get('datasetPathList');

    // only show the overlay if we have a dataset type and the job is not a metadata job
    const showOverlay = (!jobsUtils.isMetadataJob(item.get('requestType')) && !!datasetType);
    const datasetLabel = (
      <DatasetItemLabel
        name={dataset}
        style={{alignItems: 'flex-start', minHeight: 30}}
        inputValue={containsTextValue}
        fullPath={fullPath}
        showFullPath
        placement='right'
        shouldShowOverlay={showOverlay}
        typeIcon={getIconByEntityType(datasetType)}/>
      );

    return (
      <div key={key} style={style}>
        <JobTr
          active={active}
          even={even}
          onClick={this.handleJobClick.bind(this, item)}
          key={item.get('id')}
          job={item}
          jobDataset={datasetLabel}
          containsTextValue={containsTextValue}
        />
      </div>
    );
  }

  renderJobs() {
    return (
      <AutoSizer disableHeight>
        {({ width }) => (
          <List
            ref={(ref) => this.virtualList = ref}
            rowRenderer={this.renderJobTr}
            height={this.state.height || MIN_JOBS_LIST_HEIGHT}
            rowCount={this.props.jobs.size}
            rowHeight={75}
            width={width}
            scrollToIndex={this.getActiveJobIndex()}
          />
        )}
      </AutoSizer>
    );
  }

  render() {
    const { width } = this.props;

    return (
      <div className='jobs-table' key='jobsTable' style={{...style.base, width}} ref='table'>
        {this.renderHeaderBlock()}
        <div style={style.jobsWrap}>
          {this.renderJobs()}
        </div>
      </div>);
  }
}

const style = {
  base: {
    width: '50%',
    backgroundColor: '#fff',
    overflowY: 'hidden'
  },
  spinner: {
    width: '100%',
    height: '100%',
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center'
  },
  jobsWrap: {
    minWidth: MIN_JOB_TR_WIDTH
  },
  th: {
    cursor: 'pointer'
  },
  caretTheme: {
    Container: {
      margin: '0 0 0 5px'
    }
  },
  jobTableHeader: {
    height: 26,
    minWidth: MIN_JOB_TR_WIDTH,
    borderBottom: '1px solid #ddd',
    display: 'flex',
    padding: '0 50px 0 35px',
    justifyContent: 'space-between',
    alignItems: 'center'
  },
  icon: {
    fontSize: 35,
    color: 'gray'
  },
  leftFloat: {
    float: 'left'
  }
};
