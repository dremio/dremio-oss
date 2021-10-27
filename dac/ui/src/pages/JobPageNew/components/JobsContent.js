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
import { PureComponent } from 'react';
import $ from 'jquery';
import classNames from 'classnames';
import Immutable from 'immutable';
import PropTypes from 'prop-types';
import Radium from 'radium';
import { injectIntl } from 'react-intl';
import socket from '@inject/utils/socket';
import { flexColumnContainer } from '@app/uiTheme/less/layout.less';
import StatefulTableViewer from '@app/components/StatefulTableViewer';
import JobsContentMixin, {
  MIN_LEFT_PANEL_WIDTH,
  SEPARATOR_WIDTH
} from '@app/pages/JobPage/components/JobsContentMixin';
import { additionalColumnName } from '@inject/pages/JobPageNew/AdditionalJobPageColumns';
// import JobTable from '@app/pages/JobPage/components/JobsTable/JobTable';
import JobsFilters from '@app/pages/JobPage/components/JobsFilters/JobsFilters';
import JobStateIcon from '@app/pages/JobPage/components/JobStateIcon';
import timeUtils from 'utils/timeUtils';
import jobsUtils from 'utils/jobsUtils';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import { TableColumns } from '@app/constants/Constants';
import { getFormatMessageIdForQueryType } from '@app/pages/JobDetailsPageNew/Utils';
import DatasetCell from './DatasetCell';
import SQLCell from './SQLCell';
import DurationCell from './DurationCell';
import ColumnCell from './ColumnCell';
import ReflectionIcon, { getReflectionIcon } from './ReflectionIcon';
import 'react-virtualized/styles.css';

// export this for calculate min width of table tr in JobTable.js
export { SEPARATOR_WIDTH, MIN_LEFT_PANEL_WIDTH };

@injectIntl
@Radium
@JobsContentMixin
export default class JobsContent extends PureComponent {

  static propTypes = {
    jobId: PropTypes.string,
    jobs: PropTypes.instanceOf(Immutable.List).isRequired,
    queryState: PropTypes.instanceOf(Immutable.Map).isRequired,
    next: PropTypes.string,
    onUpdateQueryState: PropTypes.func.isRequired,
    viewState: PropTypes.instanceOf(Immutable.Map),
    dataWithItemsForFilters: PropTypes.object,
    isNextJobsInProgress: PropTypes.bool,
    location: PropTypes.object,
    intl: PropTypes.object.isRequired,
    className: PropTypes.string,
    loadItemsForFilter: PropTypes.func,
    loadNextJobs: PropTypes.func,
    changePages: PropTypes.func
  };

  static defaultProps = {
    jobs: Immutable.List()
  }

  static contextTypes = {
    router: PropTypes.object
  };

  constructor(props) {
    super(props);
    this.handleResizeJobs = this.handleResizeJobs.bind(this);
    this.getActiveJob = this.getActiveJob.bind(this);
    this.handleMouseReleaseOutOfBrowser = this.handleMouseReleaseOutOfBrowser.bind(this);

    this.handleStartResize = this.handleStartResize.bind(this);
    this.handleEndResize = this.handleEndResize.bind(this);
    this.setActiveJob = this.setActiveJob.bind(this);

    this.state = {
      isResizing: false,
      width: '100%',
      left: 'calc(50% - 22px)',
      curId: '',
      getColumns: [],
      getCheckedItems: Immutable.List(),
      previousJobId: ''
    };
  }

  getDefaultColumns = () => {
    const columnsObject = {};
    let selectedColumnsData = [];
    const localStorageColumns = localStorageUtils.getJobColumns() || [];
    localStorageColumns.forEach(item => columnsObject[item.key] = 1);
    TableColumns.forEach(item => columnsObject[item.key] = columnsObject[item.key] ? columnsObject[item.key] + 1 : 1);
    const existingColumns = Object.keys(columnsObject).filter((col) => columnsObject[col] > 1);
    if (existingColumns.length === TableColumns.length) {
      //for 18.1.0 release only need to be changed in the next update Ticket Number DX-37189
      const reflectionIndex = localStorageColumns.findIndex(item => item.key === 'reflection');
      if (reflectionIndex >= 0) {
        localStorageColumns[reflectionIndex].isSelected = true;
        localStorageUtils.setJobColumns(localStorageColumns);
      }
      selectedColumnsData = localStorageUtils.getJobColumns()
        ?
        localStorageUtils.getJobColumns().filter(item => columnsObject[item.key] > 1 && item.isSelected)
        :
        TableColumns.filter(item => item.isSelected);
      const initialColumns = localStorageUtils.getJobColumns()
        ?
        localStorageUtils.getJobColumns().filter(item => columnsObject[item.key] > 1)
        :
        TableColumns;
      localStorageUtils.setJobColumns(initialColumns);
    } else {
      selectedColumnsData = TableColumns.filter(item => item.isSelected);
      localStorageUtils.setJobColumns(TableColumns);
    }
    this.setState({
      getColumns: selectedColumnsData
    });
  }

  updateColumnsState = (updatedColumns) => {
    this.setState({ getColumns: updatedColumns });
  };

  sortJobsByColumn = (name) => {
    const { location } = this.props;
    const currentSortDirection = location.query.order;
    const isCurrentColumn = name === location.query.sort;
    const direction = (currentSortDirection === 'ASCENDING' || !isCurrentColumn) ? 'DESCENDING' : 'ASCENDING';
    this.context.router.push({
      ...location, query: { ...location.query, sort: name, order: direction }
    });
    return direction === 'ASCENDING' ? 'DESC' : 'ASC';
  }

  componentDidMount() {
    $(window).on('mouseup', this.handleMouseReleaseOutOfBrowser);
    this.getDefaultColumns();
  }


  componentWillReceiveProps(nextProps) {
    if (nextProps.jobs !== this.props.jobs) {
      this.runActionForJobs(nextProps.jobs, false, (jobId) => socket.startListenToQVJobProgress(jobId));

      // if we don't have an active job id highlight the first job
      if (!nextProps.jobId) {
        this.setActiveJob(nextProps.jobs.get(0), true);
      }
    }
    if (nextProps.jobId !== this.props.jobId) {
      this.setState({
        previousJobId: this.props.jobId
      });
    }
  }

  componentWillUnmount() {
    $(window).off('mouseup', this.handleMouseReleaseOutOfBrowser);
    this.runActionForJobs(this.props.jobs, true, (jobId) => socket.stoptListenToQVJobProgress(jobId));
  }

  getCurrentJobIndex() {
    const { jobId, jobs } = this.props;
    if (this.state.previousJobId === jobId) {
      return -1;
    }
    return jobs && jobs.findIndex(item => item.get('id') === jobId);
  }

  render() {
    const {
      jobs, queryState, onUpdateQueryState,
      viewState, className, loadNextJobs, intl
    } = this.props;
    const { getColumns } = this.state;
    const columnCheckedItems = localStorageUtils.getJobColumns()
      ?
      localStorageUtils.getJobColumns()
        .filter(item => item.isSelected)
        .map(label => label.key)
      :
      TableColumns
        .filter(item => item.isSelected)
        .map(label => label.key);
    const getCheckedItems = Immutable.List(columnCheckedItems);
    const styles = this.styles;
    const resizeStyle = this.state.isResizing ? styles.noSelection : {};
    let tableWidth = 0;
    getColumns.forEach(column => {
      tableWidth = tableWidth + column.width;
    });
    tableWidth = tableWidth + 160;
    const renderColumn = (data, isNumeric) => <ColumnCell data={data} isNumeric={isNumeric} />;
    const renderJobStatus = (jobState) => <JobStateIcon state={jobState} />;
    const renderSQL = (sql) => <SQLCell sql={sql} />;
    const renderDataset = (job) => <DatasetCell job={job} />;
    const renderIcon = (isAcceleration) => <ReflectionIcon isAcceleration={isAcceleration} />;
    const renderDuration = (
      duration,
      durationDetails,
      isAcceleration,
      isSpilled
    ) => <DurationCell
      durationDetails={durationDetails}
      isAcceleration={isAcceleration}
      duration={duration}
      isSpilled={isSpilled}
    />;
    const getTableData = () => {

      return jobs.map((job, index) => {
        const durationDetails = job.get('durationDetails');
        const jobDuration = jobsUtils.formatJobDuration(job.get('duration'), true);
        const planningTimeObject = durationDetails.find(duration => duration.get('phaseName') === 'PLANNING');
        const planningTime = planningTimeObject && Number(planningTimeObject.get('phaseDuration'));
        const formattedPlanningTime = planningTime && (planningTime < 1000 ? '<1s' : timeUtils.formatTimeDiff(planningTime, 'HH:mm:ss'));
        const formattedCost = jobsUtils.getFormattedNumber(job.get('plannerEstimatedCost'));
        const formattedRowsScanned = jobsUtils.getFormattedNumber(job.get('rowsScanned'));
        const formattedRowsReturned = jobsUtils.getFormattedNumber(job.get('outputRecords'));
        const getColumnName = additionalColumnName(job);

        return {
          data: {
            jobStatus: { node: () => renderJobStatus(job.get('state')), value: job.get('state') },
            job: { node: () => renderColumn(job.get('id')), value: job.get('id') },
            usr: { node: () => renderColumn(job.get('user')), value: job.get('queryUser') },
            acceleration: { node: () => renderIcon(job.get('accelerated')), value: renderIcon(job.get('accelerated')) },
            reflection: { node: () => renderIcon(job.get('accelerated')), value: renderIcon(job.get('accelerated')) },
            ds: { node: () => renderDataset(job, index), value: job },
            qt: { node: () => renderColumn(intl.formatMessage({ id: getFormatMessageIdForQueryType(job) })), value: job.get('queryType') },
            ...(getColumnName[0]),
            st: { node: () => renderColumn(timeUtils.formatTime(job.get('startTime')), false), value: timeUtils.formatTime(job.get('startTime')) },
            dur: {
              node: () => renderDuration(jobDuration, durationDetails, job.get('accelerated'), job.get('spilled')),
              value: {
                jobDuration,
                durationDetails,
                isAcceleration: job.get('accelerated'),
                isSpilled: job.get('spilled')
              }
            },
            sql: { node: () => renderSQL(job.get('queryText')), value: job.get('queryText') },
            cost: { node: () => renderColumn(formattedCost.toString(), true), value: formattedCost.toString() },
            planningTime: { node: () => renderColumn(formattedPlanningTime, true), value: formattedPlanningTime },
            rowsScanned: { node: () => renderColumn(formattedRowsScanned.toString(), true), value: formattedRowsScanned.toString() },
            rowsReturned: { node: () => renderColumn(formattedRowsReturned.toString(), true), value: formattedRowsReturned.toString() }
          }
        };
      });
    };
    return (
      <div className={classNames('jobs-content', flexColumnContainer, className)} style={[styles.base, resizeStyle]} ref='content'>
        <JobsFilters
          queryState={queryState}
          onUpdateQueryState={onUpdateQueryState}
          style={styles.filters}
          loadItemsForFilter={this.props.loadItemsForFilter}
          dataWithItemsForFilters={this.props.dataWithItemsForFilters}
          checkedItems={getCheckedItems}
          columnFilterSelect={this.filterColumnSelect}
          columnFilterUnSelect={this.filterColumnUnSelect}
          updateColumnsState={this.updateColumnsState}
          isQVJobs
        />
        <StatefulTableViewer
          virtualized
          rowHeight={40}
          tableWidth={tableWidth}
          columns={getColumns}
          tableData={getTableData()}
          scrollToIndex={this.getCurrentJobIndex()}
          viewState={viewState}
          enableHorizontalScroll
          onClick={this.props.changePages}
          resizableColumn
          loadNextRecords={loadNextJobs}
          sortRecords={this.sortJobsByColumn}
          noDataText={intl.formatMessage({ id: 'Job.NoJobs' })}
          showIconHeaders={{ acceleration: { node: getReflectionIcon } }}
          disableZebraStripes
        />
      </div>
    );
  }
}
