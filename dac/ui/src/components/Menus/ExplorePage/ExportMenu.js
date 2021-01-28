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
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { injectIntl } from 'react-intl';

import { LIST, MAP, MIXED } from '@app/constants/DataTypes';
import localStorageUtils from '@inject/utils/storageUtils/localStorageUtils';
import { getExploreJobId, getExploreState } from '@app/selectors/explore';
import { isSqlChanged } from '@app/sagas/utils';
import { addNotification } from 'actions/notification';
import { MSG_CLEAR_DELAY_SEC } from '@app/constants/Constants';
import { APIV2Call } from '@app/core/APICall';
import config from 'dyn-load/utils/config';
import MenuItem from './MenuItem';
import Menu from './Menu';

const UNSUPPORTED_TYPE_COLUMNS = {
  'CSV': new Set([MAP, LIST, MIXED])
};

const TYPES = [
  { label: 'JSON', name: 'JSON' },
  { label: 'CSV', name: 'CSV' },
  { label: 'Parquet', name: 'PARQUET' }
];

@injectIntl
export class ExportMenu extends PureComponent {
  static propTypes = {
    action: PropTypes.func,
    datasetColumns: PropTypes.array,
    datasetSql: PropTypes.string,
    intl: PropTypes.object.isRequired,
    //connected
    jobId: PropTypes.string,
    currentSql: PropTypes.string,
    addNotification: PropTypes.func
  };

  handleDatasetDownload = (type) => {
    const token = localStorageUtils.getAuthToken();
    const tempApiCall = new APIV2Call()
      .path('temp-token')
      .params({
        'durationSeconds': 30,
        'request': '/apiv2/job/' + this.props.jobId + '/download/?downloadFormat=' + type.name
      });

    this.showNotification(type.label);
    fetch(tempApiCall.toString(), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': token
      }
    })
      .then(res => res.json())
      .then(data => {
        const apiCall = new APIV2Call()
          .path('job')
          .path(this.props.jobId)
          .path('download')
          .params({
            'downloadFormat': type.name,
            '.token': data.token ? data.token : ''
          });
        window.location.assign(apiCall.toString());
      });
  };

  renderMenuItems() {
    const { jobId, datasetSql, currentSql, intl } = this.props;
    const datasetColumns = new Set(this.props.datasetColumns);
    const isSqlDirty = isSqlChanged(datasetSql, currentSql);
    const isTypesIntersected = (types) => !![...types].filter(type => datasetColumns.has(type)).length;

    return TYPES.map(type => {
      const types = UNSUPPORTED_TYPE_COLUMNS[type.name];
      const disabled = (config.downloadRecordsLimit === 0) || isSqlDirty || !jobId || (types && isTypesIntersected(types));
      const itemTitle = intl.formatMessage({ id: 'Download.DownloadLimitValue' });

      return <MenuItem key={type.name} disabled={disabled} title={itemTitle} onClick={this.handleDatasetDownload.bind(this, type)}>{type.label}</MenuItem>;
    });
  }

  showNotification(type) {
    const { intl } = this.props;
    const notificationMessage = intl.formatMessage({ id: 'Download.Notification' }, {type});
    const message = <span>{notificationMessage}</span>;
    this.props.addNotification(message, 'success', MSG_CLEAR_DELAY_SEC);
  }

  render() {
    return <Menu>{this.renderMenuItems()}</Menu>;
  }
}

function mapStateToProps(state) {
  const explorePageState = getExploreState(state);
  const currentSql = explorePageState.view.currentSql;

  const jobId = getExploreJobId(state);
  return {
    jobId,
    currentSql
  };
}

export default connect(mapStateToProps, { addNotification })(ExportMenu);
