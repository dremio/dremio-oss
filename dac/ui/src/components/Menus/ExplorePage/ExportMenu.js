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
import { getExploreJobId, getExploreState } from '@app/selectors/explore';
import { isSqlChanged } from '@app/sagas/utils';
import { addNotification } from 'actions/notification';
import { MSG_CLEAR_DELAY_SEC } from '@app/constants/Constants';
import { APIV2Call } from '@app/core/APICall';
import tokenUtils from '@inject/utils/tokenUtils';
import config from 'dyn-load/utils/config';
import { Cancel } from '@app/utils/CancelablePromise';
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
    updateDownloading: PropTypes.func,
    intl: PropTypes.object.isRequired,
    //connected
    jobId: PropTypes.string,
    currentSql: PropTypes.string,
    addNotification: PropTypes.func
  };

  handleDatasetDownload = (type) => {
    this.props.updateDownloading();

    tokenUtils.getTempToken({
      params: {
        'durationSeconds': 30,
        'request': 'job/' + this.props.jobId + '/download/?downloadFormat=' + type.name
      },
      requestApiVersion: 2
    })
      .then(data => {
        const apiCall = new APIV2Call()
          .path('job')
          .path(this.props.jobId)
          .path('download')
          .params({
            'downloadFormat': type.name,
            '.token': data.token ? data.token : ''
          });
        // window.location.assign(apiCall.toString());
        fetch(apiCall.toString())
          .then(response => {
            response.blob()
              .then(resObj => {

                // MS Edge and IE don't allow using a blob object directly as link href, instead it is necessary to use msSaveOrOpenBlob
                if (window.navigator && window.navigator.msSaveOrOpenBlob) {
                  window.navigator.msSaveOrOpenBlob(resObj);
                } else {
                // For other browsers: create a link pointing to the ObjectURL containing the blob.
                  const objUrl = window.URL.createObjectURL(resObj);

                  const link = document.createElement('a');
                  link.href = objUrl;
                  link.download = `${this.props.jobId}.${type.name}`.toLocaleLowerCase();
                  link.click();

                  // For Firefox it is necessary to delay revoking the ObjectURL.
                  setTimeout(() => {
                    window.URL.revokeObjectURL(objUrl);
                  }, 250);
                }
                this.props.updateDownloading();
                this.showNotification();
              })
              .catch((error) => {
                this.props.updateDownloading();
                if (error instanceof Cancel) return;
                throw error;
              });
          });
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
    const notificationMessage = intl.formatMessage({ id: 'Download.Completed' }, {type});
    const message = `${this.props.jobId} ${notificationMessage}`;
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
