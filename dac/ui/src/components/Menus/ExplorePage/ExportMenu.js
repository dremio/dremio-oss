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

import { MAP, LIST, MIXED } from '@app/constants/DataTypes';
import { API_URL_V2 } from '@app/constants/Api';
import localStorageUtils from '@app/utils/storageUtils/localStorageUtils';
import { addParameterToUrl } from '@app/utils/urlUtils';
import { getExploreJobId, getExploreState } from '@app/selectors/explore';
import { isSqlChanged } from '@app/sagas/utils';
import { addNotification } from 'actions/notification';

const UNSUPPORTED_TYPE_COLUMNS = {
  'CSV': new Set([MAP, LIST, MIXED])
};

import MenuItem from './MenuItem';
import Menu from './Menu';

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

  makeUrl = (type) => {
    const {jobId} = this.props;
    const downloadUrl = `/job/${encodeURIComponent(jobId)}/download?downloadFormat=${type.name}`;
    const token = localStorageUtils.getAuthToken();
    return `${API_URL_V2}${addParameterToUrl(downloadUrl, 'Authorization', token)}`;
  };

  renderMenuItems() {
    const { jobId, datasetSql, currentSql, intl } = this.props;
    const datasetColumns = new Set(this.props.datasetColumns);
    const isSqlDirty = isSqlChanged(datasetSql, currentSql);
    const isTypesIntersected = (types) => !![...types].filter(type => datasetColumns.has(type)).length;

    return TYPES.map(type => {
      const types = UNSUPPORTED_TYPE_COLUMNS[type.name];
      const disabled = isSqlDirty || !jobId || (types && isTypesIntersected(types));
      const href = this.makeUrl(type);
      const itemTitle = intl.formatMessage({ id: 'Download.DownloadLimitValue' });

      return <MenuItem key={type.name} href={href} disabled={disabled} title={itemTitle} onClick={this.showNotification.bind(this, type.label)}>{type.label}</MenuItem>;
    });
  }

  showNotification(type) {
    const { intl } = this.props;
    const notificationMessage = intl.formatMessage({ id: 'Download.Notification' }, {type});
    const message = <span>{notificationMessage}</span>;
    this.props.addNotification(message, 'success', 10);
  }

  render() {
    return <Menu>{this.renderMenuItems()}</Menu>;
  }
}

function mapStateToProps(state, props) {
  const explorePageState = getExploreState(state);
  const currentSql = explorePageState.view.currentSql;

  const jobId = getExploreJobId(state);
  return {
    jobId,
    currentSql
  };
}

export default connect(mapStateToProps, { addNotification })(ExportMenu);
