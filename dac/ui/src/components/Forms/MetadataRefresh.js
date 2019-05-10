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
import PropTypes from 'prop-types';
import { label } from 'uiTheme/radium/forms';
import { formDefault } from 'uiTheme/radium/typography';
import { Select } from 'components/Fields';
import HoverHelp from 'components/HoverHelp';
import DurationField from 'components/Fields/DurationField';
import FieldWithError from 'components/Fields/FieldWithError';

// todo wrap in la
const DISCOVERY_TOOLTIP = 'Refresh interval for top-level source object names such as names of DBs and tables. '
  + 'This is a lightweight operation.';
const DETAILS_TOOLTIP =
`Metadata Dremio needs for query planning such as information on fields, types, shards, statistics and locality.

Fetch Modes:

Only Queried Datasets:
Dremio updates details for previously queried objects in a source. This mode increases query performance as less work needs to be done at query time for these datasets.

All Datasets (deprecated):
Dremio updates details for all datasets in a source. This mode increases query performance as less work needs to be done at query time.`;

const AUTHORIZATION_TOOLTIP =
  'When impersonation is enabled, maximum amount of time Dremio will cache authorization information.';
const DEFAULT_DURATION_ONE_HOUR = 3600000;
const DEFAULT_DURATION_THREE_HOUR = DEFAULT_DURATION_ONE_HOUR * 3;
const DEFAULT_DURATION_ONE_DAY = DEFAULT_DURATION_ONE_HOUR * 24;

const MIN_TIME = 60 * 1000; // when changed, must update validation error text

export default class MetadataRefresh extends Component {

  static propTypes = {
    fields: PropTypes.object,
    showDatasetDiscovery: PropTypes.bool,
    showAuthorization: PropTypes.bool
  };

  static defaultFormValues() {
    return {
      metadataPolicy: {
        namesRefreshMillis: DEFAULT_DURATION_ONE_HOUR,
        datasetDefinitionRefreshAfterMillis: DEFAULT_DURATION_ONE_HOUR,
        datasetDefinitionExpireAfterMillis: DEFAULT_DURATION_THREE_HOUR,
        authTTLMillis: DEFAULT_DURATION_ONE_DAY,
        updateMode: 'PREFETCH_QUERIED'
      }
    };
  }

  static getFields() {
    return [
      'metadataPolicy.namesRefreshMillis',
      'metadataPolicy.datasetDefinitionRefreshAfterMillis',
      'metadataPolicy.datasetDefinitionExpireAfterMillis',
      'metadataPolicy.authTTLMillis',
      'metadataPolicy.updateMode'
    ];
  }

  static validate(values) {
    const errors = {metadataPolicy: {}};

    if (values.metadataPolicy.namesRefreshMillis < MIN_TIME) {
      errors.metadataPolicy.namesRefreshMillis = la('Dataset discovery fetch must be at least 1 minute.');
    }

    if (values.metadataPolicy.datasetDefinitionRefreshAfterMillis < MIN_TIME) {
      errors.metadataPolicy.datasetDefinitionRefreshAfterMillis = la('Dataset details fetch must be at least 1 minute.');
    }

    if (values.metadataPolicy.datasetDefinitionExpireAfterMillis < MIN_TIME) {
      errors.metadataPolicy.datasetDefinitionExpireAfterMillis = la('Dataset details expiry must be at least 1 minute.');
    } else if (values.metadataPolicy.datasetDefinitionRefreshAfterMillis > values.metadataPolicy.datasetDefinitionExpireAfterMillis) {
      errors.metadataPolicy.datasetDefinitionExpireAfterMillis = la('Dataset details cannot be configured to expire faster than they fetch.');
    }

    if (values.metadataPolicy.authTTLMillis < MIN_TIME) {
      errors.metadataPolicy.authTTLMillis = la('Authorization expiry must be at least 1 minute.');
    }

    return errors;
  }

  static mapToFormFields(source) {
    const defaultValues = MetadataRefresh.defaultFormValues();
    const metadataPolicy = source && source.toJS().metadataPolicy || {};
    return {
      ...defaultValues.metadataPolicy,
      ...metadataPolicy
    };
  }

  refreshModeOptions = [
    { label: la('Only Queried Datasets'), option: 'PREFETCH_QUERIED' },
    { label: la('All Datasets (deprecated)'), option: 'PREFETCH' }
  ];

  render() {
    const {
      fields: { metadataPolicy },
      showDatasetDiscovery,
      showAuthorization
    } = this.props;

    return (
      <div className='metadata-refresh'>
        {showDatasetDiscovery && <div style={styles.subSection}>
          <span style={styles.label}>
            {la('Dataset Discovery')}
            <HoverHelp content={la(DISCOVERY_TOOLTIP)} />
          </span>
          <div style={styles.formSubRow}>
            <FieldWithError {...metadataPolicy.namesRefreshMillis} label={la('Fetch every')} labelStyle={styles.inputLabel} errorPlacement='right'>
              <DurationField
                {...metadataPolicy.namesRefreshMillis}
                min={MIN_TIME}
                style={styles.durationField}/>
            </FieldWithError>
          </div>
        </div>}
        <div style={styles.subSection}>
          <span style={styles.label}>
            {la('Dataset Details')}
            <HoverHelp content={la(DETAILS_TOOLTIP)} />
          </span>

          <div style={styles.formSubRow}>
            <FieldWithError {...metadataPolicy.updateMode} label={la('Fetch mode')} labelStyle={styles.inputLabel} errorPlacement='right'>
              <div style={{display: 'inline-block', verticalAlign: 'middle'}}>
                <Select
                  {...metadataPolicy.updateMode}
                  items={this.refreshModeOptions}
                  style={styles.inRowSelect}/>
              </div>
            </FieldWithError>
          </div>

          <div style={styles.formSubRow}>
            <FieldWithError {...metadataPolicy.datasetDefinitionRefreshAfterMillis} label={la('Fetch every')} labelStyle={styles.inputLabel} errorPlacement='right'>
              <DurationField
                {...metadataPolicy.datasetDefinitionRefreshAfterMillis}
                min={MIN_TIME}
                style={styles.durationField}/>
            </FieldWithError>
          </div>

          <div style={styles.formSubRow}>
            <FieldWithError {...metadataPolicy.datasetDefinitionExpireAfterMillis} label={la('Expire after')} labelStyle={styles.inputLabel} errorPlacement='right'>
              <DurationField
                {...metadataPolicy.datasetDefinitionExpireAfterMillis}
                min={MIN_TIME}
                style={styles.durationField}/>
            </FieldWithError>
          </div>
        </div>
        {showAuthorization && <div style={styles.subSection}>
          <span style={styles.label}>
            {la('Authorization')}
            <HoverHelp content={la(AUTHORIZATION_TOOLTIP)}/>
          </span>
          <div style={styles.formSubRow}>
            <FieldWithError {...metadataPolicy.authTTLMillis} label={la('Expire after')} labelStyle={styles.inputLabel} errorPlacement='right'>
              <DurationField
                {...metadataPolicy.authTTLMillis}
                min={MIN_TIME}
                style={styles.durationField}/>
            </FieldWithError>
          </div>
        </div>}
      </div>
    );
  }
}

const styles = {
  subSection: {
    marginBottom: 15
  },
  numberInput: {
    width: 42
  },
  inRowSelect: {
    width: 250,
    textAlign: 'left'
  },
  label: {
    ...label,
    display: 'inline-flex',
    alignItems: 'center',
    marginBottom: 4
  },
  inputLabel: {
    ...formDefault,
    marginLeft: 10,
    marginRight: 10,
    display: 'inline-flex',
    minWidth: 85
  },
  formSubRow: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 6
  },
  durationField: {
    width: 250
  }
};
