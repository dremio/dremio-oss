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
import Radium from 'radium';
import PropTypes from 'prop-types';
import { section, label } from 'uiTheme/radium/forms';
import { formDefault } from 'uiTheme/radium/typography';
import HoverHelp from 'components/HoverHelp';
import DurationField from 'components/Fields/DurationField';
import FieldWithError from 'components/Fields/FieldWithError';
import config from 'utils/config';

const DURATION_ONE_HOUR = 3600000;
const MIN_DURATION = config.subhourAccelerationPoliciesEnabled ? 60 * 1000 : DURATION_ONE_HOUR; // when changed, must update validation error text

@Radium
export default class DataFreshnessSection extends Component {
  static propTypes = {
    fields: PropTypes.object,
    entityType: PropTypes.string
  }

  static defaultFormValueRefreshInterval() {
    return DURATION_ONE_HOUR;
  }

  static defaultFormValueGracePeriod() {
    return DURATION_ONE_HOUR * 3;
  }

  static getFields() {
    return ['accelerationRefreshPeriod', 'accelerationGracePeriod'];
  }

  static validate(values) {
    const errors = {};

    if (values.accelerationRefreshPeriod < MIN_DURATION) {
      if (config.subhourAccelerationPoliciesEnabled) {
        errors.accelerationRefreshPeriod = la('Reflection refresh must be at least 1 minute.');
      } else {
        errors.accelerationRefreshPeriod = la('Reflection refresh must be at least 1 hour.');
      }
    }

    if (values.accelerationGracePeriod < MIN_DURATION) {
      if (config.subhourAccelerationPoliciesEnabled) {
        errors.accelerationGracePeriod = la('Reflection expiry must be at least 1 minute.');
      } else {
        errors.accelerationGracePeriod = la('Reflection expiry must be at least 1 hour.');
      }
    } else if (values.accelerationRefreshPeriod > values.accelerationGracePeriod) {
      errors.accelerationGracePeriod = la('Reflections cannot be configured to expire faster than they refresh.');
    }

    return errors;
  }

  render() {
    const { fields: { accelerationRefreshPeriod, accelerationGracePeriod } } = this.props;
    const helpContent = la('How often reflections are refreshed and how long data can be served before expiration.');

    return (
      <div style={section}>
        <span style={styles.label}>
          {la('Refresh Policy')}
          <HoverHelp content={helpContent} tooltipInnerStyle={styles.hoverTip}/>
        </span>
        <table>
          <tbody>
            <tr>
              <td><div style={styles.inputLabel}>{la('Refresh every')}</div></td> {/* todo: ax: <label> */}
              <td>
                <FieldWithError errorPlacement='right' {...accelerationRefreshPeriod}>
                  <DurationField {...accelerationRefreshPeriod} min={MIN_DURATION} style={styles.durationField}/>
                </FieldWithError>
              </td>
            </tr>
            <tr>
              <td>
                <div style={styles.inputLabel}>{la('Expire after')}</div> {/* todo: ax: <label> */}
              </td>
              <td>
                <FieldWithError errorPlacement='right' {...accelerationGracePeriod}>
                  <DurationField {...accelerationGracePeriod} min={MIN_DURATION} style={styles.durationField}/>
                </FieldWithError>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    );
  }
}

const styles = {
  section: {
    display: 'flex',
    marginBottom: 6,
    alignItems: 'center'
  },
  select: {
    width: 164,
    marginTop: 3
  },
  label: {
    ...label,
    display: 'flex',
    alignItems: 'center'
  },
  inputLabel: {
    ...formDefault,
    marginLeft: 10,
    marginRight: 10
  },
  hoverTip: {
    textAlign: 'left',
    width: 300,
    whiteSpace: 'pre-line'
  },
  durationField: {
    width: 214
  }
};
