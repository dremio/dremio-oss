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
import Radium from 'radium';
import PropTypes from 'prop-types';
import { formDefault } from 'uiTheme/radium/typography';
import HoverHelp from 'components/HoverHelp';
import DurationField from 'components/Fields/DurationField';
import FieldWithError from 'components/Fields/FieldWithError';
import Checkbox from 'components/Fields/Checkbox';
import Button from 'components/Buttons/Button';
import config from 'utils/config';
import ApiUtils from 'utils/apiUtils/apiUtils';
import NotificationSystem from 'react-notification-system';
import Message from 'components/Message';

const DURATION_ONE_HOUR = 3600000;
const MIN_DURATION = config.subhourAccelerationPoliciesEnabled ? 60 * 1000 : DURATION_ONE_HOUR; // when changed, must update validation error text

@Radium
export default class DataFreshnessSection extends Component {
  static propTypes = {
    fields: PropTypes.object,
    entityType: PropTypes.string,
    datasetId: PropTypes.string,
    elementConfig: PropTypes.object,
    editing: PropTypes.bool
  };

  static defaultFormValueRefreshInterval() {
    return DURATION_ONE_HOUR;
  }

  static defaultFormValueGracePeriod() {
    return DURATION_ONE_HOUR * 3;
  }

  static getFields() {
    return ['accelerationRefreshPeriod', 'accelerationGracePeriod', 'accelerationNeverExpire', 'accelerationNeverRefresh'];
  }

  static validate(values, props) {
    const errors = {};

    if (values.accelerationRefreshPeriod  && values.accelerationRefreshPeriod < MIN_DURATION) {
      if (config.subhourAccelerationPoliciesEnabled) {
        errors.accelerationRefreshPeriod = la('Reflection refresh must be at least 1 minute.');
      } else {
        errors.accelerationRefreshPeriod = la('Reflection refresh must be at least 1 hour.');
      }
    }

    if (values.accelerationGracePeriod && values.accelerationGracePeriod < MIN_DURATION) {
      if (config.subhourAccelerationPoliciesEnabled) {
        errors.accelerationGracePeriod = la('Reflection expiry must be at least 1 minute.');
      } else {
        errors.accelerationGracePeriod = la('Reflection expiry must be at least 1 hour.');
      }
    } else if (!values.accelerationNeverRefresh && !values.accelerationNeverExpire
      && values.accelerationRefreshPeriod > values.accelerationGracePeriod) {
      errors.accelerationGracePeriod = la('Reflections cannot be configured to expire faster than they refresh.');
    }

    return errors;
  }

  state = {
    refreshingReflections: false
  };

  refreshAll = () => {
    ApiUtils.fetch(`catalog/${encodeURIComponent(this.props.datasetId)}/refresh`, {method: 'POST'}).then().catch();

    const message = la('All dependent reflections will be refreshed.');
    const level = 'success';

    const handleDismiss = () => {
      this.refs.notificationSystem.removeNotification(notification);
      return false;
    };

    const notification = this.refs.notificationSystem.addNotification({
      children: <Message onDismiss={handleDismiss} messageType={level} message={message} />,
      dismissible: false,
      level,
      position: 'tc',
      autoDismiss: 0
    });

    this.setState({refreshingReflections: true});
  };

  render() {
    const { entityType, elementConfig, editing, fields: { accelerationRefreshPeriod, accelerationGracePeriod, accelerationNeverRefresh, accelerationNeverExpire } } = this.props;
    const helpContent = la('How often reflections are refreshed and how long data can be served before expiration.');

    let message = null;
    if (editing && accelerationGracePeriod.value !== accelerationGracePeriod.initialValue) {
      message = la(`Please note that reflections dependent on this ${entityType === 'dataset' ? 'dataset' : 'source'} will not use the updated expiration configuration until their next scheduled refresh. Use "Refresh Dependent Reflections" on ${entityType === 'dataset' ? 'this dataset' : ' any affected physical datasets'} for new configuration to take effect immediately.`);
    }

    // do not show Refresh Policy header and tooltip in configurable forms with elementConfig
    return (
      <div>
        <NotificationSystem style={notificationStyles} ref='notificationSystem' autoDismiss={0} dismissible={false} />
        {!elementConfig &&
        <span style={styles.label}>
          {la('Refresh Policy')}
          <HoverHelp content={helpContent}/>
        </span>
        }
        <table>
          <tbody>
            <tr>
              <td colSpan={2}>
                <div style={styles.inputLabel}>
                  <Checkbox
                    {...accelerationNeverRefresh}
                    label={la('Never refresh')}/>
                </div>
              </td>
            </tr>
            <tr>
              <td><div style={styles.inputLabel}>{la('Refresh every')}</div></td>{/* todo: ax: <label> */}
              <td>
                <FieldWithError errorPlacement='right' {...accelerationRefreshPeriod}>
                  <div style={{display: 'flex'}}>
                    <DurationField {...accelerationRefreshPeriod} min={MIN_DURATION} style={styles.durationField} disabled={!!accelerationNeverRefresh.value}/>
                    {entityType === 'dataset' && <Button
                      disable={this.state.refreshingReflections}
                      disableSubmit
                      onClick={this.refreshAll}
                      type='SECONDARY'
                      style={{marginBottom: 0, marginLeft: 10, marginTop: 2}}
                      text={la('Refresh Now')}
                    />}
                  </div>
                </FieldWithError>
              </td>
            </tr>
            <tr>
              <td colSpan={2}>
                <div style={styles.inputLabel}>
                  <Checkbox
                    {...accelerationNeverExpire}
                    label={la('Never expire')}/>
                </div>
              </td>
            </tr>
            <tr>
              <td>
                <div style={styles.inputLabel}>{la('Expire after')}</div> {/* todo: ax: <label> */}
              </td>
              <td>
                <FieldWithError errorPlacement='right' {...accelerationGracePeriod}>
                  <DurationField {...accelerationGracePeriod} min={MIN_DURATION} style={styles.durationField} disabled={!!accelerationNeverExpire.value}/>
                </FieldWithError>
              </td>
            </tr>
          </tbody>
        </table>
        { message && <Message style={{marginTop: 5}} messageType={'warning'} message={message}/> }
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
    fontSize: '18px',
    fontWeight: 300,
    margin: '0 0 8px 0px',
    color: '#555555',
    display: 'flex',
    alignItems: 'center'
  },
  inputLabel: {
    ...formDefault,
    marginRight: 10,
    fontWeight: 500,
    marginBottom: 4
  },
  durationField: {
    width: 250,
    marginBottom: 7
  }
};

const notificationStyles = {
  Dismiss: {
    DefaultStyle: {
      width: 24,
      height: 24,
      color: 'inherit',
      fontWeight: 'inherit',
      backgroundColor: 'none',
      top: 10,
      right: 5
    }
  },
  NotificationItem: {
    DefaultStyle: {
      margin: 5,
      borderRadius: 1,
      border: 'none',
      padding: 0,
      background: 'none',
      zIndex: 45235
    }
  }
};
