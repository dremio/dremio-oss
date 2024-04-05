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
import { Component, createRef } from "react";
import Immutable from "immutable";
import PropTypes from "prop-types";
import config from "dyn-load/utils/config";
import { formDefault } from "uiTheme/radium/typography";
import DurationField from "components/Fields/DurationField";
import FieldWithError from "components/Fields/FieldWithError";
import Checkbox from "components/Fields/Checkbox";
import { Button } from "dremio-ui-lib/components";
import ApiUtils from "utils/apiUtils/apiUtils";
import NotificationSystem from "react-notification-system";
import Message from "components/Message";
import { isCME, isNotSoftware } from "dyn-load/utils/versionUtils";
import { intl } from "@app/utils/intl";
import { getSupportFlag } from "@app/exports/endpoints/SupportFlags/getSupportFlag";
import {
  REFLECTION_SCHEDULER_POLICY,
  SUBHOUR_ACCELERATION_POLICY,
} from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { getMaxDistanceOfDays } from "@app/utils/scheduleRefreshUtils";
import ReflectionRefresh from "@app/components/Acceleration/ReflectionRefresh";
import { isCommunity } from "dyn-load/utils/versionUtils";
import { getSupportFlags } from "@app/selectors/supportFlags";
import { fetchSupportFlags } from "@app/actions/supportFlags";
import { connect } from "react-redux";

const DURATION_ONE_HOUR = 3600000;
window.subhourMinDuration = config.subhourAccelerationPoliciesEnabled
  ? 60 * 1000
  : DURATION_ONE_HOUR; // when changed, must update validation error text
export const SCHEDULE_POLICIES = {
  NEVER: "NEVER",
  PERIOD: "PERIOD",
  SCHEDULE: "SCHEDULE",
};
const defaultApplyToDays = 60 * 60 * 1000 * 24;

class DataFreshnessSection extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    fields: PropTypes.object,
    entityType: PropTypes.string,
    datasetId: PropTypes.string,
    elementConfig: PropTypes.object,
    editing: PropTypes.bool,
    isSchedulerEnabled: PropTypes.bool,
  };

  static defaultFormValueRefreshInterval() {
    return DURATION_ONE_HOUR;
  }

  static defaultFormValueGracePeriod() {
    return DURATION_ONE_HOUR * 3;
  }

  static defaultFormValuePolicyType() {
    return SCHEDULE_POLICIES.PERIOD;
  }

  static defaultFormValueRefreshSchedule() {
    return "0 0 8 * * *";
  }

  static getFields() {
    return [
      "accelerationRefreshPeriod",
      "accelerationGracePeriod",
      "accelerationNeverExpire",
      "accelerationNeverRefresh",
      "accelerationActivePolicyType",
      "accelerationRefreshSchedule",
    ];
  }

  static validate(values) {
    const errors = {};
    if (
      values.accelerationRefreshPeriod === 0 ||
      values.accelerationRefreshPeriod < window.subhourMinDuration
    ) {
      if (window.subhourMinDuration === DURATION_ONE_HOUR) {
        errors.accelerationRefreshPeriod = laDeprecated(
          "Reflection refresh must be at least 1 hour.",
        );
      } else {
        errors.accelerationRefreshPeriod = laDeprecated(
          "Reflection refresh must be at least 1 minute.",
        );
      }
    }

    if (
      values.accelerationGracePeriod &&
      values.accelerationGracePeriod < window.subhourMinDuration
    ) {
      if (window.subhourMinDuration === DURATION_ONE_HOUR) {
        errors.accelerationGracePeriod = laDeprecated(
          "Reflection expiry must be at least 1 hour.",
        );
      } else {
        errors.accelerationGracePeriod = laDeprecated(
          "Reflection expiry must be at least 1 minute.",
        );
      }
    } else if (
      !window.schedulerEnabled &&
      !values.accelerationNeverRefresh &&
      !values.accelerationNeverExpire &&
      values.accelerationRefreshPeriod > values.accelerationGracePeriod
    ) {
      errors.accelerationGracePeriod = laDeprecated(
        "Reflections cannot be configured to expire faster than they refresh.",
      );
    } else if (
      window.schedulerEnabled &&
      !values.accelerationNeverRefresh &&
      !values.accelerationNeverExpire &&
      ((values.accelerationActivePolicyType === "PERIOD" &&
        values.accelerationRefreshPeriod > values.accelerationGracePeriod) ||
        (values.accelerationActivePolicyType === "SCHEDULE" &&
          getMaxDistanceOfDays(values.accelerationRefreshSchedule) *
            defaultApplyToDays >
            values.accelerationGracePeriod))
    ) {
      errors.accelerationGracePeriod = laDeprecated(
        "Reflections cannot be configured to expire faster than they refresh.",
      );
    }

    return errors;
  }

  constructor(props) {
    super(props);
    this.notificationSystemRef = createRef();
    this.state = {
      refreshingReflections: false,
      minDuration: window.subhourMinDuration,
    };
  }

  async componentDidMount() {
    if (isNotSoftware?.()) {
      try {
        const subHourRes = await getSupportFlag(SUBHOUR_ACCELERATION_POLICY);
        this.setState({
          minDuration: subHourRes?.value ? 60 * 1000 : DURATION_ONE_HOUR,
        });
        window.subhourMinDuration = subHourRes?.value
          ? 60 * 1000
          : DURATION_ONE_HOUR;
      } catch (e) {
        //
      }
    }

    if (!isCommunity?.()) {
      this.props.fetchSupportFlags(REFLECTION_SCHEDULER_POLICY);
    }
  }

  componentDidUpdate() {
    window.schedulerEnabled = this.props.isSchedulerEnabled;
  }

  refreshAll = () => {
    ApiUtils.fetch(
      `catalog/${encodeURIComponent(this.props.datasetId)}/refresh`,
      { method: "POST" },
    );

    const message = laDeprecated(
      "All dependent reflections will be refreshed.",
    );
    const level = "success";

    const handleDismiss = () => {
      this.notificationSystemRef?.current?.removeNotification(notification);
      return false;
    };

    const notification = this.notificationSystemRef?.current?.addNotification({
      children: (
        <Message
          onDismiss={handleDismiss}
          messageType={level}
          message={message}
        />
      ),
      dismissible: false,
      level,
      position: "tc",
      autoDismiss: 0,
    });

    this.setState({ refreshingReflections: true });
  };

  isRefreshAllowed = () => {
    const { entity } = this.props;
    let result = true;
    if (isCME && !isCME()) {
      result = entity
        ? entity.get("permissions").get("canAlterReflections")
        : false;
    }
    return result;
  };

  render() {
    const {
      entityType,
      editing,
      fields: {
        accelerationRefreshPeriod,
        accelerationGracePeriod,
        accelerationNeverExpire,
        accelerationNeverRefresh,
      },
      fields,
    } = this.props;
    const helpContent = laDeprecated(
      "How often reflections are refreshed and how long data can be served before expiration.",
    );

    let message = null;
    if (
      editing &&
      accelerationGracePeriod.value !== accelerationGracePeriod.initialValue
    ) {
      message = laDeprecated(
        `Please note that reflections dependent on this ${
          entityType === "dataset" ? "dataset" : "source"
        } will not use the updated expiration configuration until their next scheduled refresh. Use "Refresh Dependent Reflections" on ${
          entityType === "dataset"
            ? "this dataset"
            : " any affected physical datasets"
        } for new configuration to take effect immediately.`,
      );
    }
    return (
      <div style={styles.container}>
        <NotificationSystem
          style={notificationStyles}
          ref={this.notificationSystemRef}
          autoDismiss={0}
          dismissible={false}
        />
        <span style={styles.label}>{laDeprecated("Refresh Policy")}</span>
        <div style={styles.info}>{helpContent}</div>
        {this.props.isSchedulerEnabled ? (
          <ReflectionRefresh
            entityType={entityType}
            refreshAll={this.refreshAll}
            isRefreshAllowed={this.isRefreshAllowed()}
            fields={fields}
            minDuration={this.state.minDuration}
            refreshingReflections={this.state.refreshingReflections}
          />
        ) : (
          <table>
            <tbody>
              <tr>
                <td colSpan={2}>
                  <div style={styles.inputLabel}>
                    <Checkbox
                      {...accelerationNeverRefresh}
                      label={laDeprecated("Never refresh")}
                    />
                  </div>
                </td>
              </tr>
              <tr>
                <td>
                  <div style={styles.inputLabel}>
                    {laDeprecated("Refresh every")}
                  </div>
                </td>
                {/* todo: ax: <label> */}
                <td>
                  <FieldWithError
                    errorPlacement="right"
                    {...accelerationRefreshPeriod}
                  >
                    <div style={{ display: "flex" }}>
                      <DurationField
                        {...accelerationRefreshPeriod}
                        min={this.state.minDuration}
                        style={styles.durationField}
                        disabled={!!accelerationNeverRefresh.value}
                      />
                      {this.isRefreshAllowed() && entityType === "dataset" && (
                        <Button
                          disabled={this.state.refreshingReflections}
                          onClick={this.refreshAll}
                          variant="secondary"
                          style={{
                            marginLeft: 10,
                          }}
                        >
                          {intl.formatMessage({
                            id: "Reflection.Refresh.Now",
                          })}
                        </Button>
                      )}
                    </div>
                  </FieldWithError>
                </td>
              </tr>
              <tr>
                <td colSpan={2}>
                  <div style={styles.inputLabel}>
                    <Checkbox
                      {...accelerationNeverExpire}
                      label={laDeprecated("Never expire")}
                    />
                  </div>
                </td>
              </tr>
              <tr>
                <td>
                  <div style={styles.inputLabel}>
                    {laDeprecated("Expire after")}
                  </div>{" "}
                  {/* todo: ax: <label> */}
                </td>
                <td>
                  <FieldWithError
                    errorPlacement="right"
                    {...accelerationGracePeriod}
                  >
                    <DurationField
                      {...accelerationGracePeriod}
                      min={this.state.minDuration}
                      style={styles.durationField}
                      disabled={!!accelerationNeverExpire.value}
                    />
                  </FieldWithError>
                </td>
              </tr>
            </tbody>
          </table>
        )}
        {message && (
          <Message
            style={{ marginTop: 5 }}
            messageType={"warning"}
            message={message}
          />
        )}
      </div>
    );
  }
}

const mapStateToProps = (state) => ({
  isSchedulerEnabled: getSupportFlags(state)[REFLECTION_SCHEDULER_POLICY],
});

const styles = {
  container: {
    marginTop: 6,
  },
  info: {
    maxWidth: 525,
    marginBottom: 26,
  },
  section: {
    display: "flex",
    marginBottom: 6,
    alignItems: "center",
  },
  select: {
    width: 164,
    marginTop: 3,
  },
  label: {
    fontSize: "18px",
    fontWeight: 600,
    margin: "0 0 8px 0px",
    display: "flex",
    alignItems: "center",
    color: "var(--color--neutral--900)",
  },
  inputLabel: {
    ...formDefault,
    marginRight: 10,
    marginBottom: 4,
  },
  durationField: {
    width: 250,
    marginBottom: 7,
  },
};

const notificationStyles = {
  Dismiss: {
    DefaultStyle: {
      width: 24,
      height: 24,
      color: "inherit",
      fontWeight: "inherit",
      backgroundColor: "none",
      top: 10,
      right: 5,
    },
  },
  NotificationItem: {
    DefaultStyle: {
      margin: 5,
      borderRadius: 1,
      border: "none",
      padding: 0,
      background: "none",
      zIndex: 45235,
    },
  },
};
export default connect(mapStateToProps, {
  fetchSupportFlags: fetchSupportFlags,
})(DataFreshnessSection);
