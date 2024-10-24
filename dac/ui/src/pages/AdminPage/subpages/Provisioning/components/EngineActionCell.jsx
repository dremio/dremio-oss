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
import { PureComponent } from "react";
import PropTypes from "prop-types";
import Immutable from "immutable";
import SettingsBtn from "#oss/components/Buttons/SettingsBtn";
import EngineActionCellMixin from "dyn-load/pages/AdminPage/subpages/Provisioning/components/EngineActionCellMixin";
import { Button } from "dremio-ui-lib/components";
import { intl } from "#oss/utils/intl";

export function StartStopButton(props) {
  const {
    engine,
    handleStartStop,
    isHeaderButton,
    style = {},
    textStyle = {},
  } = props;

  const currentState = engine.get("currentState");
  const desiredState = engine.get("desiredState");
  const canStart =
    (currentState === "STOPPED" && desiredState !== "RUNNING") || // not starting
    currentState === "FAILED"; // can't reliably check if already starting
  const canStop = currentState === "RUNNING" && desiredState !== "STOPPED";
  let startStop = "";
  let startStopIcon = "";
  if (canStart) {
    startStop = "Common.Start";
    startStopIcon = "sql-editor/run";
  } else if (canStop) {
    startStop = "Common.Stop";
    startStopIcon = "sql-editor/stop";
  }
  if (!startStop) return null;

  const onStartStop = (e) => {
    e.stopPropagation();
    e.preventDefault();
    handleStartStop(engine);
  };

  return isHeaderButton ? (
    <Button
      variant="secondary"
      data-qa="start-stop-btn"
      className="mt-05"
      onClick={onStartStop}
    >
      <dremio-icon name={startStopIcon} alt="" />
      {intl.formatMessage({ id: startStop })}
    </Button>
  ) : (
    <button
      className="settings-button"
      onClick={onStartStop}
      data-qa="start-stop-btn"
      style={{ ...styles.settingsButton, ...style }}
    >
      <dremio-icon
        name={startStopIcon}
        style={{ ...styles.buttonIcon, marginRight: 2 }}
      />
      <div style={{ marginRight: 4, ...textStyle }}>
        {intl.formatMessage({ id: startStop })}
      </div>
    </button>
  );
}

StartStopButton.propTypes = {
  engine: PropTypes.instanceOf(Immutable.Map),
  handleStartStop: PropTypes.func,
  style: PropTypes.object,
  textStyle: PropTypes.object,
  isHeaderButton: PropTypes.bool,
};

@EngineActionCellMixin
export class EngineActionCell extends PureComponent {
  static propTypes = {
    engine: PropTypes.instanceOf(Immutable.Map),
    editProvision: PropTypes.func,
    removeProvision: PropTypes.func,
    handleStartStop: PropTypes.func,
    handleAddRemove: PropTypes.func,
    className: PropTypes.string,
  };

  render() {
    const { engine, className } = this.props;
    const currentState = engine.get("currentState");
    const isStartingOrStopping =
      currentState === "STARTING" || currentState === "STOPPING";
    return (
      <div
        onClick={(e) => {
          e.stopPropagation();
          e.preventDefault();
        }}
        className={`actions-wrap ${className}`}
        style={{ display: "flex" }}
      >
        {this.renderButton()}
        {!isStartingOrStopping && (
          <SettingsBtn
            // TODO: DX-92996: update accessibility when moved into leantable
            className="settings-button"
            style={styles.settingsButton}
            handleSettingsClose={() => {}}
            handleSettingsOpen={() => {}}
            dataQa={engine.get("tag")}
            menu={this.renderMenu()}
            hideArrowIcon
          >
            <dremio-icon
              name="interface/more"
              alt={intl.formatMessage({ id: "Common.More" })}
              styles={styles.buttonIcon}
            />
          </SettingsBtn>
        )}
      </div>
    );
  }
}

const styles = {
  startStopButton: {
    height: 32,
    width: 100,
    marginTop: 5,
    color: "var(--icon--primary)",
  },
  settingsButton: {
    display: "flex",
    padding: "0px 5px 0 3px",
    marginBottom: 1,
    border: 0,
    boxShadow: "var(--dremio--shadow--layer-1)",
    borderRadius: 2,
    background: "var(--fill--tertiary)",
    color: "var(--text--primary)",
    height: 23,
    fontSize: 11,
    alignItems: "center",
  },
  buttonIcon: {
    height: 20,
    width: 20,
    color: "var(--icon--primary)",
  },
};
