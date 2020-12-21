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
import {PureComponent} from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import Art from '@app/components/Art';
import SettingsBtn from '@app/components/Buttons/SettingsBtn';
import EngineActionCellMixin from 'dyn-load/pages/AdminPage/subpages/Provisioning/components/EngineActionCellMixin';


export function StartStopButton(props) {
  const {engine, handleStartStop, style = {}, textStyle = {}} = props;
  const currentState = engine.get('currentState');
  const desiredState = engine.get('desiredState');
  const canStart =
    (currentState === 'STOPPED' && desiredState !== 'RUNNING') // not starting
    || currentState === 'FAILED'; // can't reliably check if already starting
  const canStop = currentState === 'RUNNING' && desiredState !== 'STOPPED';
  let startStop = '';
  let startStopIcon = '';
  if (canStart) {
    startStop = 'Start';
    startStopIcon = 'StartSquare.svg';
  } else if (canStop) {
    startStop = 'Stop';
    startStopIcon = 'StopSquare.svg';
  }
  if (!startStop) return null;

  const onStartStop = () => {
    handleStartStop(engine);
  };

  return (
    <button className='settings-button'
      onClick={onStartStop}
      data-qa='start-stop-btn' style={{...styles.settingsButton, ...style}}>
      <Art src={startStopIcon} alt={startStop} title style={styles.buttonWithTextIcon}/>
      <div style={{...styles.textByIcon, ...textStyle}}>{startStop}</div>
    </button>
  );
}

StartStopButton.propTypes = {
  engine: PropTypes.instanceOf(Immutable.Map),
  handleStartStop: PropTypes.func,
  style: PropTypes.object,
  textStyle: PropTypes.object
};

@EngineActionCellMixin
export class EngineActionCell extends PureComponent {
  static propTypes = {
    engine: PropTypes.instanceOf(Immutable.Map),
    editProvision: PropTypes.func,
    removeProvision: PropTypes.func,
    handleStartStop: PropTypes.func,
    handleAddRemove: PropTypes.func
  };


  render() {
    const { engine } = this.props;

    return <div className='actions-wrap' style={{display: 'flex'}}>
      {this.renderButton()}
      <SettingsBtn className='settings-button' style={styles.settingsButton}
        handleSettingsClose={() => {}}
        handleSettingsOpen={() => {}}
        dataQa={engine.get('tag')}
        menu={this.renderMenu()}
        hideArrowIcon
      >
        <Art src='Ellipsis.svg' alt={la('more...')} title style={styles.buttonIcon}/>
      </SettingsBtn>
    </div>;
  }
}

const styles = {
  settingsButton: {
    display: 'flex',
    padding: '0px 5px 0 3px',
    marginBottom: 1,
    border: 0,
    boxShadow: '0 1px 1px #c2c2c2',
    borderRadius: 2,
    background: '#eee',
    color: '#333',
    height: 23,
    fontSize: 11,
    alignItems: 'center'
  },
  buttonWithTextIcon: {
    height: 20,
    width: 20
  },
  buttonIcon: {
    height: 24,
    width: 24
  }
};
