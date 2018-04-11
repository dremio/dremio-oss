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
import moment from 'moment';
import Modal from 'components/Modals/Modal';
import InfoWarningMessage from 'components/InfoWarningMessage';

const styles = {
  modal: {
    background: 'transparent',
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center'
  }
};

const SECONDS_IN_MINUTE = 60;
const DELAY = 1000;

export default class TransformWarningModal extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    onConfirm: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired
  };

  constructor(props) {
    super(props);
    this.startTimer = this.startTimer.bind(this);
    this.state = {
      elapsedTime: Date.now()
    };
    if (props.isOpen) {
      this.startTimer();
    }
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.isOpen && !this.timer) {
      this.setState({
        elapsedTime: Date.now()
      });
      this.startTimer();
    } else {
      this.stopTimer();
    }
  }

  startTimer() {
    this.timer = setTimeout(() => {
      this.setState({
        time: this.formatTime()
      });

      this.startTimer();
    }, DELAY);
  }

  stopTimer() {
    clearTimeout(this.timer);
    this.timer = undefined;
    this.setState({
      time: 0
    });
  }

  formatTime() {
    const seconds = moment().diff(this.state.elapsedTime, 'seconds');
    const isWeHaveMinutes = seconds > SECONDS_IN_MINUTE;
    const minutes = isWeHaveMinutes ? String(Math.floor(seconds / SECONDS_IN_MINUTE)) : '0';
    const formatedMinutes = minutes.length > 1 ? minutes : `0${minutes}`;
    const formatedSeconds = String(seconds % 60).length > 1
      ? seconds % SECONDS_IN_MINUTE
      : `0${seconds % SECONDS_IN_MINUTE}`;
    return seconds < SECONDS_IN_MINUTE ? `00:${formatedSeconds}` : `${formatedMinutes}:${formatedSeconds}`;
  }

  render() {
    const { isOpen, onCancel } = this.props;

    return (
      <Modal
        isOpen={isOpen}
        hide={onCancel}
        size='small'
        style={styles.modal}
      >
        <InfoWarningMessage
          text={la('This transformation is taking longer than expected.')}
          info={`Elapsed time: ${this.state.time || this.formatTime()}`}
          buttonLabel={la('Cancel')}
          onClick={this.props.onConfirm}
        />
      </Modal>
    );
  }
}
