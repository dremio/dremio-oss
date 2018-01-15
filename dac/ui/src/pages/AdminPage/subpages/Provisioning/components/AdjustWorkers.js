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
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import Button from 'components/Buttons/Button';
import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import { Popover, PopoverAnimationVertical } from 'material-ui/Popover';
import { triangleTop } from 'uiTheme/radium/overlay';
import AdjustWorkersForm  from './forms/AdjustWorkersForm';

export default class AdjustWorkers extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    readonly: PropTypes.bool
  }

  state = {
    isOpen: false
  };

  onClick = (e) => {
    this.setState((prevState) => ({ isOpen: !prevState.isOpen }));
    this.setState({
      anchorEl: this.state.anchorEl || e.currentTarget.parentElement
    });
    if (e) {
      e.stopPropagation();
    }
    return false;
  }

  render() {
    const { entity, readonly } = this.props;
    return (
      <div>
        <Button
          disable={readonly}
          style={{width: '100%'}}
          type={ButtonTypes.NEXT}
          text={la('Add / Remove Workers')}
          onClick={this.onClick}
        />
        <div>
          <Popover
            style={styles.popover}
            anchorEl={this.state.anchorEl}
            useLayerForClickAway={false}
            open={this.state.isOpen}
            anchorOrigin={{horizontal: 'left', vertical: 'bottom'}}
            targetOrigin={{horizontal: 'left', vertical: 'top'}}
            animation={PopoverAnimationVertical}>
            <div style={styles.triangle}/>
            <h4 style={styles.formTitle}>{la('Workers')}</h4>
            <AdjustWorkersForm
              onCancel={this.onClick}
              ref='dropDown'
              entity={entity}
              parent={this}/>
          </Popover>
        </div>
      </div>
    );
  }
}

const styles = {
  formTitle: {
    paddingTop: 10,
    paddingLeft: 10
  },
  popover: {
    overflow: 'visible',
    marginTop: 7,
    width: 280
  },
  triangle: {
    ...triangleTop,
    left: 6
  }
};
