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
import Immutable from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { injectIntl } from 'react-intl';
import FontIcon from 'components/Icon/FontIcon';
import SimpleButton from 'components/Buttons/SimpleButton';
import { Tooltip } from '@app/components/Tooltip';
import HoverTrigger from 'components/HoverTrigger';

@injectIntl
@Radium
export default class ExploreSettingsButton extends Component {
  static propTypes = {
    disabled: PropTypes.bool,
    dataset: PropTypes.instanceOf(Immutable.Map),
    side: PropTypes.oneOf(['left', 'right', 'bottom', 'top']),
    intl: PropTypes.object.isRequired
  };

  static defaultProps = {
    side: 'left',
    dataset: Immutable.Map()
  };

  static contextTypes = {
    router: PropTypes.object,
    location: PropTypes.object
  }

  state = {
    isOpenOverlay: false
  };

  handleMouseEnter = () => {
    if (this.props.disabled) {
      this.setState({ isOpenOverlay: true });
    }
  };

  handleMouseLeave = () => {
    this.setState({ isOpenOverlay: false });
  };

  handleOnClick = () => {
    const { disabled, dataset } = this.props;
    if (disabled) {
      return;
    }
    const { router, location } = this.context;
    router.push({
      ...location,
      state: {
        modal: 'DatasetSettingsModal',
        datasetUrl: dataset.getIn(['apiLinks', 'namespaceEntity']),
        datasetType: dataset.get('datasetType'),
        query: { then: 'query' }
      }
    });
  };

  render() {
    const { disabled, side, intl } = this.props;
    return (
      <HoverTrigger
        onEnter={this.handleMouseEnter}
        onLeave={this.handleMouseLeave}
        style={styles.base}>
        <SimpleButton
          ref='settingsButton'
          disabled={disabled}
          style={styles.button}
          onClick={this.handleOnClick}
          buttonStyle='secondary'>
          <FontIcon type='Settings' theme={styles.icon}/>
        </SimpleButton>
        <Tooltip
          container={document.body}
          target={() => this.state.isOpenOverlay ? this.refs.settingsButton : null}
          placement={side}
          tooltipInnerStyle={styles.overlay}
        >
          {intl.formatMessage({ id: 'Dataset.ChangeSettingsTooltip' })}
        </Tooltip>
      </HoverTrigger>
    );
  }
}

const styles = {
  base: {
    position: 'relative'
  },
  button: {
    minWidth: 40,
    lineHeight: 'inherit'
  },
  icon: {
    height: 28,
    'Icon': {
      verticalAlign: 'middle'
    }
  },
  overlay: {
    width: 300
  }
};
