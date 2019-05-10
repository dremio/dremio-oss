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
import { Component, createRef } from 'react';
import PropTypes from 'prop-types';
import { Tooltip } from 'components/Tooltip';

export default class ChartTooltip extends Component {
  static propTypes = {
    position: PropTypes.object,
    /** If anchorEl is not provided, then position property is used to display a tooltip */
    anchorEl: PropTypes.object,
    content: PropTypes.node
  }

  ref = createRef();

  render() {
    const { content, position, anchorEl } = this.props;
    return (
      <div style={{ position: 'absolute', ...position }} ref={this.ref}>
        <Tooltip
          target={() => anchorEl || this.ref.current}
          id='tooltip'
          type='status'
          placement='top'
          tooltipInnerStyle={styles.tooltipInner}
        >
          {content}
        </Tooltip>
      </div>
    );
  }
}

const styles = {
  tooltipInner: {
    textAlign: 'center'
  }
};
