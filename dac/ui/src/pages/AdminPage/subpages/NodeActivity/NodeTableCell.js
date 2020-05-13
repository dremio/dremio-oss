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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';

import EllipsedText from '@app/components/EllipsedText';
import { circle, container, nameContainer, nodeName, typeContainer } from './NodeTableCell.less';

export const NodeTableCellColors = {
  GREEN: 'green',
  GREY: 'grey'
};

export default class NodeTableCell extends PureComponent {

  static propTypes = {
    name: PropTypes.string,
    status: PropTypes.string,
    tooltip: PropTypes.string,
    isMaster: PropTypes.bool,
    isCoordinator: PropTypes.bool,
    isExecutor: PropTypes.bool
  };
  // these colors are unique for this component (moved from old NodeActivityView)
  static colors = {
    [NodeTableCellColors.GREEN]: '#84D754',
    [NodeTableCellColors.GREY]: '#777777'
  };

  render() {
    const { name, status = NodeTableCellColors.GREEN, tooltip = la('Active'), isExecutor, isMaster, isCoordinator }
      = this.props;

    const color = NodeTableCell.colors[status];

    let type = '';
    if (isCoordinator) {
      type = isMaster ? 'master coordinator' : 'coordinator';
    }

    if (isExecutor) {
      if (isCoordinator) {
        type += ', ';
      }
      type += 'executor';
    }

    return (
      <div className={container}>
        <div style={{width: 12}}>
          <div className={circle} style={{background: color}} title={tooltip}></div>
        </div>
        <div className={nameContainer}>
          <EllipsedText text={name} className={nodeName} />
          <div className={typeContainer}>{type}</div>
        </div>
      </div>
    );
  }
}
