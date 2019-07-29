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

export const NodeTableCellColors = {
  GREEN: 'green',
  GREY: 'grey'
};

export default class NodeTableCell extends PureComponent {

  static propTypes = {
    name: PropTypes.string,
    status: PropTypes.string,
    tooltip: PropTypes.string
  };
  // these colors are unique for this component (moved from old NodeActivityView)
  static colors = {
    [NodeTableCellColors.GREEN]: '#84D754',
    [NodeTableCellColors.GREY]: '#777777'
  };

  render() {
    const { name, status = NodeTableCellColors.GREEN, tooltip = la('Active') } = this.props;

    const color = NodeTableCell.colors[status];
    const style = {...styles.circle, background: color};

    return (
      <span style={styles.node}>
        <span style={style} title={tooltip}>{status}</span><span>{name}</span>
      </span>
    );
  }

}

const styles = {
  node: {
    display: 'flex',
    alignItems: 'center'
  },
  circle: {
    width: 12,
    height: 12,
    margin: '0 10px',
    borderRadius: '50%',
    textIndent: '-9999px'
  }
};
