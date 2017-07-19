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
import { Tooltip, OverlayTrigger } from 'react-bootstrap';

import './AdvancedOptions.less';

export default class AdvancedOptions extends Component {

  constructor(props) {
    super(props);
    this.getAdvancedOptionsBlock = this.getAdvancedOptionsBlock.bind(this);

    this.state = {
      blocks: [
        {
          title: 'Cluster coordination',
          tooltipMsg: 'Cluster coordination',
          inputs:
          [
            {
              type: 'radio',
              text: 'Use embedded Zookeeper on this node',
              id: 'using-embedded',
              name: 'coordination',
              htmlFor: 'using-embedded'
            },
            {
              type: 'radio',
              text: 'Connect to External Zookeeper cluster',
              id: 'external-zookeeper',
              name: 'coordination',
              htmlFor: 'external-zookeeper'
            }
          ]
        },
        {
          title: 'Shared storage',
          tooltipMsg: 'Shared storage',
          inputs:
          [
            {
              type: 'radio',
              text: 'Use current node to hold shared state',
              id: 'using-current-node',
              name: 'shared-storage',
              htmlFor: 'using-current-node'
            },
            {
              type: 'radio',
              text: 'Network Attaced Storage (NAS)',
              id: 'network-attaced',
              name: 'shared-storage',
              htmlFor: 'network-attaced'
            }
          ]
        }
      ]
    };
  }

  getAdvancedOptionsBlock() {
    return this.state.blocks.map((block) => {
      const tooltip = (
        <Tooltip>{block.tooltipMsg}</Tooltip>
      );
      return (
        <div className='row'>
          <div className='row-title'>
            {block.title}
            <OverlayTrigger placement='top' overlay={tooltip}>
              <span className='notice'>?</span>
            </OverlayTrigger>
          </div>
          {this.getInputsForBlock(block)}
        </div>);
    });
  }

  getInputForBlock(input) {
    return (
      <div className='input-holder'>
        <input type={input.type}
          id={input.id}
          name={input.name}/>
        <label htmlFor={input.htmlFor}>{input.text}</label>
      </div>
    );
  }

  getInputsForBlock(block) {
    return block.inputs.map((input) => {
      return this.getInputForBlock(input);
    });
  }

  render() {
    return (
      <div className='advanced-options'>
        {this.getAdvancedOptionsBlock()}
      </div>
    );
  }
}
