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
import { Component, createRef } from 'react';
import { Popover, MouseEvents } from '@app/components/Popover';

import PropTypes from 'prop-types';

import './HeaderProjectsList.less';

const sampleData = [
  'Propoise',
  'Dolphine',
  'Octopus',
  'Walrus',
  'Two-Headed Narwhal',
  'Elephant Seal',
  'Humpback Whale',
  'Gray Whale',
  'Blue Whale',
  'Killer Whale',
  'Great While Shark',
  'Gray Whale & Humpback Whale & Blue Whale & Killer Whale',
  'Blue Shark',
  'Bull Shark',
  'Tiger Shark',
  'Nurse Shark',
  'Harbor Seal'
];

export default class HeaderProjectList extends Component {
  static propTypes = {
    dataQa: PropTypes.string,
    className: PropTypes.string
  };

  static defaultProps = {
  };

  constructor(props) {
    super(props);

    this.state = {
      anchorEl: null,
      activeProject: ''
    };
  }

  isOpen = () => Boolean(this.state.anchorEl);

  contentRef = createRef();

  onClickListContainer = () => {
    const {anchorEl} = this.state;

    if (anchorEl !== null) {
      return;
    }

    this.setState({anchorEl: this.contentRef.current});
  }

  closePopover = () => {
    this.setState({anchorEl: null});
  }

  selectProject = (newSelectedValue) => {
    this.setState({
      activeProject: newSelectedValue,
      anchorEl: null
    });
  }

  buildProjectList = () => {
    const {activeProject} = this.state;

    let selectedProject = activeProject;
    const projectList = sampleData.map((val, index) => {
      if (activeProject === '' && index === 0) {
        selectedProject = val;
      }

      let entryClassName = 'listEntry';
      if (val === selectedProject) {
        entryClassName += ' selected';
      }

      return (
        <div className={entryClassName} onClick={() => this.selectProject(val)}>
          <div className='value'>{val}</div>
        </div>
      );
    });

    return {active: selectedProject, list: projectList};
  }

  render() {
    const {className, dataQa} = this.props;
    const {anchorEl} = this.state;
    const open = this.isOpen();
    const projectData = this.buildProjectList();

    return (
      <div className={className} data-qa={dataQa}>
        <div className='headerProjectsList'>
          <div
            className='listContainer'
            onClick={this.onClickListContainer}
            ref={this.contentRef}
          >
            <div className='containerContent'>
              <div className='label'>{projectData.active}</div>
              <div className='icon fa fa-angle-down'></div>
            </div>
            <Popover
              anchorEl={open ? anchorEl : null}
              onClose={this.closePopover}
              dataQa='headerProjectsListPopover' // todo change that
              listStyle={styles.popover}
              clickAwayMouseEvent={MouseEvents.onClick}
              listWidthSameAsAnchorE
            >
              <div className='dropdownList'>
                {projectData.list}
              </div>
            </Popover>

          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  popover: {
    overflow: 'visible'
  }
};
