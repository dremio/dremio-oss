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
import { Link } from 'react-router';
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { injectIntl } from 'react-intl';

import config from 'utils/config';
import Art from 'components/Art';
import { headerRightPadding } from '@app/uiTheme/radium/allSpacesAndAllSources';
import { ENTITY_TYPES } from 'constants/Constants';
import { WikiButton } from '@app/pages/HomePage/components/WikiButton';

import HeaderButtonsMixin from 'dyn-load/pages/HomePage/components/HeaderButtonsMixin';

@injectIntl
@Radium
@HeaderButtonsMixin
export class HeaderButtons extends Component {
  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    toggleVisibility: PropTypes.func.isRequired,
    rootEntityType: PropTypes.oneOf(Object.values(ENTITY_TYPES)),
    user: PropTypes.string,
    rightTreeVisible: PropTypes.bool,
    intl: PropTypes.object.isRequired,
    onWiki: PropTypes.func,
    isWikiShown: PropTypes.bool
  };

  static defaultProps = {
    entity: Immutable.Map()
  };

  getButtonsForEntityType(entityType) {
    switch (entityType) {
    case ENTITY_TYPES.space:
      return this.getSpaceSettingsButtons();
    case ENTITY_TYPES.source:
      return this.getSourceSettingsButtons().concat(this.getSourceButtons());
    case ENTITY_TYPES.home:
      return this.getHomeButtons();
    default:
      return [];
    }
  }

  getSourceButtons() {
    const { entity } = this.props;
    const buttons = [];

    if (entity.get('isPhysicalDataset')) {
      buttons.push({
        qa: 'query-folder',
        iconType: 'Query',
        to: entity.getIn(['links', 'query']),
        isAdd: false
      });
    } else if (entity.get('fileSystemFolder')) {
      buttons.push({
        qa: 'convert-folder',
        iconType: 'FolderConvert',
        style: styles.largeButton,
        to: {...this.context.location, state: {
          modal: 'DatasetSettingsModal',
          tab: 'format',
          entityType: entity.get('entityType'),
          entityId: entity.get('id'),
          query: {then: 'query'}
        }},
        isAdd: false
      });
    }
    return buttons;
  }

  getHomeButtons() {
    const { location } = this.context;
    const buttons = [];
    buttons.push(
      {
        qa: 'add-folder',
        iconType: 'Folder',
        to: {...location, state: {modal: 'AddFolderModal'}},
        isAdd: true
      }
    );
    if (config.allowFileUploads) {
      buttons.push(
        {
          qa: 'add-file',
          iconType: 'File',
          to: {...location, state: {modal: 'AddFileModal'}},
          isAdd: true
        }
      );
    }
    return buttons;
  }

  getIconAltText(iconType) {
    const messages = {
      File: 'File.File',
      VirtualDataset: 'Dataset.VirtualDataset',
      Folder: 'Folder.Folder',
      FolderConvert: 'Folder.FolderConvert',
      Query: 'Job.Query',
      Settings: 'Common.Settings'
    };
    const iconMessageId = messages[iconType];
    return (iconMessageId) ? this.props.intl.formatMessage({id: iconMessageId}) : '';
  }

  renderButton = (item, index) => {
    const iconAlt = this.getIconAltText(item.iconType);

    return <Link
      className='button-white'
      data-qa={`${item.qa}-button`}
      to={item.to ? item.to : '.'}
      key={`${item.iconType}-${index}`}
      style={{...styles.button, ...item.style}}>
      {item.isAdd && <Art
        src='SimpleAdd.svg'
        alt={this.props.intl.formatMessage({ id: 'Common.Add' })}
        style={styles.addIcon}/>}
      <Art src={`${item.iconType}.svg`} alt={iconAlt} style={styles.typeIcon} />
    </Link>;
  };

  render() {
    const {
      rootEntityType,
      isWikiShown,
      onWiki
    } = this.props;
    const buttonsForCurrentPage = this.getButtonsForEntityType(rootEntityType);


    return (
      <span className='main-settings-holder' style={styles.mainSettingsHolder}>
        {buttonsForCurrentPage.map(this.renderButton)}
        <WikiButton key='wikiButton'
          isSelected={isWikiShown}
          onClick={onWiki}
        />
      </span>
    );
  }
}

//TODO: refactor styles

const styles = {
  addIcon: {
    width: 8,
    marginLeft: 3
  },
  typeIcon: {
    height: 24
  },
  mainSettingsHolder: {
    display: 'flex',
    /* Currently we have following structure
      // header has 5px padding on the right. See headerRightPadding in allSpacesAndAllSources.js
      <header> // row layout
        ...
        <WikiButton /> // has 10px right padding, which is consistent with WikiContent (see below)
      </header>
      <content>
        <LeftColumn />
        // does not have right paddings
        <RightColumn>
          <WikiContent />
        </RightColumn>
      </content>
      So visually we have
      [WikiButton]|10px|5px
      [WikiContent]|10px|0
      which looks weird in terms of alignment
      So I have to apply -5px margin here for alignment purposes
      NOTE: this approach will work only if HeaderButtons are only used in BrowseTable.
      At moment when I wrote this code, that was the case.
    */
    marginRight: -headerRightPadding
  },
  button: {
    background: '#dbe8ed',
    borderRadius: '2px',
    marginRight: '6px',
    height: 25,
    width: 40,
    boxShadow: '0 1px 1px #b2bec7',
    cursor: 'pointer',
    display: 'flex',
    justifyContent: 'center'
  },
  largeButton: {
    width: 54,
    paddingTop: 1
  },
  innerTextStyle: {
    top: '-7px',
    textAlign: 'left'
  },
  iconBox: {
    width: 24,
    height: 24
  },
  iconContainer: {
    marginRight: 1,
    lineHeight: '24px',
    width: 24,
    position: 'relative'
  }
};

