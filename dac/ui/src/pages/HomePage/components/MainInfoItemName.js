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
import CopyButton from 'components/Buttons/CopyButton';
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import FontIcon from 'components/Icon/FontIcon';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import EllipsedText from 'components/EllipsedText';
import { injectIntl } from 'react-intl';
import { constructFullPath, splitFullPath, getFullPathListFromEntity } from 'utils/pathUtils';
import { getIconDataTypeFromEntity } from 'utils/iconUtils';

@injectIntl
@Radium
export default class MainInfoItemName extends Component {

  static propTypes = {
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    intl: PropTypes.object.isRequired,
    onMount: PropTypes.func // takes width parameter
  };

  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
    this.wrap = null;
  }

  componentDidMount() {
    const {onMount} = this.props;
    if (onMount) {
      onMount(this.getComponentWidth());
    }
  }

  setWrapRef = (element) => {
    this.wrap = element;
  };

  getComponentWidth() {
    return this.wrap.clientWidth;
  }

  getHref(entity) {
    const fileType = entity.get('fileType');
    if (entity.get('fileType') === 'file') {
      if (entity.get('queryable')) {
        return entity.getIn(['links', 'query']);
      }
      return {
        ...this.context.location, state: {
          modal: 'DatasetSettingsModal',
          tab: 'format',
          entityType: entity.get('entityType'),
          entityId: entity.get('id'),
          fullPath: entity.get('filePath'),
          query: {then: 'query'}
        }
      };
    }
    if (fileType === 'folder') {
      if (entity.get('queryable')) {
        return entity.getIn(['links', 'query']);
      }
      return entity.getIn(['links', 'self']);
    }
    return {
      ...this.context.location,
      state: {
        ...this.context.location.state,
        originalDatasetVersion: entity.get('datasetConfig') && entity.getIn(['datasetConfig', 'version'])
      },
      pathname: entity.getIn(['links', 'query'])
    };
  }


  renderDatasetItemLabel() {
    const { item } = this.props;
    const type = item.get('entityType');
    const typeIcon = getIconDataTypeFromEntity(item);
    if (type === 'dataset' || type === 'physicalDataset' || type === 'file' && item.get('queryable')
        || type === 'folder' && item.get('queryable')) {
      return (
        <DatasetItemLabel
          name={item.get('name')}
          item={item}
          fullPath={item.get('fullPathList') || item.getIn(['fileFormat', 'fullPath'])
                    || splitFullPath(item.get('filePath'))}
          typeIcon={typeIcon}/>
      );
    }
    return (
      <div style={styles.flexAlign}>
        <FontIcon type={typeIcon} />
        <EllipsedText className='last-File' style={styles.fullPath} text={item.get('name')} />
      </div>
    );
  }

  render() {
    const { item, intl } = this.props;
    const fileType = item.get('fileType');
    const fullPath = constructFullPath(getFullPathListFromEntity(item));
    const href = this.getHref(item);
    const linkStyle = (fileType === 'folder' && !item.get('queryable'))
        ? styles.flexAlign
        : {...styles.flexAlign, ...styles.leafLink};
    const holderClass = fileType + '-path';

    return (
      <div style={[styles.flexAlign, styles.base]} className={holderClass} ref={this.setWrapRef}>
        <Link style={linkStyle} to={href}>
          {this.renderDatasetItemLabel()}
        </Link>
        { fullPath && <CopyButton
          text={fullPath}
          title={intl.formatMessage({ id: 'Path.Copy' })}
          style={{transform: 'translateY(1px)'}}
        /> }
      </div>
    );
  }
}

const styles = {
  base: {
    maxWidth: 'calc(100% - 100px)' // reserve 100px for tags [IE 11]
  },
  fullPath: {
    marginLeft: 5
  },
  flexAlign: {
    display: 'flex',
    //flex: '0 1', // should get rid ofthis for [IE 11]
    alignItems: 'center',
    maxWidth: '100%'
  },
  leafLink: {
    textDecoration: 'none',
    color: '#333'
  }
};
