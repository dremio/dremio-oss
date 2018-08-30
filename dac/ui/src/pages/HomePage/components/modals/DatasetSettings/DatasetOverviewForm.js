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
// todo: rename this file

import { Component } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import pureRender from 'pure-render-decorator';

import { FormattedMessage } from 'react-intl';
import { FormBody, FormTitle } from 'components/Forms';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';

import { getIconDataTypeFromEntity } from 'utils/iconUtils';

import DatasetOverviewFormMixin
  from 'dyn-load/pages/HomePage/components/modals/DatasetSettings/DatasetOverviewFormMixin'; // eslint-disable-line max-len

@pureRender
@DatasetOverviewFormMixin
export default class DatasetOverviewForm extends Component {

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    location: PropTypes.object
  }

  render() {
    const { entity } = this.props;

    if (!entity) {
      return null;
    }

    const typeIcon = getIconDataTypeFromEntity(entity);

    // todo: if a real form likely want wrapped in ModalForm like siblings?
    return (
      <FormBody>
        <FormTitle>
          <FormattedMessage id = 'Common.Overview' />
        </FormTitle>
        <div>
          <DatasetItemLabel
            name={entity.get('name')}
            item={entity}
            fullPath={entity.get('fullPathList')}
            showFullPath
            shouldShowOverlay={false}
            typeIcon={typeIcon}/>
        </div>
      </FormBody>
    );
  }
}

