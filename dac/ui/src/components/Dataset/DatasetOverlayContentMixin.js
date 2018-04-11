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
import { Link } from 'react-router';
import FontIcon from 'components/Icon/FontIcon';
import { abilities } from 'utils/datasetUtils';
import { datasetTypeToEntityType } from 'constants/datasetTypes';

export default function(input) {
  Object.assign(input.prototype, { // eslint-disable-line no-restricted-properties
    renderPencil(summaryDataset) {
      const { canEdit } = abilities(
        summaryDataset,
        datasetTypeToEntityType[summaryDataset.get('datasetType')],
        summaryDataset.get('datasetType') === 'PHYSICAL_DATASET_HOME_FILE'
      );
      return canEdit
        ? <Link to={summaryDataset.getIn(['links', 'edit'])}><FontIcon type='Edit'/></Link>
        : null;
    }
  });
}
