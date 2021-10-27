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
import { getCreatedSource, getViewState } from 'selectors/resources';
import DataFreshnessSection from 'components/Forms/DataFreshnessSection';
import MetadataRefresh from 'components/Forms/MetadataRefresh';
import { VIEW_ID } from 'pages/HomePage/components/modals/EditSourceView';

export const additionalMapDispatchToProps = {};

export const mapStateToProps = (state) => {
  const createdSource = getCreatedSource(state); // todo: why is this called "created" source here in "edit"?
  const messages = createdSource && createdSource.getIn(['state', 'messages']);

  return {
    messages,
    viewState: getViewState(state, VIEW_ID),
    source: createdSource,
    initialFormValues: {
      accelerationRefreshPeriod: createdSource && createdSource.has('accelerationRefreshPeriod') ? createdSource.get('accelerationRefreshPeriod') : DataFreshnessSection.defaultFormValueRefreshInterval(),
      accelerationGracePeriod: createdSource && createdSource.has('accelerationGracePeriod') ? createdSource.get('accelerationGracePeriod') : DataFreshnessSection.defaultFormValueGracePeriod(),
      metadataPolicy: MetadataRefresh.mapToFormFields(createdSource)
    }
  };
};

export const getFinalSubmit = (form, sourceType, props) => {
  return props.createSource(form, sourceType);
};

export default function(input) {
  Object.assign(input.prototype, { // eslint-disable-line no-restricted-properties
    mutateFormValues(values) {
      return values;
    },
    fetchData() {
    }
  });
}

export const ENABLE_USE_LEGACY_DIALECT_OPTION = true;
