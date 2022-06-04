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
import {
  createNessieContext,
  NessieContext
} from '@app/pages/NessieHomePage/utils/context';
import { NessieRootState } from '@app/reducers/nessie/nessie';
import { fetchDefaultReference as fetchDefaultReferenceAction } from '@app/actions/nessie/nessie';
import { connect } from 'react-redux';
import { useEffect } from 'react';
import BranchPicker from '../BranchPicker/BranchPicker';

type ConnectedProps = {
  nessie: NessieRootState;
  fetchDefaultReference: any;
};

type SourceBranchPickerProps = {
  source: any;
  anchorEl?: any;
  position?: any;
  prefix?: string;
};

function SourceBranchPicker({
  source,
  nessie,
  fetchDefaultReference,
  anchorEl,
  position,
  prefix = '' //Prefix for redux state key
}: SourceBranchPickerProps & ConnectedProps) {
  const config = source.config;
  const endpoint = config.nessieEndpoint;
  const context = createNessieContext(
    { id: source.id, name: source.name, endpoint },
    nessie,
    prefix
  );

  const stateKey = `${prefix}${source.name}`;
  useEffect(() => {
    fetchDefaultReference(stateKey, context.api);
  }, [fetchDefaultReference, stateKey, context.api]);

  return (
    <div className='sources-branch-picker'>
      <NessieContext.Provider value={context}>
        <BranchPicker
          position={position}
          redirectOnChange={false}
          anchorEl={anchorEl}
        />
      </NessieContext.Provider>
    </div>
  );
}
const mapStateToProps = ({ nessie }: any) => ({ nessie });
const mapDispatchToProps = {
  fetchDefaultReference: fetchDefaultReferenceAction
};
export default connect(mapStateToProps, mapDispatchToProps)(SourceBranchPicker);
