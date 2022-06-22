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
  NessieContext,
} from "@app/pages/NessieHomePage/utils/context";
import { NessieRootState } from "@app/reducers/nessie/nessie";
import { fetchDefaultReference as fetchDefaultReferenceAction } from "@app/actions/nessie/nessie";
import { connect } from "react-redux";
import { useEffect } from "react";
import { isDefaultReferenceLoading } from "@app/selectors/nessie/nessie";
import FontIcon from "@app/components/Icon/FontIcon";
import { getRootEntityLinkUrl } from "@app/selectors/home";
import BranchPicker from "../BranchPicker/BranchPicker";

type ConnectedProps = {
  nessie: NessieRootState;
  fetchDefaultReference: any;
  redirectUrl: string;
};

type SourceBranchPickerProps = {
  source: any;
  anchorEl?: any;
  position?: any;
  prefix?: string;
  redirect?: boolean;
  onClose?: () => void;
};

function SourceBranchPicker({
  source,
  nessie,
  fetchDefaultReference,
  anchorEl,
  position,
  prefix = "", //Prefix for redux state key
  redirectUrl,
  onClose,
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

  const isLoading = isDefaultReferenceLoading(nessie[stateKey]);
  if (isLoading) {
    return (
      <FontIcon
        type="Loader spinner"
        theme={{ Container: { marginTop: 2 }, Icon: { width: 15, height: 15 } }}
      />
    );
  }

  return (
    <div className="sources-branch-picker">
      <NessieContext.Provider value={context}>
        <BranchPicker
          position={position}
          redirectUrl={redirectUrl}
          anchorEl={anchorEl}
          onClose={onClose}
        />
      </NessieContext.Provider>
    </div>
  );
}
const mapStateToProps = (
  state: any,
  { source, redirect = true }: SourceBranchPickerProps
) => {
  return {
    nessie: state.nessie,
    redirectUrl:
      redirect && source ? getRootEntityLinkUrl(state, source.id) : "",
  };
};
const mapDispatchToProps = {
  fetchDefaultReference: fetchDefaultReferenceAction,
};
export default connect(mapStateToProps, mapDispatchToProps)(SourceBranchPicker);
