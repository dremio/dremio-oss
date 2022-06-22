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
import FontIcon from "@app/components/Icon/FontIcon";
import { NESSIE_REF_PREFIX } from "@app/constants/nessie";
import { TagContent } from "@app/pages/HomePage/components/BranchPicker/components/BranchPickerTag/BranchPickerTag";
import { NessieRootState } from "@app/reducers/nessie/nessie";
import { isDefaultReferenceLoading } from "@app/selectors/nessie/nessie";
import { connect } from "react-redux";

import "./CurrentRefInfo.less";

type ConnectedProps = {
  nessie: NessieRootState;
};
type CurrentRefInfoProps = {
  sources: any[];
};

function CurrentRefInfo({
  sources,
  nessie,
}: CurrentRefInfoProps & ConnectedProps) {
  if (!sources.length) return null;

  const firstSource = sources[0];
  const state = nessie[`${NESSIE_REF_PREFIX}${firstSource.name}`];
  const isLoading = isDefaultReferenceLoading(state);

  return (
    <span className="currentRefInfo">
      {isLoading ? (
        <FontIcon
          type="Loader spinner"
          theme={{ Icon: { width: 15, height: 15 } }}
        />
      ) : (
        <>
          <span
            title={firstSource.name}
            className="currentRefInfo-sourceName text-ellipsis"
          >
            {firstSource.name}
          </span>
          {state && state.reference && (
            <>
              <span
                className="currentRefInfo-refWrapper"
                title={state.reference.name}
              >
                <TagContent reference={state.reference} hash={state.hash} />
              </span>
              {sources.length > 1 && (
                <span className="currentRefInfo-more">
                  {`+${sources.length - 1}`}
                </span>
              )}
            </>
          )}
        </>
      )}
    </span>
  );
}
const mapStateToProps = (state: any) => ({
  nessie: state.nessie as NessieRootState,
});

export default connect(mapStateToProps)(CurrentRefInfo);
