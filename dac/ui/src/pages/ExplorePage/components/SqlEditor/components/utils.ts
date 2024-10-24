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
import { useEffect, useRef, useState } from "react";
import { useDispatch, useSelector } from "react-redux";

import { NESSIE_REF_PREFIX } from "#oss/constants/nessie";
import {
  type DatasetReference,
  fetchDefaultReferenceIfNeeded as fetchDefaultRef,
} from "#oss/actions/nessie/nessie";
import {
  getEndpointFromSource,
  getNessieReferencePayload,
} from "#oss/utils/nessieUtils";
import { selectSourceByName } from "#oss/selectors/home";
import { getApiV2 } from "#oss/services/nessie/impl/TreeApi";
import { isDefaultReferenceLoading } from "#oss/selectors/nessie/nessie";
import { constructFullPath } from "#oss/utils/pathUtils";
import { type NessieRootState } from "#oss/types/nessie";
import { isEmpty } from "lodash";

const DEFAULT_CONTEXT = "<none>";

export const useInitializeNessieState = (sourceName?: string) => {
  const dispatch = useDispatch();
  // Fetch the default reference if context is set to a source and no branch is fetched/set
  const source = useSelector(selectSourceByName(sourceName));
  const apiV2 = source ? getApiV2(getEndpointFromSource(source)) : null;
  useEffect(() => {
    if (!apiV2) return;
    fetchDefaultRef(NESSIE_REF_PREFIX + sourceName, apiV2)(dispatch);
  }, [apiV2, sourceName, dispatch]);
};

export const useCtxPickerActions = ({
  nessieState,
  resetRefs,
}: {
  nessieState: NessieRootState | null;
  resetRefs: (refs: DatasetReference) => void;
}) => {
  const datasetRefs = getNessieReferencePayload(nessieState);

  const [show, setShow] = useState(false);
  const initialRefState = useRef<any>(null);
  const open = () => {
    // Any pre-existing refs will be restored on cancel
    initialRefState.current = datasetRefs;
    setShow(true);
  };
  const close = () => {
    initialRefState.current = null;
    setShow(false);
  };
  const cancel = () => {
    const curRefs = initialRefState.current;
    if (curRefs && !isEmpty(curRefs)) {
      resetRefs(curRefs); // Reset state after cancel
    }
    close();
  };

  return [show, open, close, cancel] as const;
};

export const getCtxState = ({
  sourceName,
  nessieState = {} as NessieRootState,
}: {
  sourceName: string;
  nessieState: NessieRootState | null;
}) => {
  const stateKey = NESSIE_REF_PREFIX + sourceName;
  const refState = nessieState?.[stateKey];

  const isLoading =
    !!refState &&
    !refState.reference?.name &&
    !refState.hash &&
    isDefaultReferenceLoading(refState);

  return [refState, isLoading] as const;
};

export const getContextValue = (context?: any) => {
  const contextValue = context
    ? constructFullPath(context, true)
    : DEFAULT_CONTEXT;
  const emptyContext =
    typeof contextValue === "string" && contextValue.trim() === "";
  const textValue = emptyContext
    ? laDeprecated("(None selected)")
    : contextValue;

  return textValue;
};
