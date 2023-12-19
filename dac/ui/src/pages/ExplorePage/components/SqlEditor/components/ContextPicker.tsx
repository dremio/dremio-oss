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
import clsx from "clsx";
import { constructFullPath } from "@app/utils/pathUtils";
import { useCallback, useRef } from "react";
import Modal from "components/Modals/Modal";
import SelectContextForm from "../../forms/SelectContextForm";
import { TagContent } from "@app/pages/HomePage/components/BranchPicker/components/BranchPickerTag/BranchPickerTag";
import { Spinner } from "dremio-ui-lib/components";
import {
  getContextValue,
  getCtxState,
  useCtxPickerActions,
  useInitializeNessieState,
} from "./utils";

import * as classes from "./ContextPicker.module.less";
import { useDispatch } from "react-redux";
import {
  resetRefs as resetRefsAction,
  type DatasetReference,
} from "@app/actions/nessie/nessie";
import { useSelector } from "react-redux";
import { getHomeSource, getSortedSources } from "@app/selectors/home";
import { useIsArsEnabled } from "@inject/utils/arsUtils";
import { clearResourceTree } from "@app/actions/resources/tree";
import { getViewState } from "@app/selectors/resources";
import { useFilterTreeArs } from "@app/utils/datasetTreeUtils";

type ContextPickerProps = {
  value: any;
  className?: string;
  onChange: (newValue: any) => void;
};

export const ContextPicker = ({
  value: context,
  onChange,
  className,
}: ContextPickerProps) => {
  const sourceName = context?.get(0);
  useInitializeNessieState(sourceName); //Fetch default reference if required
  const [isArsLoading, isArsEnabled] = useIsArsEnabled();
  const filterTree = useFilterTreeArs();

  const dispatch = useDispatch();
  const resetRefs = useCallback(
    (refs: DatasetReference) => {
      resetRefsAction(refs)(dispatch);
    },
    [dispatch]
  );

  const isSourcesLoading = useSelector(
    (state) => getViewState(state, "AllSources")?.get("isInProgress") ?? true
  );
  const homeSource = useSelector((state) =>
    getHomeSource(getSortedSources(state))
  );
  const nessieState = useSelector((state: any) => state.nessie);
  const [refState, refLoading] = getCtxState({
    sourceName,
    nessieState,
  });
  const [show, open, close, cancel] = useCtxPickerActions({
    nessieState,
    resetRefs,
  });

  const ref = useRef<HTMLSpanElement>(null);
  const textValue = getContextValue(context);
  const title = refState?.hash || refState?.reference?.name;

  const Content = (
    <>
      <span
        title={textValue}
        className={clsx("branchPickerTag-labelDiv", classes["context"])}
      >
        {textValue}
      </span>
      <span className={classes["branch"]}>
        {refLoading ? (
          <Spinner />
        ) : (
          refState?.reference && (
            <div {...(!!title && { title })} className={classes["contextTag"]}>
              <TagContent reference={refState.reference} hash={refState.hash} />
            </div>
          )
        )}
      </span>
    </>
  );

  // Workaround to prevent doube-fetch when expanding pre-existing nodes
  const clearResources = () => {
    dispatch(clearResourceTree({ fromModal: true }));
  };
  const handleCancel = () => {
    clearResources();
    cancel();
  };
  const handleClose = () => {
    clearResources();
    close();
  };

  return (
    <>
      <div className="sqlAutocomplete__context">
        <>
          <span className={classes["offset"]}>{laDeprecated("Context:")}</span>
          <span
            ref={ref}
            className={clsx(className, refLoading && classes["loadingWrapper"])}
            onClick={open}
          >
            {Content}
          </span>
        </>
      </div>
      {!isArsLoading && !isSourcesLoading && show && (
        <Modal isOpen hide={handleCancel} size="small" modalHeight="600px">
          <SelectContextForm
            onFormSubmit={(resource: any) => {
              onChange(resource.context);
              handleClose();
            }}
            onCancel={handleCancel}
            initialValues={{ context: constructFullPath(context) }}
            filterTree={filterTree}
            {...(isArsEnabled &&
              !!homeSource && {
                getPreselectedNode: (context: string) => {
                  return context.startsWith(homeSource.get("name")) ||
                    // SelectContextForm will send in `value.`, not sure why
                    context === `${homeSource.get("name")}.`
                    ? homeSource.get("name")
                    : context;
                },
              })}
          />
        </Modal>
      )}
    </>
  );
};
