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
import Immutable from "immutable";
import { useState, useContext } from "react";
import { useDispatch } from "react-redux";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import {
  Button,
  ModalContainer,
  DialogContent,
} from "dremio-ui-lib/components";
import Message from "@app/components/Message";
import {
  handleAddFolder,
  type versionContext,
} from "@app/endpoints/AddFolder/AddFolder";
import { useForm } from "react-hook-form";
import { TextInput } from "@mantine/core";
import { constructFullPath } from "utils/pathUtils";
import { loadResourceTree } from "@app/actions/resources/tree";
import {
  RESOURCE_TREE_VIEW_ID,
  LOAD_RESOURCE_TREE,
} from "@app/components/Tree/resourceTreeUtils";
import { TreeConfigContext } from "../Tree/treeConfigContext";
import * as classes from "./AddFolderDialog.module.less";

type AddFolderDialogProps = {
  open: boolean;
  close: () => void;
  node: Immutable.Map<string, any>;
  preselectedNodeId: string;
  selectedVersionContext: versionContext;
  isArsEnabled: boolean;
};
export const AddFolderDialog = ({
  open,
  close,
  node,
  preselectedNodeId,
  selectedVersionContext,
  isArsEnabled,
}: AddFolderDialogProps): JSX.Element => {
  const { t } = getIntlContext();
  const [error, setError] = useState(undefined);
  const fullPath = node?.get("fullPath")?.toJS() || [
    preselectedNodeId.replace(/['"]+/g, ""),
  ];
  const rootType = () => {
    if (node) {
      return node.get("rootType").toLowerCase();
    } else {
      return isArsEnabled ? "source" : "home";
    }
  };
  const dispatch = useDispatch();
  const { resourceTreeControllerRef } = useContext(TreeConfigContext);
  const {
    register,
    handleSubmit,
    formState: { isValid, isSubmitting },
  } = useForm({ mode: "onChange" });

  const onSubmit = async ({ name }: { name: string }) => {
    const rootName = fullPath[0];
    const folderPath = fullPath.slice(1).join("/");
    const constructedPath = constructFullPath([...fullPath]);
    const createdFolderPath = constructFullPath([...fullPath, name]);
    try {
      await handleAddFolder(
        rootType(),
        rootName,
        folderPath,
        { name },
        selectedVersionContext
      );
      const treeResponse = await dispatch(
        loadResourceTree(
          LOAD_RESOURCE_TREE,
          RESOURCE_TREE_VIEW_ID,
          constructedPath,
          {
            showDatasets: false,
            showSources: true,
            showSpaces: true,
            showHomes: true,
          },
          true,
          node,
          true
        ) as any
      );
      if (treeResponse && resourceTreeControllerRef) {
        resourceTreeControllerRef.current.handleSelectedNodeChange(
          createdFolderPath
        );
        resourceTreeControllerRef.current.expandPathToSelectedNode(
          createdFolderPath
        );
        close();
      }
    } catch (e: any) {
      setError(e?.responseBody?.errorMessage);
    }
  };
  const renderVersionContext = () => {
    if (selectedVersionContext?.refValue) {
      return (
        <span>
          <dremio-icon
            name="vcs/branch"
            alt="branch"
            class={`${classes["icon"]} mx-05`}
          ></dremio-icon>
          {selectedVersionContext.refValue}
        </span>
      );
    }
  };
  return (
    <ModalContainer open={() => {}} isOpen={open} close={close}>
      <form
        className={classes["form"]}
        onSubmit={(e) => {
          e.stopPropagation();
          handleSubmit(onSubmit)(e);
        }}
      >
        <DialogContent
          toolbar={
            <dremio-icon
              name="interface/close-big"
              alt=""
              class={classes["close-icon"]}
              onClick={close}
            />
          }
          error={error && <Message message={error} messageType="error" />}
          className={classes["add-folder-dialog"]}
          title={t("Create.Folder")}
          actions={
            <>
              <Button
                variant="secondary"
                onClick={close}
                disabled={isSubmitting}
              >
                {t("Common.Actions.Cancel")}
              </Button>
              <Button
                variant="primary"
                type="submit"
                disabled={!isValid}
                pending={isSubmitting}
              >
                {t("Common.Actions.Create")}
              </Button>
            </>
          }
        >
          <p>{t("Add.To")}</p>
          <p className="mb-3 mt-1 dremio-typography-less-important">
            {fullPath[fullPath.length - 1]}
            {renderVersionContext()}
          </p>
          <TextInput
            label={t("Folder.Name")}
            {...register("name", {
              validate: (value) => value.length > 0,
            })}
            disabled={false}
            className="mt-1"
          />
        </DialogContent>
      </form>
    </ModalContainer>
  );
};
