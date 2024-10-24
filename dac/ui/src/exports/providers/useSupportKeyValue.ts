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

import { SupportKeyState } from "#oss/queries/flags";
import { supportKey } from "@inject/queries/flags";
import { useSuspenseQuery } from "@tanstack/react-query";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { ReactElement } from "react";

/**
 * Fetches a support key using Suspense to automatically throw loading state to the parent
 * @param defaultValue A fallback value to return if there was an error fetching the key
 */
export const useSupportKeyValue = <T extends SupportKeyState["value"]>(
  key: string,
  defaultValue?: T,
): T =>
  useSuspenseQuery(
    supportKey(getSonarContext().getSelectedProjectId?.())(key, defaultValue),
  ).data.value as T;

type SupportKeyValueProps<T extends SupportKeyState["value"]> = {
  children: (supportKeyState: T) => ReactElement | null;
  supportKey: string;
  defaultValue?: T;
};

/**
 * Fetches a support key using Suspense to automatically throw loading state to the parent.
 * Render prop provider makes it easier to nest the support key provider deeper in the tree.
 */
export const SupportKeyValue = <T extends SupportKeyState["value"]>(
  props: SupportKeyValueProps<T>,
) => props.children(useSupportKeyValue(props.supportKey, props.defaultValue));
