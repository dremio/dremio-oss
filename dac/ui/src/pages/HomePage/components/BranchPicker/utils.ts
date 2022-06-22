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
import { createContext, useContext, useRef } from "react";
import { InjectedProps as PopupState } from "material-ui-popup-state";

type BranchPickerContextType = {
  ref: React.Ref<PopupState | null>;
};

export const BranchPickerContext =
  createContext<BranchPickerContextType | null>(null);

export function useBranchPickerContext(): BranchPickerContextType {
  const context = useContext(BranchPickerContext) || {};
  return context as BranchPickerContextType;
}
// Create a context value to pass into a provider
// Putting this here so consumption doesn't require importing PopupState
export function useContextValue() {
  return {
    ref: useRef<PopupState>(null),
  };
}
