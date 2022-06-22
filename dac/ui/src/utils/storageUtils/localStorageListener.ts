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

/**
 * Local storage listener
 * - Add/remove listeners using exported methods rather than appending multiple listeners to window
 */

type Listener = (event: StorageEvent) => void;
const listeners: Listener[] = [];

window.addEventListener("storage", (event) => {
  if (event.storageArea != localStorage) return;
  for (let i = 0; i < listeners.length; i++) {
    listeners[i](event);
  }
});

export function add(listener: Listener) {
  listeners.push(listener);
  return () => {
    remove(listener);
  };
}

export function remove(listener: Listener) {
  listeners.splice(listeners.indexOf(listener), 1);
}
