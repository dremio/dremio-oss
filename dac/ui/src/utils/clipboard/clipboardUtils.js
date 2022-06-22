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

// Copies a string to the clipboard.
// Must be called from within an event handler such as click.
export function copyTextToClipboard(text) {
  // first try navigator.clipboard, which may require a secure origin — either HTTPS or localhost
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard
      .writeText(text)
      .then(() => {
        return true;
      })
      .catch((err) => {
        console.error("Could not copy text to clipboard: ", err);
        return false;
      });
  }

  // then try window.clipboardData
  if (window.clipboardData && window.clipboardData.setData) {
    // IE specific code path to prevent textarea being shown while dialog is visible.
    return window.clipboardData.setData("Text", text);
  }

  // then try document.execCommand
  //@note: For some strange reason this code works if called not directly from a click
  // handler, as recommended by documentation, but if called asynchronously.
  // So, if the app is used in non-secure environment with navigator.clipboard (above) working, then
  // this function should be called via window.setTimeout(copyTextToClipboard(this, text), 1); or in
  // a similar async mode.
  if (
    document.queryCommandSupported &&
    document.queryCommandSupported("copy")
  ) {
    let success = false;
    const textarea = document.createElement("textarea");
    textarea.textContent = text;
    textarea.style.position = "fixed"; // Prevent scrolling to bottom of page in MS Edge.
    textarea.style.zIndex = "-1";
    document.body.appendChild(textarea);
    textarea.select();
    try {
      success = document.execCommand("copy"); // Security exception may be thrown by some browsers.
    } catch (ex) {
      console.warn("Copy to clipboard failed.", ex);
    } finally {
      document.body.removeChild(textarea);
    }
    return success;
  }

  // none of the methods are avaialable
  return false;
}
