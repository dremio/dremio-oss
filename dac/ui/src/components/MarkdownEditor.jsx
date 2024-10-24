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
import { PureComponent } from "react";
import PropTypes from "prop-types";
import { v4 as uuidv4 } from "uuid";
import classNames from "clsx";
import { debounce } from "lodash";
import SimpleMDE from "simplemde";
import "simplemde/dist/simplemde.min.css";
import "#oss/components/markedjsOverrides.js";
import { withErrorBoundary } from "#oss/components/OldErrorBoundary";
import additionalWikiControls from "@inject/shared/AdditionalWikiControls";
import { Spinner } from "dremio-ui-lib";
import {
  editor as editorCls,
  readMode as readModeCls,
  saveButton,
  cancelButton,
  loader,
  fitToParent as fitToParentCls,
} from "./MarkdownEditor.less";
import "./MarkdownEditorIcons.less";

// simple mde overrides ---------------------------

const refreshEditorPreview = (mdeEditor) => {
  const cm = mdeEditor.codemirror;
  const wrapper = cm.getWrapperElement();
  const preview = wrapper.lastChild;
  if (preview) {
    preview.innerHTML = mdeEditor.options.previewRender(
      mdeEditor.value(),
      preview,
    );
  }
};

(() => {
  const originalValue = SimpleMDE.prototype.value;

  const newValue = function (value) {
    const result = originalValue.call(this, value);

    if (value !== undefined && this.isPreviewActive()) {
      // make sure that preview is updated on value change
      refreshEditorPreview(this);
    }

    return result;
  };

  SimpleMDE.prototype.value = newValue;
})();

// End of "simple mde overrides" ------------------

const customMenus = {
  save: "Save",
  cancel: "Cancel",
  fullScreenMode: "dremio-full-screen",
};

export class MarkdownEditorView extends PureComponent {
  static propTypes = {
    value: PropTypes.string,
    readMode: PropTypes.bool, // Specify true if you need just display a wiki
    fitToContainer: PropTypes.bool, //Set true, if you need, that editor fits to it's parent container. It is a static property, which is applied only on component mount.
    className: PropTypes.string, // a css class name
    onChange: PropTypes.func, // (newValue: string) => {}, fires if content is changed
    onSaveClick: PropTypes.func, // (currentValue: string) => {}; Save menu item click handler
    onCancelClick: PropTypes.func, // () => {}; Cancel menu item click handler
    onReadModeHasScrollChanged: PropTypes.func, // (hasScroll: bool) => {} would be fired in readMode, when internal hasScroll will change it value
    fullScreenAvailable: PropTypes.bool,
    onFullScreenChanged: PropTypes.func, // (fullScreenMode: bool) => {}
    isModal: PropTypes.any,
    entityId: PropTypes.string,
    fullPath: PropTypes.any,
    setTutorialWikiEditor: PropTypes.func,
    showSummary: PropTypes.bool,
  };

  static defaultProps = {
    readMode: true,
  };

  _id = uuidv4();
  _hasScroll = false;
  _isMounted = false;

  state = {
    isLoading: true,
    showSummary: this.props.showSummary,
    fullScreenMode: false, //only works if readMode = false. Defined whether editor in a full screen side by side mode
  };

  async componentDidMount() {
    this._isMounted = true;
    if (additionalWikiControls()?.summary) {
      const additionalToolbarButton =
        await additionalWikiControls()?.additionalToolbarButton(
          this.toggleSummary,
        );
      if (!this._isMounted) return;
      this.setState({ isLoading: false });
      this.createEditor(additionalToolbarButton);
    } else {
      this.setState({ isLoading: false });
      this.createEditor();
    }

    this.handlePropsChange(undefined, undefined, this.props, this.state); // we should set initial editor state. Reuse componentDidUpdate for that purposes.
    this.updateHasScroll();
  }

  componentWillUnmount() {
    if (this.editor) {
      this.editor.toTextArea();
      this.props.setTutorialWikiEditor?.(null);
    }
    this.editor = null; // release resources
    this._isMounted = false;
  }

  createEditor = (additionalToolbarButton) => {
    const { value } = this.props;

    this.editor = new SimpleMDE({
      autoDownloadFontAwesome: false,
      toolbar: this.getToolbar(additionalToolbarButton), // should be rendered in any mode. Toolbar would be hidden via styles in read mode
      initialValue: value,
      spellChecker: false,
      status: false,
      shortcuts: {
        // reset key bindings. As these operation would be controlled by internal logic
        toggleSideBySide: null,
        toggleFullScreen: null,
        togglePreview: null,
      },
      element: document.getElementById(this._id),
    });

    this.editor.codemirror.on("change", () => {
      const { onChange } = this.props;

      if (onChange) {
        onChange(this.editor.value());
      }
    });

    this.props.setTutorialWikiEditor?.(this.editor);
  };

  static getDerivedStateFromProps(nextProps, prevState) {
    return {
      fullScreenMode: getFullScreenFlag(
        nextProps.readMode,
        prevState.fullScreenMode,
      ),
    };
  }

  componentDidUpdate(prevProps = {}, prevState = {}) {
    this.handlePropsChange(prevProps, prevState, this.props, this.state);
  }

  handlePropsChange = (prevProps = {}, prevState = {}, newProps, newState) => {
    const editor = this.editor;
    if (!editor) return;

    const { readMode, value, onFullScreenChanged } = newProps;

    const { fullScreenMode } = newState;

    const valueChanged = value !== prevProps.value;
    //check if we need to update a value. This check must be first.
    if (
      (valueChanged || // value property is changed
        readMode) && // we should make sure, that in readMode UI displays a value from properties
      value !== this.editor.value() // displayed value is differs from value property
    ) {
      editor.value(value);
    }

    const readModeChanged = readMode !== prevProps.readMode;
    if (readModeChanged) {
      this.setReadMode(readMode);
    }

    const fullScreenModeChanged = fullScreenMode !== prevState.fullScreenMode;
    if (fullScreenModeChanged) {
      // check fullScreenMode in that case
      this.setFullScreenMode(fullScreenMode);
      if (onFullScreenChanged) {
        onFullScreenChanged(fullScreenMode);
      }
    }

    if (readMode) {
      // we should make sure, that in readMode UI displays a value from properties
      refreshEditorPreview(this.editor);
    }

    if (
      readMode &&
      (readModeChanged || fullScreenModeChanged || valueChanged)
    ) {
      this.updateHasScroll();
    }
  };

  setReadMode = (readMode) => {
    const editor = this.editor;
    const isPreview = editor.isPreviewActive();

    if (readMode) {
      if (!isPreview) {
        // activate preview
        editor.togglePreview();
      }
    } else if (isPreview) {
      // disable preview, if it is a preview mode
      editor.togglePreview();
      editor.codemirror.refresh(); // refresh a code mirror as it has just become visible (see https://github.com/codemirror/CodeMirror/issues/2469#issuecomment-40575940)
    }
  };

  setFullScreenMode = (fullScreenMode) => {
    const editor = this.editor;
    const isViewInFullScreen = editor.isFullscreenActive();
    const isViewInSideBySide = editor.isSideBySideActive();

    const menu = editor.toolbarElements[customMenus.fullScreenMode];
    if (menu) {
      this.toggleClass(menu, "active", fullScreenMode);
    }

    if (fullScreenMode) {
      if (!isViewInFullScreen) {
        editor.toggleFullScreen();
      }
      if (!isViewInSideBySide) {
        editor.toggleSideBySide();
      }
    } else {
      // we should disable side by side mode and exit from full screen mode
      if (isViewInSideBySide) {
        editor.toggleSideBySide();
      }
      if (isViewInFullScreen) {
        editor.toggleFullScreen();
      }
    }
  };

  toggleClass(el, className, addClass /*: true|false */) {
    if (!el) return;

    // as IE does not support classList.toggle method with the second argument, we have to manipulate with basic classList api
    const hasClass = el.classList.contains(className);
    if (hasClass && !addClass) {
      el.classList.remove(className);
    } else if (!hasClass && addClass) {
      el.classList.add(className);
    }
  }

  toggleFullScreen = () => {
    this.setState(
      (/* prevState */ { fullScreenMode }, /* props */ { readMode }) => {
        return {
          fullScreenMode: getFullScreenFlag(readMode, !fullScreenMode),
        };
      },
    );
  };

  toggleSummary = () => {
    this.setState({
      showSummary: !this.state.showSummary,
    });
  };

  getMdeInstance = (editor) => {
    this.editor = editor;
  };

  focus() {
    if (this.editor) this.editor.codemirror.focus();
  }

  //needed as instance method for tests
  _updateHasScrollImpl = () => {
    const { onReadModeHasScrollChanged, readMode } = this.props;

    // we will track a scroll only in a readMode. It is needed for rare use case. It does not make sense to run the calculation in other case to not overkill widget preformance
    if (!readMode || !onReadModeHasScrollChanged || !this.editor) {
      return;
    }

    const newHasScroll = this.hasScrollInReadMode();

    if (newHasScroll !== this._hasScroll) {
      this._hasScroll = newHasScroll;
      onReadModeHasScrollChanged(newHasScroll);
    }
  };

  // we have to delay a bit this calculation to let scroll bar time to appear.
  updateHasScroll = debounce(this._updateHasScrollImpl, 100, {
    trailing: true,
    maxWait: 500, // the event could not be delayed more than for half of a second
  });

  hasScrollInReadMode = () => {
    const { readMode } = this.props;

    if (!readMode) {
      throw new Error("This call is allowed only a in read mode");
    }

    const wrapper = this.editor.codemirror.getWrapperElement(); // get a wrapper of code mirror. This line base on knowledge of internal stuctire of simple mde.
    const previewEl = wrapper.getElementsByClassName("editor-preview")[0];

    return previewEl && previewEl.clientHeight < previewEl.scrollHeight;
  };

  getValue() {
    return this.editor ? this.editor.value() : this.props.value;
  }

  onSaveClick = () => {
    const { onSaveClick, value } = this.props;

    if (onSaveClick) {
      onSaveClick(this.editor ? this.editor.value() : value);
    }
  };

  getToolbar = (additionalToolbarButton) => {
    const { onCancelClick, onSaveClick, fullScreenAvailable } = this.props;

    // simple mde has a bug, that menu items should be presented to use some functionality.
    // That is why 'fullscreen', 'side-by-side', 'preview' are added here, but these menu options would be hidden using css.
    const buttons = [
      "bold",
      "italic",
      "strikethrough",
      "heading",
      "|",
      "quote",
      "unordered-list",
      "ordered-list",
      "|",
      "link",
      "image",
    ];

    if (additionalToolbarButton) {
      buttons.push(additionalToolbarButton);
    }

    if (fullScreenAvailable) {
      buttons.push("|", {
        name: customMenus.fullScreenMode,
        title: "full screen",
        action: this.toggleFullScreen,
        className: "fa dremio-menu fa-arrows-alt no-disable", // no-disable is needed to not disable this menu item in preview mode
      });
    }

    // not displayed on the ui, but have to add
    // buttons.push("preview", "fullscreen", "side-by-side");

    //custom buttons, that are added to the right of the toolbar. Put buttons in reverse order, as floating is applied
    if (onCancelClick) {
      buttons.push({
        name: customMenus.cancel,
        title: "Cancel",
        action: onCancelClick,
        className: classNames(cancelButton, "no-disable"),
      });
    }
    if (onSaveClick) {
      buttons.push({
        name: customMenus.save,
        title: "Save",
        action: this.onSaveClick,
        className: classNames(saveButton, "no-disable"),
      });
    }

    return buttons;
  };

  render() {
    const { readMode, className, fitToContainer, isModal, entityId, fullPath } =
      this.props;
    return (
      <>
        <div
          className={classNames(
            editorCls,
            readMode && readModeCls,
            fitToContainer && fitToParentCls,
            className,
          )}
        >
          <textarea
            id={this._id}
            style={{ display: this.state.isLoading ? "none" : "default" }}
          />
          {this.state.isLoading && (
            <div className={loader}>
              <Spinner size="xl" />
            </div>
          )}
        </div>
        {additionalWikiControls()?.summary?.({
          entityId,
          fullPath,
          shouldRender: !this.state.isLoading && this.state.showSummary,
          editor: this.editor,
        })}
      </>
    );
  }
}

const getFullScreenFlag = (readMode, fullScreen) => {
  return !readMode && fullScreen;
};

export default withErrorBoundary(MarkdownEditorView);
