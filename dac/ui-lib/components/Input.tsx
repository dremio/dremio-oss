/* eslint-disable react/prop-types */
//@ts-nocheck
import { forwardRef, HTMLProps, useRef, useState } from "react";
import mergeRefs from "react-merge-refs";
import clsx from "clsx";

type InputProps = Omit<HTMLProps<HTMLInputElement>, "prefix"> & {
  wrapperRef?: any;
  clearable?: boolean;
  prefix?: JSX.Element;
  textPrefix?: string;
  suffix?: JSX.Element;
};

export const Input = forwardRef<HTMLInputElement, InputProps>((props, ref) => {
  const inputEl = useRef(null);
  const [internalValue, setInternalValue] = useState(props.value);
  const {
    wrapperRef,
    clearable,
    prefix,
    textPrefix,
    suffix,
    style,
    className,
    ...rest
  } = props;
  const handleClear = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setInternalValue("");
    inputEl.current.value = "";
    inputEl.current.dispatchEvent(new Event("input", { bubbles: true }));
    inputEl.current.focus();
  };
  return (
    <div
      className={clsx(className, "form-control")}
      style={style}
      {...(wrapperRef && { ref: wrapperRef })}
      aria-disabled={props.disabled}
    >
      {textPrefix ? (
        <div className="form-control__prefix">{textPrefix}</div>
      ) : (
        prefix
      )}
      <input
        ref={mergeRefs([ref, inputEl])}
        {...rest}
        onChange={(e) => {
          setInternalValue(e.target.value);
          props.onChange?.(e);
        }}
      />
      {suffix}
      {clearable && internalValue?.length > 0 && (
        <button
          className="dremio-icon-button m-0"
          onClick={handleClear}
          type="button"
          tabIndex={-1}
        >
          <dremio-icon name="interface/close-small"></dremio-icon>
        </button>
      )}
    </div>
  );
});
