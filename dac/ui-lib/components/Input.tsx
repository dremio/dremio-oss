/* eslint-disable react/prop-types */
//@ts-nocheck
import { forwardRef, HTMLProps, useEffect, useRef, useState } from "react";
import mergeRefs from "react-merge-refs";
import clsx from "clsx";
import { Spinner } from "./Spinner/Spinner";
import { CSSTransition } from "react-transition-group";

type InputProps = Omit<HTMLProps<HTMLInputElement>, "prefix"> & {
  wrapperRef?: any;
  clearable?: boolean;
  pending?: boolean;
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
    value,
    pending,
    ...rest
  } = props;
  const handleClear = () => {
    setInternalValue("");
    inputEl.current.value = "";
    inputEl.current.dispatchEvent(new Event("input", { bubbles: true }));
    inputEl.current.focus();
  };
  useEffect(() => {
    setInternalValue(value);
  }, [value]);
  const spinnerRef = useRef<HTMLDivElement>(null);
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
        value={value}
        onChange={(e) => {
          setInternalValue(e.target.value);
          props.onChange?.(e);
        }}
        onKeyDown={(e) => {
          if (e.key === "Escape" && clearable && internalValue !== "") {
            e.preventDefault();
            e.stopPropagation();
            handleClear();
          }

          props.onKeyDown?.(e);
        }}
      />
      <CSSTransition
        addEndListener={(done) =>
          spinnerRef.current?.addEventListener("transitionend", done, false)
        }
        classNames="transition-fade-in-out-delayed"
        in={pending}
        nodeRef={spinnerRef}
        unmountOnExit
      >
        <div ref={spinnerRef} className="transition-fade-in-out-delayed">
          <Spinner />
        </div>
      </CSSTransition>
      {suffix}
      {clearable && internalValue?.length > 0 && (
        <button
          className="dremio-icon-button m-0"
          onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            handleClear();
          }}
          type="button"
          tabIndex={-1}
        >
          <dremio-icon name="interface/close-small"></dremio-icon>
        </button>
      )}
    </div>
  );
});
