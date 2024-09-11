import clsx from "clsx";
import React, { FC, InputHTMLAttributes } from "react";

type RadioProps = {
  className?: string;
  label?: string;
};
export const Radio: FC<RadioProps & InputHTMLAttributes<HTMLInputElement>> = (
  props: RadioProps,
) => {
  return (
    <label style={{ display: "flex", alignItems: "center" }}>
      <input
        {...props}
        className={clsx(props.className, "form-control")}
        type="radio"
      />
      {props.label && <span className="ml-1">{props.label}</span>}
    </label>
  );
};
