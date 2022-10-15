import * as React from "react";
import clsx from "clsx";

//@ts-ignore
const externalIcon = (
  //@ts-ignore
  <dremio-icon
    name="interface/external-link"
    class="dremio-external-link__icon"
    //@ts-ignore
  ></dremio-icon>
);

type ExternalLinkProps = {
  children: JSX.Element;
  className?: string;
  variant?: "inline" | "list";
};

export const ExternalLink = React.forwardRef<
  HTMLAnchorElement,
  ExternalLinkProps
>((props, ref): JSX.Element => {
  const { children, className, variant = "inline", ...rest } = props;
  return (
    <a
      target="_blank"
      {...rest}
      ref={ref}
      className={clsx(
        "dremio-external-link",
        `dremio-external-link--${variant}`,
        className
      )}
    >
      {variant === "inline" && (
        <>
          {children} {externalIcon}
        </>
      )}
      {variant === "list" && (
        <>
          {externalIcon} {children}
        </>
      )}
    </a>
  );
});
