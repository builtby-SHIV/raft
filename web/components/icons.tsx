import type { SVGProps } from "react";

type IconProps = SVGProps<SVGSVGElement>;

const base = {
  viewBox: "0 0 24 24",
  fill: "none",
  stroke: "currentColor",
  strokeWidth: 1.8,
  strokeLinecap: "round" as const,
  strokeLinejoin: "round" as const,
  "aria-hidden": true,
};

export function PlayIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="M7 4.5v15l13-7.5-13-7.5Z" fill="currentColor" stroke="none" />
    </svg>
  );
}

export function StopIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <rect x="5" y="5" width="14" height="14" rx="2" fill="currentColor" stroke="none" />
    </svg>
  );
}

export function SkullIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="M5 11a7 7 0 1 1 14 0c0 2.2-1 3.5-2 4.5V19a2 2 0 0 1-2 2h-1v-2h-2v2H9a2 2 0 0 1-2-2v-3.5C6 14.5 5 13.2 5 11Z" />
      <circle cx="9.2" cy="11" r="1.4" fill="currentColor" stroke="none" />
      <circle cx="14.8" cy="11" r="1.4" fill="currentColor" stroke="none" />
      <path d="M10.5 16h3" />
    </svg>
  );
}

export function RestartIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="M3 11a9 9 0 1 1 2.6 6.4" />
      <path d="M3 5v6h6" />
    </svg>
  );
}

export function TerminalIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <rect x="3" y="4" width="18" height="16" rx="2.5" />
      <path d="m7 9 3 3-3 3M13 15h4" />
    </svg>
  );
}

export function PlusIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="M12 5v14M5 12h14" />
    </svg>
  );
}

export function TrashIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="M4 7h16M9 7V5a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2M6 7l1 13h10l1-13" />
      <path d="M10 11v5M14 11v5" />
    </svg>
  );
}

export function DatabaseIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <ellipse cx="12" cy="5.5" rx="7" ry="2.5" />
      <path d="M5 5.5v6c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5v-6" />
      <path d="M5 11.5v6c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5v-6" />
    </svg>
  );
}

export function PulseIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="M3 12h4l2.5-6 4 12 2.5-6h5" />
    </svg>
  );
}

export function XIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="m6 6 12 12M18 6 6 18" />
    </svg>
  );
}

export function RefreshIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="M20 11a8 8 0 1 0-.8 4.3" />
      <path d="M20 5v6h-6" />
    </svg>
  );
}

export function LayersIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="m12 3 9 5-9 5-9-5 9-5Z" />
      <path d="m3 12 9 5 9-5M3 16l9 5 9-5" />
    </svg>
  );
}

export function WifiIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="M4.5 9.5a11 11 0 0 1 15 0M7.5 13a7 7 0 0 1 9 0M10.5 16.5a3 3 0 0 1 3 0" />
      <circle cx="12" cy="19" r="1" fill="currentColor" stroke="none" />
    </svg>
  );
}

export function WifiOffIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="M4.5 9.5a11 11 0 0 1 4.2-2.4M15.8 7.5a11 11 0 0 1 3.7 2M7.5 13a7 7 0 0 1 3-1.6M15.2 12.4a7 7 0 0 1 1.3.6M10.5 16.5a3 3 0 0 1 3 0" />
      <circle cx="12" cy="19" r="1" fill="currentColor" stroke="none" />
      <path d="m4 4 16 16" />
    </svg>
  );
}

export function CheckIcon(props: IconProps) {
  return (
    <svg {...base} {...props}>
      <path d="m5 12.5 4.5 4.5L19 7" />
    </svg>
  );
}
