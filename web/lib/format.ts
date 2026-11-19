export function formatClock(ms: number): string {
  if (!ms) {
    return "--:--:--";
  }
  return new Date(ms).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

export function formatAgo(ms: number, now = Date.now()): string {
  if (!ms) {
    return "never";
  }
  const delta = Math.max(0, now - ms);
  if (delta < 1000) {
    return "just now";
  }
  const seconds = Math.floor(delta / 1000);
  if (seconds < 60) {
    return `${seconds}s ago`;
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m ${seconds % 60}s ago`;
  }
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m ago`;
}

export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) {
    return "0s";
  }
  const totalSeconds = Math.floor(ms / 1000);
  if (totalSeconds < 60) {
    return `${totalSeconds}s`;
  }
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) {
    return `${minutes}m ${seconds}s`;
  }
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

export function formatValue(value: string | undefined | null): string {
  if (value === undefined || value === null) {
    return "";
  }
  return value;
}

export function truncate(value: string, max = 22): string {
  if (value.length <= max) {
    return value;
  }
  return `${value.slice(0, max - 1)}…`;
}

// Curated CRT phosphor palette so consecutive terms read as distinct colours
// (amber, cyan, magenta, lime, orange, violet) instead of arbitrary hues.
const TERM_HUES = [36, 184, 318, 92, 14, 268, 52, 158];

export function termHue(term: number): number {
  const index = ((term % TERM_HUES.length) + TERM_HUES.length) % TERM_HUES.length;
  return TERM_HUES[index];
}

export function termColor(term: number, alpha = 1): string {
  return `hsla(${termHue(term)}, 88%, 63%, ${alpha})`;
}

export function termSoftColor(term: number, alpha = 0.14): string {
  return `hsla(${termHue(term)}, 82%, 58%, ${alpha})`;
}

export function plural(count: number, singular: string, pluralForm?: string) {
  return count === 1 ? singular : (pluralForm ?? `${singular}s`);
}

export function shortPeer(addr: string): string {
  const port = addr.split(":").pop() ?? addr;
  const numeric = Number(port);
  if (Number.isFinite(numeric) && numeric >= 7000 && numeric <= 7999) {
    return `N${numeric - 7000}`;
  }
  return addr;
}
