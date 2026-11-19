import type { Metadata } from "next";
import { Chakra_Petch, Orbitron, Share_Tech_Mono } from "next/font/google";

import "./globals.css";

// Retro-futurist type system: a Eurostile-flavoured display face for headings,
// a squared technical grotesque for interface copy, and a phosphor terminal
// mono for every number and log line.
const displayFont = Orbitron({
  subsets: ["latin"],
  weight: ["500", "600", "700", "800", "900"],
  variable: "--font-display",
  display: "swap",
});

const uiFont = Chakra_Petch({
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
  variable: "--font-ui",
  display: "swap",
});

const monoFont = Share_Tech_Mono({
  subsets: ["latin"],
  weight: "400",
  variable: "--font-mono",
  display: "swap",
});

export const metadata: Metadata = {
  title: "Raft Live Lab — Consensus Telemetry",
  description:
    "A retro-futurist mission console for a five-process Raft cluster: entry replication, quorum commits, process crashes and catch-up.",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html
      lang="en"
      className={`${displayFont.variable} ${uiFont.variable} ${monoFont.variable}`}
    >
      <body>{children}</body>
    </html>
  );
}
