import fs from "node:fs";
import path from "node:path";

/**
 * The Next.js app lives in <repo>/web, but the Go node binary must run from
 * the repository root so it uses <repo>/data for its WAL. Walk upwards until
 * go.mod is found so both `next dev` from web/ and from the repo root work.
 */
export function findRepoRoot(): string {
  let dir = process.cwd();
  for (let depth = 0; depth < 8; depth += 1) {
    if (fs.existsSync(path.join(dir, "go.mod"))) {
      return dir;
    }
    const parent = path.dirname(dir);
    if (parent === dir) {
      break;
    }
    dir = parent;
  }
  return path.resolve(process.cwd(), "..");
}
