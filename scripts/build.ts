import { build as esbuild } from "esbuild";
import path from "node:path";
import { randomUUID } from "node:crypto";
import url from "node:url";

const srcPath = path.join(process.cwd(), "src");
const buildPath = path.join(process.cwd(), "build");

async function build() {
  const _buildId = randomUUID().replace(/-/g, "");

  return esbuild({
    platform: "node",
    target: "node21",
    format: "esm",
    nodePaths: [srcPath],
    sourcemap: true,
    external: [
      "better-sqlite3",
      "pg",
      "mysql2",
      "mysql",
      "pg-query-stream",
      "sqlite3",
      "tedious",
      "oracledb",
    ],
    bundle: true,
    entryPoints: [path.join(srcPath, "index.ts")],
    /*banner: {
      js: `
            import { createRequire as createRequire${buildId} } from 'module';
            import { fileURLToPath as fileURLToPath${buildId} } from 'url';
            import { dirname as dirname${buildId} } from 'path';

            // using var here to allow subsequent override by authors of this
            // library that would be using the same ESM trick
            var require = createRequire${buildId}(import.meta.url);
            var __filename = fileURLToPath${buildId}(import.meta.url);
            var __dirname = dirname${buildId}(__filename);
      `,
    },*/
    outdir: buildPath,
  });
}

if (import.meta.url.startsWith("file:")) {
  if (process.argv[1] === url.fileURLToPath(import.meta.url)) {
    await build();
  }
}
