import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig((_configEnv) => {
  const config = {
    test: {
      exclude: [...configDefaults.exclude, "**/node_modules/**"],
      coverage: {
        provider: "istanbul" as const,
        include: ["src/**/*.ts"],
        exclude: ["**/__tests__/**"],
      },
      env: {
        TZ: "UTC",
      },
      testTimeout: 30000,
      hookTimeout: 60000,
    },
  };

  return config;
});
