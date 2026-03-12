import { defineConfig } from "vitest/config";

export default defineConfig({
    test: {
        coverage: {
            provider: "v8",
            exclude: [
                "**/*.ttl", // exclude all Turtle files
            ],
        },
    },
    ssr: {
        // Needed to fix mocking of 'node:fs' in tests
        noExternal: ["@rdfc/js-runner"],
    },
});
