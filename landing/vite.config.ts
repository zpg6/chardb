import { resolve } from "node:path";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
    plugins: [react()],
    build: {
        rollupOptions: {
            input: {
                main: resolve(import.meta.dirname, "index.html"),
                why: resolve(import.meta.dirname, "why/index.html"),
                og: resolve(import.meta.dirname, "og.html"),
                banner: resolve(import.meta.dirname, "banner.html"),
                favicon: resolve(import.meta.dirname, "favicon.html"),
            },
        },
    },
});
