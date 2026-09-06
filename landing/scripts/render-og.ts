import { existsSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { type Page, chromium } from "playwright-core";
import { createServer } from "vite";

const ROOT = resolve(import.meta.dirname, "..");
const PUBLIC = resolve(ROOT, "public");

type Target = {
    route: string;
    out: string;
    width: number;
    height: number;
    /** 2 = retina; final PNG is width*scale × height*scale. */
    scale: number;
    omitBackground?: boolean;
};

const TARGETS: Target[] = [
    // OpenGraph + Twitter card (1200x630, served at 2x for crisp resampling)
    { route: "/og.html", out: "og.png", width: 1200, height: 630, scale: 2 },
    // README header (1280x400, served at 2x).
    { route: "/banner.html", out: "banner.png", width: 1280, height: 400, scale: 2 },
    // Favicon at 512×512 — used as <link rel="icon"> on supported browsers.
    { route: "/favicon.html", out: "favicon-512.png", width: 512, height: 512, scale: 1 },
    // Apple touch icon at 180×180.
    { route: "/favicon.html", out: "apple-touch-icon.png", width: 180, height: 180, scale: 1 },
];

const CHROME_PATHS = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
];

function findChrome() {
    for (const p of CHROME_PATHS) if (existsSync(p)) return p;
    throw new Error("Could not find Google Chrome or Chromium. Install Chrome or set executablePath manually.");
}

async function renderOne(page: Page, baseUrl: string, t: Target) {
    await page.setViewportSize({ width: t.width, height: t.height });
    await page.goto(`${baseUrl}${t.route}`, { waitUntil: "networkidle" });
    await page.evaluate(() => document.fonts?.ready);
    // Let the shader run a few frames so the ember stabilizes.
    await page.waitForTimeout(800);
    const path = resolve(PUBLIC, t.out);
    await page.screenshot({ path, type: "png", omitBackground: !!t.omitBackground });
    console.log(`wrote ${path} (${t.width}×${t.height})`);
}

async function main() {
    await mkdir(PUBLIC, { recursive: true });

    const server = await createServer({
        root: ROOT,
        configFile: resolve(ROOT, "vite.config.ts"),
        server: { port: 0 },
    });
    await server.listen();
    const url = server.resolvedUrls?.local?.[0];
    if (!url) throw new Error("vite dev server failed to start");
    const baseUrl = url.replace(/\/$/, "");

    const browser = await chromium.launch({ executablePath: findChrome(), headless: true });
    try {
        for (const t of TARGETS) {
            const context = await browser.newContext({ deviceScaleFactor: t.scale });
            const page = await context.newPage();
            try {
                await renderOne(page, baseUrl, t);
            } finally {
                await context.close();
            }
        }
    } finally {
        await browser.close();
        await server.close();
    }
}

main().catch(err => {
    console.error(err);
    process.exit(1);
});
