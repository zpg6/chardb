import { CloudflareWorkersMark } from "../components/CloudflareWorkersMark";
import { CoalShader } from "../components/CoalShader";

/** README header. Rendered to public/banner.png by scripts/render-og.ts. */
export function Banner() {
    return (
        <div className="relative bg-ink-950 text-fg overflow-hidden" style={{ width: 1280, height: 400 }}>
            <div className="absolute" style={{ width: 640, height: 640, left: 720, top: -60 }}>
                <CoalShader />
            </div>
            <div
                className="absolute"
                style={{
                    left: 64,
                    top: 0,
                    bottom: 0,
                    display: "flex",
                    flexDirection: "column",
                    justifyContent: "center",
                }}
            >
                <span className="mb-7 font-mono lowercase text-fg" style={{ fontSize: 28, letterSpacing: "-0.02em" }}>
                    chardb
                </span>
                <h1
                    className="font-sans font-semibold tracking-tight text-fg"
                    style={{ fontSize: 60, lineHeight: 1.05, letterSpacing: "-0.02em" }}
                >
                    A real database inside your
                    <span className="worker-lockup">
                        <span className="worker-mark">
                            <CloudflareWorkersMark />
                        </span>
                        <span>
                            <span className="worker-cloudflare">Cloudflare</span> Worker.
                        </span>
                    </span>
                </h1>
                <span className="mt-6 inline-flex w-fit rounded-full border border-accent/35 bg-accent/10 px-3 py-1 font-mono text-[13px] uppercase tracking-[0.14em] text-accent">
                    Experimental
                </span>
            </div>
        </div>
    );
}
