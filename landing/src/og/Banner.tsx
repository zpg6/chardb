import { CoalShader } from "../components/CoalShader";

/** README header. Rendered to public/banner.png by scripts/render-og.ts. */
export function Banner() {
    return (
        <div className="relative bg-ink-950 text-fg overflow-hidden" style={{ width: 1280, height: 400 }}>
            <div className="absolute inset-0 flex items-center justify-between px-20">
                <div className="relative z-10">
                    <div className="flex items-center gap-3 font-mono text-[22px] text-fg-muted lowercase">
                        <span className="text-fg">chardb</span>
                        <span className="text-fg-dim">·</span>
                        <span>chardb.dev</span>
                    </div>

                    <h1
                        className="mt-6 font-sans font-semibold tracking-tight text-fg"
                        style={{ fontSize: 56, lineHeight: 1.08, letterSpacing: "-0.02em", maxWidth: 760 }}
                    >
                        An auth-native database for Cloudflare Workers.
                    </h1>

                    <div className="mt-7 font-mono text-[17px] text-fg-dim lowercase flex items-center gap-3">
                        <span>docs.chardb.dev</span>
                        <span className="text-fg-dim/60">·</span>
                        <span>github.com/zpg6/chardb</span>
                    </div>
                </div>

                <div style={{ width: 320, height: 320 }}>
                    <CoalShader />
                </div>
            </div>

            <div
                aria-hidden="true"
                className="absolute inset-0 pointer-events-none"
                style={{
                    boxShadow: "inset 0 0 0 1px rgba(255,255,255,0.04), inset 0 -1px 0 rgba(236,87,19,0.25)",
                }}
            />
        </div>
    );
}
