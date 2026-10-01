export declare const PREVIEW_SCHEMA_VERSION: 2;
export declare const PREVIEW_UPGRADE_SCHEMA_VERSION: 3;
export declare function parsePreviewUpgradeArgs(argv: readonly string[]): {
    readonly help: boolean;
    readonly input?: string;
    readonly output?: string;
};
export declare function renderVersionTwoSchema(source: string): string;
export declare function renderVersionTwoMigrations(source: string): string;
