import { describe, expect, it } from "vitest";
import { APP_LOCALES } from "./locales";
import { appCatalogs, createAppI18n } from "./instance";

describe("visibility preview and folded-worker localization", () => {
	it.each(APP_LOCALES)("provides complete visibility copy and interpolation in %s", (locale) => {
		const keys = Object.keys(appCatalogs.en).filter((key) => key.startsWith("bulkVisibility.") || [
			"shell.hiddenSessionNotice", "shell.collapseOlderWorkers", "shell.expandOlderWorkers",
			"shell.hideOlderWorkers", "shell.showOlderWorkers",
		].includes(key));
		expect(keys).toHaveLength(24);
		for (const key of keys) {
			const message = appCatalogs[locale][key];
			expect(typeof message).toBe("string");
			expect(message).not.toBe("");
			expect(String(message).match(/\{\{\w+\}\}/g)?.sort() ?? []).toEqual(
				String(appCatalogs.en[key]).match(/\{\{\w+\}\}/g)?.sort() ?? [],
			);
		}
		const instance = createAppI18n(locale);
		const summary = instance.t("bulkVisibility.summary", {
			action: instance.t("bulkVisibility.hideAction"), selected: 371, eligible: 240, skipped: 131, chunks: 2, maximum: 200,
		});
		for (const count of [371, 240, 131, 2, 200]) expect(summary).toContain(String(count));
		expect(summary).not.toContain("{{");
		for (const count of [0, 1, 2]) {
			for (const key of ["shell.collapseOlderWorkers", "shell.expandOlderWorkers"] as const) {
				const label = instance.t(key, { count, workspace: "Project <A>" });
				expect(label).toContain(String(count));
				expect(label).toContain("Project <A>");
				expect(label).not.toContain("{{");
			}
		}
		expect(instance.t("shell.hiddenSessionNotice", { title: "Worker <B>" })).toContain("Worker <B>");
		expect(instance.t("bulkVisibility.failed", { error: "Denied" })).toContain("Denied");
	});

	it("preserves English status and accessible names used by the existing UI tests", () => {
		const instance = createAppI18n("en");
		expect(instance.t("bulkVisibility.summary", { action: "hide", selected: 1, eligible: 1, skipped: 0, chunks: 1, maximum: 200 }))
			.toBe("hide: 1 selected, 1 eligible, 0 skipped; 1 chunks (maximum 200 each).");
		expect(instance.t("shell.expandOlderWorkers", { count: 2, workspace: "Project" })).toBe("Expand 2 older workers in Project");
		expect(instance.t("shell.hideOlderWorkers", { count: 2 })).toBe("Hide 2 older workers");
		expect(instance.t("shell.hiddenSessionNotice", { title: "Worker" })).toBe("Hidden session: Worker. Show hidden sessions to unhide.");
	});
});
