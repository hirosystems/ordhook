import * as path from 'path';
import { workspace, ExtensionContext } from 'vscode';
import { Config, NIGHTLY_TAG } from './config';
import { log, assert } from './util';
import { PersistentState } from './persistent_state';
import { fetchRelease, download } from './net';
import { promises as fs } from "fs";
import * as vscode from 'vscode';
// import { spawnSync } from 'child_process';
import * as os from "os";

import {
	LanguageClient,
	LanguageClientOptions,
	Executable,
} from 'vscode-languageclient/node';

let client: LanguageClient;

export async function activate(context: ExtensionContext) {

	const config = new Config(context);
	const state = new PersistentState(context.globalState);

	const serverPath = await bootstrap(config, state);

	const serverOptions: Executable = {
		command: serverPath
	};

	// Options to control the language client
	const clientOptions: LanguageClientOptions = {
		// Register the server for plain text documents
		documentSelector: [{ scheme: 'file', language: 'clarity' }],
		synchronize: {
			// Notify the server about file changes to '.clientrc files contained in the workspace
			fileEvents: workspace.createFileSystemWatcher('**/.clientrc')
		}
	};

	// Create the language client and start the client.
	client = new LanguageClient(
		'clarityLanguageServer',
		'Clarity Language Server',
		serverOptions,
		clientOptions
	);

	// Start the client. This will also launch the server
	client.start();
}

export async function deactivate() {
	if (!client) {
		return;
	}
	await client.stop();
}

async function bootstrap(config: Config, state: PersistentState): Promise<string> {
	await fs.mkdir(config.globalStoragePath, { recursive: true });

	await bootstrapExtension(config, state);
	const path = await bootstrapServer(config, state);

	return path;
}

async function bootstrapExtension(config: Config, state: PersistentState): Promise<void> {
	if (config.package.releaseTag === null) return;
	if (config.channel === "stable") {
		if (config.package.releaseTag === NIGHTLY_TAG) {
			void vscode.window.showWarningMessage(
				`You are running a nightly version of clarity-lsp extension. ` +
				`To switch to stable, uninstall the extension and re-install it from the marketplace`
			);
		}
		return;
	};

	const lastCheck = state.lastCheck;
	const now = Date.now();

	const anHour = 60 * 60 * 1000;
	const shouldDownloadNightly = state.releaseId === undefined || (now - (lastCheck ?? 0)) > anHour;

	if (!shouldDownloadNightly) return;

	const release = await fetchRelease("nightly").catch((e) => {
		log.error(e);
		if (state.releaseId === undefined) { // Show error only for the initial download
			vscode.window.showErrorMessage(`Failed to download clarity-lsp nightly ${e}`);
		}
		return undefined;
	});
	if (release === undefined || release.id === state.releaseId) return;

	const userResponse = await vscode.window.showInformationMessage(
		"New version of clarity-lsp (nightly) is available (requires reload).",
		"Update"
	);
	if (userResponse !== "Update") return;

	const artifact = release.assets.find(artifact => artifact.name === "clarity-lsp.vsix");
	assert(!!artifact, `Bad release: ${JSON.stringify(release)}`);

	const dest = path.join(config.globalStoragePath, "clarity-lsp.vsix");
	await download(artifact.browser_download_url, dest, "Downloading clarity-lsp extension");

	await vscode.commands.executeCommand("workbench.extensions.installExtension", vscode.Uri.file(dest));
	await fs.unlink(dest);

	await state.updateReleaseId(release.id);
	await state.updateLastCheck(now);
	await vscode.commands.executeCommand("workbench.action.reloadWindow");
}

async function bootstrapServer(config: Config, state: PersistentState): Promise<string> {
	const path = await getServer(config, state);
	if (!path) {
		throw new Error(
			"Clarity Language Server is not available. " +
			"Please, ensure its [proper installation](https://github.com/hirosystems/clarity-lsp)."
		);
	}

	// log.debug("Using server binary at", path);

	// const res = spawnSync(path, ["--version"], { encoding: 'utf8' });
	// log.debug("Checked binary availability via --version", res);
	// log.debug(res, "--version output:", res.output);
	// if (res.status !== 0) {
	//     throw new Error(`Failed to execute ${path} --version`);
	// }

	return path;
}

async function getServer(config: Config, state: PersistentState): Promise<string | undefined> {
	const explicitPath = process.env.__RA_LSP_SERVER_DEBUG ?? config.serverPath;
	if (explicitPath) {
		if (explicitPath.startsWith("~/")) {
			return os.homedir() + explicitPath.slice("~".length);
		}
		return explicitPath;
	};
	if (config.package.releaseTag === null) return "clarity-lsp";

	let binaryName: string | undefined = undefined;
	if (process.arch === "x64" || process.arch === "ia32") {
		if (process.platform === "linux") binaryName = "clarity-lsp-linux";
		if (process.platform === "darwin") binaryName = "clarity-lsp-mac";
		if (process.platform === "win32") binaryName = "clarity-lsp-windows.exe";
	}
	if (binaryName === undefined) {
		vscode.window.showErrorMessage(
			"Unfortunately we don't ship binaries for your platform yet. " +
			"You need to manually clone clarity-lsp repository and " +
			"run `cargo xtask install --server` to build the language server from sources. " +
			"If you feel that your platform should be supported, please create an issue " +
			"about that [here](https://github.com/hirosystems/clarity-lsp/issues) and we " +
			"will consider it."
		);
		return undefined;
	}

	const dest = path.join(config.globalStoragePath, binaryName);
	const exists = await fs.stat(dest).then(() => true, () => false);
	if (!exists) {
		await state.updateServerVersion(undefined);
	}

	if (state.serverVersion === config.package.version) return dest;

	if (config.askBeforeDownload) {
		const userResponse = await vscode.window.showInformationMessage(
			`Language server version ${config.package.version} for clarity-lsp is not installed.`,
			"Download now"
		);
		if (userResponse !== "Download now") return dest;
	}

	const release = await fetchRelease(config.package.releaseTag);
	const artifact = release.assets.find(artifact => artifact.name === binaryName);
	assert(!!artifact, `Bad release: ${JSON.stringify(release)}`);

	await download(artifact.browser_download_url, dest, "Downloading clarity-lsp server", { mode: 0o755 });
	await state.updateServerVersion(config.package.version);
	return dest;
}
