import * as path from 'path';
import { workspace, ExtensionContext } from 'vscode';
import { Config, NIGHTLY_TAG } from './config';
import { log, assert } from './util';
import { PersistentState } from './persistent_state';
import { fetchRelease, download } from './net';
import { promises as fs } from "fs";
import * as vscode from 'vscode';
import { spawnSync } from 'child_process';
import * as os from "os";

import {
	LanguageClient,
	LanguageClientOptions,
	Executable,
} from 'vscode-languageclient';

let client: LanguageClient;

export async function activate(context: ExtensionContext) {

	const config = new Config(context);
	const state = new PersistentState(context.globalState);

	const serverPath = await bootstrap(config, state);

	const serverOptions: Executable = {
		command: serverPath,
		args: ["lsp"]
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
	const path = await bootstrapServer();

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

async function bootstrapServer(): Promise<string> {
	const path = await getServer();
	if (!path) {
		throw new Error(
			"Clarinet is not available.\n" +
			"Please ensure it is correctly [installed](https://github.com/hirosystems/clarinet)"
		);
	}

	const res = spawnSync(path, ["--version"], { encoding: 'utf8' });
	log.error("Checked binary availability via --version", res);
	log.debug(res, "--version output:", res.output);
	// Yikes: `$ clarinet --version` returns
	// clarinet 0.21.0
	//
	// The LSP was merged in Clarinet in v0.21.0, we want to make sure that 
	// we're using an adequate version.
	const version = res.output
		.toString()    // clarinet 0.21.0
		.split("\n")[0] // 0.21.0
		.split(" ")[1] // 0.21.0
		.split(".");   // ["0", "21", "0"]
	if (parseInt(version[0]) === 0 && parseInt(version[1]) < 22) {
		throw new Error(
			"Clarinet is outdated.\n" +
			"Please update to [v0.22.0 or newer](https://github.com/hirosystems/clarinet)"
		);
	}

	return path;
}

function getServer(): Promise<string> {
	switch (os.platform()) {
		case "win32":
			return getClarinetWindowsPath();
		default:
			return Promise.resolve("clarinet");
	}

	async function getClarinetWindowsPath() {
		// Adapted from https://github.com/npm/node-which/blob/master/which.js
		const clarinetCmd = "clarinet";
		const pathExtValue = process.env.PATHEXT ?? ".EXE;.CMD;.BAT;.COM";
		// deno-lint-ignore no-undef
		const pathValue = process.env.PATH ?? "";
		const pathExtItems = splitEnvValue(pathExtValue);
		const pathFolderPaths = splitEnvValue(pathValue);

		for (const pathFolderPath of pathFolderPaths) {
			for (const pathExtItem of pathExtItems) {
				const cmdFilePath = path.join(pathFolderPath, clarinetCmd + pathExtItem);
				if (await fileExists(cmdFilePath)) {
					return cmdFilePath;
				}
			}
		}

		// nothing found; return back command
		return clarinetCmd;

		function splitEnvValue(value: string) {
			return value
				.split(";")
				.map((item) => item.trim())
				.filter((item) => item.length > 0);
		}
	}

	function fileExists(executableFilePath: string): Promise<boolean> {
		return new Promise<boolean>(async (resolve) => {
			try {
				const stat = await fs.stat(executableFilePath);
				return resolve(stat.isFile());
			} catch (error) {
				return resolve(false);
			}
		}).catch(() => {
			// ignore all errors
			return false;
		});
	}
}