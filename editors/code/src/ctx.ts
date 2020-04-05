import * as vscode from 'vscode';
import * as lc from 'vscode-languageclient';

import { Config } from './config';
import { createClient } from './client';
import { isClarityEditor, ClarityEditor } from './util';

export class Ctx {
    private constructor(
        readonly config: Config,
        private readonly extCtx: vscode.ExtensionContext,
        readonly client: lc.LanguageClient,
        readonly serverPath: string,
    ) {

    }

    static async create(
        config: Config,
        extCtx: vscode.ExtensionContext,
        serverPath: string,
        cwd: string,
    ): Promise<Ctx> {
        const client = await createClient(config, serverPath, cwd);
        const res = new Ctx(config, extCtx, client, serverPath);
        res.pushCleanup(client.start());
        await client.onReady();
        return res;
    }

    get activeClarityEditor(): ClarityEditor | undefined {
        const editor = vscode.window.activeTextEditor;
        return editor && isClarityEditor(editor)
            ? editor
            : undefined;
    }

    get visibleClarityEditors(): ClarityEditor[] {
        return vscode.window.visibleTextEditors.filter(isClarityEditor);
    }

    registerCommand(name: string, factory: (ctx: Ctx) => Cmd) {
        const fullName = `clarity-lsp.${name}`;
        const cmd = factory(this);
        const d = vscode.commands.registerCommand(fullName, cmd);
        this.pushCleanup(d);
    }

    get globalState(): vscode.Memento {
        return this.extCtx.globalState;
    }

    get subscriptions(): Disposable[] {
        return this.extCtx.subscriptions;
    }

    pushCleanup(d: Disposable) {
        this.extCtx.subscriptions.push(d);
    }
}

export interface Disposable {
    dispose(): void;
}
export type Cmd = (...args: any[]) => unknown;
