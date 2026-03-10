import { Thread } from './thread.ts';
import { AsyncLocalStorage } from 'node:async_hooks';



export interface Worker {
    thread: Thread;
}
export namespace Worker {
    export const ROOT: Worker = { thread: Thread.ROOT };
    const als = new AsyncLocalStorage<Worker>();

    function getWorker(): Worker {
        return als.getStore() ?? Worker.ROOT;
    }

    export function getThread(): Thread {
        return getWorker().thread;
    }

    export function exec(thread: Thread): void {
        getWorker().thread = thread;
    }

    export function fork<T>(
        thread: Thread,
        f: () => T,
    ): T {
        return als.run({ thread }, f);
    }

}
