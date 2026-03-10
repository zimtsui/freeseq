

export interface Thread {
    name: string;
    id: number;
}


export namespace Thread {
    export let count = 0;
    export const ROOT: Thread = {
        name: 'root',
        id: ++Thread.count,
    };

    export const masters = new WeakMap<Thread, Thread>();

    export class Promise<T> extends globalThis.Promise<T> {
        protected constructor(
            executor: ConstructorParameters<typeof globalThis.Promise<T>>[0],
            public thread: Thread,
        ) {
            super(executor);
        }

        public static transform<T>(promise: globalThis.Promise<T>, thread: Thread): Promise<T> {
            return new Promise<T>((resolve, reject) => promise.then(resolve, reject), thread);
        }
    }

    export function fork(name: string, master: Thread): Thread {
        const slave: Thread = {
            name,
            id: ++Thread.count,
        };
        Thread.masters.set(slave, master);
        return slave;
    }
}
