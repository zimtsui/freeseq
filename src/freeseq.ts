import { Thread } from './thread.ts';
import { Worker } from './worker.ts';



export type FreeSeq = FreeSeq.Instance;
export namespace FreeSeq {
    export function create(
        onfork: (slave: Thread, master: Thread) => void,
        onjoin: (joinee: Thread, joiner: Thread) => void,
    ): Instance {
        return new Instance(onfork, onjoin);
    }

    export class Instance {
        public constructor(
            protected onfork: (slave: Thread, master: Thread) => void,
            protected onjoin: (joinee: Thread, joiner: Thread) => void,
        ) {}

        public fork<T>(
            name: string,
            f: () => T,
        ): Thread.Promise<Awaited<T>> {
            const child = this.forkSync(name);
            const promise = Worker.fork(child, async (): Promise<Awaited<T>> => await f());
            return Thread.Promise.transform(promise, child);
        }

        public forkSync(name: string): Thread {
            const master = Worker.getThread();
            const slave = Thread.fork(name, master);
            this.onfork(slave, master);
            return slave;
        }

        public spawn<T>(
            name: string,
            f: () => T,
        ): Thread.Promise<Awaited<T>> {
            const child = this.spawnSync(name);
            const promise = Worker.fork(child, async (): Promise<Awaited<T>> => await f());
            return Thread.Promise.transform(promise, child);
        }

        public spawnSync(name: string): Thread {
            const master = Thread.ROOT;
            const slave = Thread.fork(name, master);
            this.onfork(slave, master);
            return slave;
        }

        /**
         * @throws may throw synchronously if `onjoin` throws.
         */
        public join<T>(
            promise: Thread.Promise<T>,
        ): PromiseLike<T> {
            return promise.finally(() => this.joinSync(promise.thread));
        }

        public joinSync(joinee: Thread): void {
            const joiner = Worker.getThread();
            this.onjoin(joinee, joiner);
        }
        public joinBySelfSync(joinee: Thread): void {
            this.onjoin(joinee, joinee);
        }
        public joinByMasterSync(slave: Thread): void {
            const master = Thread.masters.get(slave);
            if (master)
                this.onjoin(slave, master);
            else
                return this.joinBySelfSync(slave);
        }
    }

}
