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

        protected forkAsync<T>(
            name: string,
            f: () => T,
        ): Thread.Promise<Awaited<T>> {
            const child = this.forkSync(name);
            const promise = Worker.fork(child, async (): Promise<Awaited<T>> => await f());
            return Thread.Promise.transform(promise, child);
        }
        protected forkSync(name: string): Thread {
            const master = Worker.getThread();
            const slave = Thread.fork(name, master);
            this.onfork(slave, master);
            return slave;
        }

        public fork(name: string): Thread;
        public fork<T>(name: string, f: () => T): Thread.Promise<Awaited<T>>;
        public fork<T>(name: string, f?: () => T): Thread | Thread.Promise<Awaited<T>> {
            if (f) return this.forkAsync(name, f);
            else return this.forkSync(name);
        }

        protected spawnAsync<T>(name: string, f: () => T): Thread.Promise<Awaited<T>> {
            const child = this.spawnSync(name);
            const promise = Worker.fork(child, async (): Promise<Awaited<T>> => await f());
            return Thread.Promise.transform(promise, child);
        }
        protected spawnSync(name: string): Thread {
            const master = Thread.ROOT;
            const slave = Thread.fork(name, master);
            this.onfork(slave, master);
            return slave;
        }

        public spawn<T>(name: string, f: () => T): Thread.Promise<Awaited<T>>;
        public spawn(name: string): Thread;
        public spawn<T>(name: string, f?: () => T): Thread | Thread.Promise<Awaited<T>> {
            if (f) return this.spawnAsync(name, f);
            else return this.spawnSync(name);
        }

        protected joinAsync<T>(promise: Thread.Promise<T>): PromiseLike<T> {
            return promise.finally(() => this.joinSync(promise.thread));
        }
        protected joinSync(joinee: Thread, joiner = Worker.getThread()): void {
            this.onjoin(joinee, joiner);
        }

        /**
         * @throws may throw synchronously if `onjoin` throws.
         */
        public join<T>(promise: Thread.Promise<T>): PromiseLike<T>;
        public join(joinee: Thread, joiner?: Thread): void;
        public join<T>(promiseOrJoinee: Thread.Promise<T> | Thread, joiner?: Thread): PromiseLike<T> | void {
            if (promiseOrJoinee instanceof Thread.Promise) return this.joinAsync(promiseOrJoinee);
            else return this.joinSync(promiseOrJoinee, joiner);
        }

        /**
         * @throws may throw synchronously if `onjoin` throws.
         */
        public forkjoin<T>(name: string, f: () => T): PromiseLike<Awaited<T>> {
            return this.join(this.fork(name, f));
        }

        public async *hook<T, TReturn, TNext>(
            name: string,
            generator: AsyncGenerator<T, TReturn, TNext>,
        ): AsyncGenerator<T, TReturn, TNext> {
            let p = this.forkjoin(name, () => generator.next()), r = await p;
            for (; !r.done; r = await p) try {
                const y = yield r.value;
                p = this.forkjoin(name, () => generator.next(y));
            } catch (e) {
                p = this.forkjoin(name, () => generator.throw(e));
            }
            return r.value;
        }

    }

}
