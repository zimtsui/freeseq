import test from 'ava';

import { FreeSeq } from './freeseq.ts';
import { Thread } from './thread.ts';
import { Worker } from './worker.ts';

test.serial('Thread.fork records the master thread', (t) => {
    const master = Thread.fork('master', Thread.ROOT);
    const slave = Thread.fork('slave', master);

    t.is(Thread.master(slave), master);
    t.is(slave.name, 'slave');
    t.true(slave.id > master.id);
    t.throws(() => Thread.master(Thread.ROOT), {
        message: 'The thread ROOT has no master.',
    });
});

test.serial('Worker.fork and Worker.exec keep thread state inside async context', async (t) => {
    const parent = Thread.fork('parent', Thread.ROOT);
    const child = Thread.fork('child', parent);

    t.is(Worker.getThread(), Thread.ROOT);

    await Worker.fork(parent, async () => {
        t.is(Worker.getThread(), parent);

        await Promise.resolve();
        t.is(Worker.getThread(), parent);

        Worker.exec(child);
        t.is(Worker.getThread(), child);

        await Promise.resolve();
        t.is(Worker.getThread(), child);
    });

    t.is(Worker.getThread(), Thread.ROOT);
});

test.serial('FreeSeq.fork runs work on a child thread and joins back to the caller thread', async (t) => {
    const events: string[] = [];
    const freeseq = FreeSeq.create(
        (slave, master) => events.push(`fork:${master.name}->${slave.name}`),
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const parent = Thread.fork('parent', Thread.ROOT);

    await Worker.fork(parent, async () => {
        const promise = freeseq.fork('child', async () => {
            t.is(Worker.getThread().name, 'child');
            await Promise.resolve();
            t.is(Worker.getThread().name, 'child');
            return Worker.getThread().name;
        });

        t.is(Thread.master(promise.thread), parent);
        t.is(await freeseq.join(promise), 'child');
        t.is(Worker.getThread(), parent);
    });

    t.deepEqual(events, [
        'fork:parent->child',
        'join:child->parent',
    ]);
});

test.serial('FreeSeq.spawn always uses the root thread as master', async (t) => {
    const events: string[] = [];
    const freeseq = FreeSeq.create(
        (slave, master) => events.push(`fork:${master.name}->${slave.name}`),
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const parent = Thread.fork('parent', Thread.ROOT);

    await Worker.fork(parent, async () => {
        const promise = freeseq.spawn('detached', async () => Worker.getThread().name);

        t.is(Thread.master(promise.thread), Thread.ROOT);
        t.is(await freeseq.join(promise), 'detached');
    });

    t.deepEqual(events, [
        'fork:root->detached',
        'join:detached->parent',
    ]);
});

test.serial('FreeSeq sync overloads return threads and emit join events directly', (t) => {
    const events: string[] = [];
    const freeseq = FreeSeq.create(
        (slave, master) => events.push(`fork:${master.name}->${slave.name}`),
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const parent = Thread.fork('parent', Thread.ROOT);

    Worker.fork(parent, () => {
        const forked = freeseq.fork('child');
        const spawned = freeseq.spawn('detached');

        t.is(Thread.master(forked), parent);
        t.is(Thread.master(spawned), Thread.ROOT);

        freeseq.join(forked);
        freeseq.join(spawned);
    });

    t.deepEqual(events, [
        'fork:parent->child',
        'fork:root->detached',
        'join:child->parent',
        'join:detached->parent',
    ]);
});

test.serial('FreeSeq.join joins even when the task rejects', async (t) => {
    const events: string[] = [];
    const freeseq = FreeSeq.create(
        () => {},
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const parent = Thread.fork('parent', Thread.ROOT);

    await Worker.fork(parent, async () => {
        const promise = freeseq.fork('child', async () => {
            throw new Error('boom');
        });

        await t.throwsAsync(async () => await freeseq.join(promise), {
            message: 'boom',
        });
    });

    t.deepEqual(events, ['join:child->parent']);
});

test.serial('FreeSeq.forkjoin runs on a child thread and joins back automatically', async (t) => {
    const events: string[] = [];
    const freeseq = FreeSeq.create(
        (slave, master) => events.push(`fork:${master.name}->${slave.name}`),
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const parent = Thread.fork('parent', Thread.ROOT);

    await Worker.fork(parent, async () => {
        const result = await freeseq.forkjoin('child', async () => {
            t.is(Worker.getThread().name, 'child');
            await Promise.resolve();
            t.is(Worker.getThread().name, 'child');
            return 'done';
        });

        t.is(result, 'done');
        t.is(Worker.getThread(), parent);
    });

    t.deepEqual(events, [
        'fork:parent->child',
        'join:child->parent',
    ]);
});

test.serial('FreeSeq.forkjoin still joins when the task rejects', async (t) => {
    const events: string[] = [];
    const freeseq = FreeSeq.create(
        (slave, master) => events.push(`fork:${master.name}->${slave.name}`),
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const parent = Thread.fork('parent', Thread.ROOT);

    await Worker.fork(parent, async () => {
        await t.throwsAsync(async () => await freeseq.forkjoin('child', async () => {
            throw new Error('boom');
        }), {
            message: 'boom',
        });

        t.is(Worker.getThread(), parent);
    });

    t.deepEqual(events, [
        'fork:parent->child',
        'join:child->parent',
    ]);
});

test.serial('FreeSeq.join accepts an explicit joiner thread for sync joins', (t) => {
    const events: string[] = [];
    const freeseq = FreeSeq.create(
        () => {},
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const master = Thread.fork('master', Thread.ROOT);
    const slave = Thread.fork('slave', master);
    const orphan = {
        name: 'orphan',
        id: ++Thread.count,
    };
    const joiner = Thread.fork('joiner', Thread.ROOT);

    freeseq.join(slave);
    freeseq.join(slave, master);
    freeseq.join(orphan, joiner);

    t.deepEqual(events, [
        'join:slave->root',
        'join:slave->master',
        'join:orphan->joiner',
    ]);
});

test.serial('FreeSeq.hook drives an async generator on child threads and returns to the caller thread', async (t) => {
    const events: string[] = [];
    const generatorEvents: string[] = [];
    const freeseq = FreeSeq.create(
        (slave, master) => events.push(`fork:${master.name}->${slave.name}`),
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const parent = Thread.fork('parent', Thread.ROOT);

    await Worker.fork(parent, async () => {
        async function *source(): AsyncGenerator<string, string, string> {
            t.is(Worker.getThread().name, 'child');
            generatorEvents.push('start:child');

            const first = yield 'step-1';

            await Promise.resolve();
            t.is(Worker.getThread().name, 'child');
            generatorEvents.push(`first:${first}`);

            const second = yield `step-2:${first}`;

            await Promise.resolve();
            t.is(Worker.getThread().name, 'child');
            generatorEvents.push(`second:${second}`);

            return `${first}:${second}`;
        }

        const hooked = freeseq.hook('child', source());

        t.deepEqual(await hooked.next(), { value: 'step-1', done: false });
        t.is(Worker.getThread(), parent);

        t.deepEqual(await hooked.next('alpha'), { value: 'step-2:alpha', done: false });
        t.is(Worker.getThread(), parent);

        t.deepEqual(await hooked.next('beta'), { value: 'alpha:beta', done: true });
        t.is(Worker.getThread(), parent);
    });

    t.deepEqual(generatorEvents, [
        'start:child',
        'first:alpha',
        'second:beta',
    ]);
    t.deepEqual(events, [
        'fork:parent->child',
        'join:child->parent',
        'fork:parent->child',
        'join:child->parent',
        'fork:parent->child',
        'join:child->parent',
    ]);
});

test.serial('FreeSeq.hook forwards throw calls into the async generator and still joins correctly', async (t) => {
    const events: string[] = [];
    const generatorEvents: string[] = [];
    const freeseq = FreeSeq.create(
        (slave, master) => events.push(`fork:${master.name}->${slave.name}`),
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const parent = Thread.fork('parent', Thread.ROOT);
    const boom = new Error('boom');

    await Worker.fork(parent, async () => {
        async function *source(): AsyncGenerator<string, string, string> {
            try {
                t.is(Worker.getThread().name, 'child');
                generatorEvents.push('start:child');
                yield 'step-1';
                t.fail();
            } catch (error) {
                t.is(error, boom);
                await Promise.resolve();
                t.is(Worker.getThread().name, 'child');
                generatorEvents.push(`caught:${(error as Error).message}`);
                yield 'recovered';
            }

            await Promise.resolve();
            t.is(Worker.getThread().name, 'child');
            generatorEvents.push('finish:child');
            return 'done';
        }

        const hooked = freeseq.hook('child', source());

        t.deepEqual(await hooked.next(), { value: 'step-1', done: false });
        t.is(Worker.getThread(), parent);

        t.deepEqual(await hooked.throw(boom), { value: 'recovered', done: false });
        t.is(Worker.getThread(), parent);

        t.deepEqual(await hooked.next(), { value: 'done', done: true });
        t.is(Worker.getThread(), parent);
    });

    t.deepEqual(generatorEvents, [
        'start:child',
        'caught:boom',
        'finish:child',
    ]);
    t.deepEqual(events, [
        'fork:parent->child',
        'join:child->parent',
        'fork:parent->child',
        'join:child->parent',
        'fork:parent->child',
        'join:child->parent',
    ]);
});

test.serial('FreeSeq.hook forwards return calls without auto-disposing the wrapped async generator', async (t) => {
    const events: string[] = [];
    const generatorEvents: string[] = [];
    const freeseq = FreeSeq.create(
        (slave, master) => events.push(`fork:${master.name}->${slave.name}`),
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const parent = Thread.fork('parent', Thread.ROOT);
    let disposed = false;

    const generator: AsyncGenerator<string, string, string> = {
        async next(value?: string) {
            generatorEvents.push(`next:${value ?? ''}`);
            return { value: 'step-1', done: false };
        },
        async return(value: string | PromiseLike<string>) {
            const resolved = await value;
            generatorEvents.push(`return:${resolved}`);
            return { value: resolved, done: true };
        },
        async throw(error?: unknown) {
            throw error;
        },
        async [Symbol.asyncDispose]() {
            disposed = true;
            generatorEvents.push(`dispose:${Worker.getThread().name}`);
        },
        [Symbol.asyncIterator]() {
            return this;
        },
    };

    await Worker.fork(parent, async () => {
        const hooked = freeseq.hook('child', generator);

        t.deepEqual(await hooked.next(), { value: 'step-1', done: false });
        t.is(Worker.getThread(), parent);

        t.deepEqual(await hooked.return('stop'), { value: 'stop', done: true });
        t.is(Worker.getThread(), parent);
    });

    t.false(disposed);
    t.deepEqual(generatorEvents, [
        'next:',
        'return:stop',
    ]);
    t.deepEqual(events, [
        'fork:parent->child',
        'join:child->parent',
        'fork:parent->child',
        'join:child->parent',
    ]);
});

test.serial('FreeSeq.hook forwards async disposal to the wrapped async generator', async (t) => {
    const events: string[] = [];
    const generatorEvents: string[] = [];
    const freeseq = FreeSeq.create(
        (slave, master) => events.push(`fork:${master.name}->${slave.name}`),
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const parent = Thread.fork('parent', Thread.ROOT);
    let disposed = false;

    const generator: AsyncGenerator<string, string, string> = {
        async next(value?: string) {
            generatorEvents.push(`next:${value ?? ''}`);
            return { value: 'step-1', done: false };
        },
        async return(value: string | PromiseLike<string>) {
            const resolved = await value;
            generatorEvents.push(`return:${resolved}`);
            return { value: resolved, done: true };
        },
        async throw(error?: unknown) {
            throw error;
        },
        async [Symbol.asyncDispose]() {
            disposed = true;
            generatorEvents.push(`dispose:${Worker.getThread().name}`);
        },
        [Symbol.asyncIterator]() {
            return this;
        },
    };

    await Worker.fork(parent, async () => {
        const hooked = freeseq.hook('child', generator);

        t.deepEqual(await hooked.next(), { value: 'step-1', done: false });
        t.is(Worker.getThread(), parent);

        await hooked[Symbol.asyncDispose]();
        t.is(Worker.getThread(), parent);
    });

    t.true(disposed);
    t.deepEqual(generatorEvents, [
        'next:',
        'dispose:parent',
    ]);
    t.deepEqual(events, [
        'fork:parent->child',
        'join:child->parent',
    ]);
});

test.serial('FreeSeq.hook feeds asynchronous wrapped generator rejections back through generator.throw and can continue', async (t) => {
    const events: string[] = [];
    const generatorEvents: string[] = [];
    const freeseq = FreeSeq.create(
        (slave, master) => events.push(`fork:${master.name}->${slave.name}`),
        (joinee, joiner) => events.push(`join:${joinee.name}->${joiner.name}`),
    );
    const parent = Thread.fork('parent', Thread.ROOT);
    const boom = new Error('boom');

    await Worker.fork(parent, async () => {
        async function *source(): AsyncGenerator<string, string, string> {
            try {
                t.is(Worker.getThread().name, 'child');
                generatorEvents.push('start:child');
                const first = yield 'step-1';

                await Promise.resolve();
                t.is(Worker.getThread().name, 'child');
                generatorEvents.push(`first:${first}`);
                await Promise.reject(boom);
            } catch (error) {
                t.is(error, boom);
                await Promise.resolve();
                t.is(Worker.getThread().name, 'child');
                generatorEvents.push(`caught:${(error as Error).message}`);

                const resumed = yield 'recovered';

                await Promise.resolve();
                t.is(Worker.getThread().name, 'child');
                generatorEvents.push(`resume:${resumed}`);
                return `done:${resumed}`;
            }

            return 'unreachable';
        }

        const hooked = freeseq.hook('child', source());

        t.deepEqual(await hooked.next(), { value: 'step-1', done: false });
        t.is(Worker.getThread(), parent);

        t.deepEqual(await hooked.next('alpha'), { value: 'recovered', done: false });
        t.is(Worker.getThread(), parent);

        t.deepEqual(await hooked.next('resume'), { value: 'done:resume', done: true });
        t.is(Worker.getThread(), parent);
    });

    t.deepEqual(generatorEvents, [
        'start:child',
        'first:alpha',
        'caught:boom',
        'resume:resume',
    ]);
    t.deepEqual(events, [
        'fork:parent->child',
        'join:child->parent',
        'fork:parent->child',
        'join:child->parent',
        'fork:parent->child',
        'join:child->parent',
    ]);
});
