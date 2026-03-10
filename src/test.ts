import test from 'ava';

import { FreeSeq } from './freeseq.ts';
import { Thread } from './thread.ts';
import { Worker } from './worker.ts';

test.serial('Thread.fork records the master thread', (t) => {
    const master = Thread.fork('master', Thread.ROOT);
    const slave = Thread.fork('slave', master);

    t.is(Thread.masters.get(slave), master);
    t.is(slave.name, 'slave');
    t.true(slave.id > master.id);
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

        t.is(Thread.masters.get(promise.thread), parent);
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

        t.is(Thread.masters.get(promise.thread), Thread.ROOT);
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

        t.is(Thread.masters.get(forked), parent);
        t.is(Thread.masters.get(spawned), Thread.ROOT);

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

test.serial('FreeSeq selfjoin and masterjoin use the expected joiner thread', (t) => {
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

    freeseq.selfjoin(slave);
    freeseq.masterjoin(slave);
    freeseq.masterjoin(orphan);

    t.deepEqual(events, [
        'join:slave->slave',
        'join:slave->master',
        'join:orphan->orphan',
    ]);
});
