import { strict as assert } from "node:assert";
import type { Inbound, OutputRecord, Route } from "../../src/contracts";
import { type GatewayFixture, HEAD, REPLACEMENT, record } from "./fixture";

const route: Route = {
  platform: "slack", accountId: "B1", authorityId: "T1", guildId: null, channelId: "D1", threadId: null, topicId: "original",
};
const otherTopic: Route = { ...route, topicId: "other" };
const success = { http: 200, ok: true } as const;

function message(messageId: string, text: string): Inbound {
  return {
    id: `slack:B1:D1:${messageId}`, messageId, route, text,
    receivedAt: 100, attachments: [],
  };
}

function barrier(frame: Record<string, unknown>) {
  const sequence = frame.sequence;
  assert.ok(typeof sequence === "number" && Number.isSafeInteger(sequence), "expected a receipt sequence");
  const event = record(frame.event);
  assert.equal(event.kind, "reconnect");
  const id = event.barrier;
  assert.ok(typeof id === "string", "expected a reconciliation barrier ID");
  assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  return { ...frame, sequence, event: { ...event, barrier: "$observed-barrier" } };
}

export async function admission(fixture: GatewayFixture) {
  const process = await fixture.start(1);
  const firstInput = message("1750000000.000001", "original");
  const offline = message("1750000000.000002", "while detached");
  const last = message("1750000000.000003", "following message");
  const foreign = await process.call("/attach", { client: REPLACEMENT, body: { pid: 1 } });
  const noToken = await process.call("/attach", { client: REPLACEMENT, body: { pid: 1 }, unauthenticated: true });
  const first = await process.attach(HEAD, true);
  const competing = await process.call("/attach", { client: REPLACEMENT, body: { pid: globalThis.process.pid } });
  assert.deepEqual(await process.call("/ready", { client: HEAD, body: {} }), success);
  const reconciliation = barrier(await first.next());
  const admitFirst = await process.beginAdmission({ kind: "message", input: firstInput });
  const held = await process.status();
  const heldRoute = await process.call("/route", { client: HEAD, body: { route } });
  const mismatched = await first.receipt(0);
  const malformed = await first.receipt(reconciliation.sequence, "unknown");
  assert.deepEqual(await first.receipt(reconciliation.sequence), success);
  assert.deepEqual(await admitFirst(), { admitted: true });
  const delivered = await first.next();
  assert.deepEqual(delivered.event, { kind: "message", input: firstInput });
  const pendingRoute = await process.call("/route", { client: HEAD, body: { route } });
  assert.deepEqual(await first.receipt(delivered.sequence), success);
  const currentRoute = await process.call("/route", { client: HEAD, body: { route } });
  const foreignRoute = await process.call("/route", { client: HEAD, body: { route: { ...route, accountId: "B2" } } });
  assert.deepEqual(await process.call("/detach", { client: HEAD, body: {} }), success);
  await first.end();

  const fresh = await process.admit({ kind: "message", input: offline });
  const duplicate = await process.admit({ kind: "message", input: { ...offline, receivedAt: 200 } });
  const conflict = await process.admit({ kind: "message", input: { ...offline, route: otherTopic } });
  assert.deepEqual(await process.admit({ kind: "message", input: last }), { admitted: true });
  const detached = await process.status();
  const unreceipted = await process.attach();
  assert.deepEqual(await process.call("/ready", { client: HEAD, body: {} }), success);
  const firstDelivery = await unreceipted.next();
  assert.deepEqual(firstDelivery.event, { kind: "message", input: offline });
  assert.deepEqual(await process.call("/detach", { client: HEAD, body: {} }), success);
  await unreceipted.end();

  const replacement = await process.attach(REPLACEMENT);
  assert.deepEqual(await process.call("/ready", { client: REPLACEMENT, body: {} }), success);
  const replay = await replacement.next();
  assert.deepEqual(replay, firstDelivery);
  const detachedReceipt = await unreceipted.receipt(replay.sequence);
  assert.deepEqual(await replacement.receipt(replay.sequence), success);
  const following = await replacement.next();
  assert.deepEqual(following.event, { kind: "message", input: last });
  assert.deepEqual(await replacement.receipt(following.sequence, "rejected"), success);
  const redeliveredAfterReceipt = await process.admit({ kind: "message", input: offline });
  const settled = await process.status();
  assert.deepEqual(await process.call("/detach", { client: REPLACEMENT, body: {} }), success);
  await replacement.end();
  await process.close();
  const activity = (await fixture.journal("activity")).sort((a, b) => String(a.messageId).localeCompare(String(b.messageId)));
  return { foreign, noToken, competing, reconciliation, held, heldRoute, mismatched, malformed, delivered,
    pendingRoute, currentRoute, foreignRoute, fresh, duplicate, conflict, detached, replay, detachedReceipt,
    following, redeliveredAfterReceipt, settled, activity };
}

export async function approval(fixture: GatewayFixture) {
  const original = await fixture.start(1);
  const first = await original.attach();
  assert.deepEqual(await original.call("/ready", { client: HEAD, body: {} }), success);
  const answer = { kind: "answer", approvalId: "native-approval:1", response: { confirmed: true }, route };
  assert.deepEqual(await original.admit(answer), { admitted: true });
  const firstDelivery = await first.next();
  assert.deepEqual(firstDelivery.event, answer);
  assert.deepEqual(await first.receipt(firstDelivery.sequence, "unresolved"), success);
  await first.end();
  const held = await original.status();
  const ordinary = message("1750000000.000004", "keep ordering");
  const stop = message("1750000000.000005", "/stop head");
  await original.admit({ kind: "message", input: ordinary });
  await original.admit({ kind: "message", input: stop });
  await original.close();

  // Same durable queue, new server, no live adapter approval cache. A closed
  // answer must not be revived by redelivery or a response for a different route.
  const restarted = await fixture.start(2);
  const sameRoute = await restarted.hasAnswer(answer.approvalId, route);
  const wrongTopic = await restarted.hasAnswer(answer.approvalId, otherTopic);
  const replacement = await restarted.attach(REPLACEMENT, true);
  assert.deepEqual(await restarted.call("/ready", { client: REPLACEMENT, body: {} }), success);
  const reconciliation = barrier(await replacement.next());
  const barrierStatus = await restarted.status();
  assert.deepEqual(await replacement.receipt(reconciliation.sequence), success);
  const priority = await replacement.next();
  assert.deepEqual(priority.event, { kind: "message", input: stop });
  const quarantined = await restarted.status();
  assert.deepEqual(await replacement.receipt(priority.sequence), success);
  const attempted = await replacement.next();
  const sequence = attempted.sequence;
  assert.deepEqual(attempted, { ...firstDelivery, sequence: firstDelivery.sequence, reconciliation: true });
  assert.deepEqual(await replacement.receipt(sequence), success);
  const following = await replacement.next();
  assert.deepEqual(following.event, { kind: "message", input: ordinary });
  assert.deepEqual(await replacement.receipt(following.sequence), success);
  const closed = await restarted.hasAnswer(answer.approvalId, route);
  const callbackRedelivery = await restarted.admit(answer);
  const callbackConflict = await restarted.admit({ ...answer, response: { confirmed: false } });
  const final = await restarted.status();
  assert.deepEqual(await restarted.call("/detach", { client: REPLACEMENT, body: {} }), success);
  await replacement.end();
  await restarted.close();
  return { firstDelivery, held, sameRoute, wrongTopic, reconciliation, barrierStatus, priority,
    quarantined, attempted, following, closed, callbackRedelivery, callbackConflict, final };
}

function output(id: string, text: string): OutputRecord {
  return { id, sourceId: "original-source", route, text, state: "sending", remoteId: null, error: null };
}
const send = (id: string, value: OutputRecord) => ({ id, operation: "send", args: [value] });

export async function effects(fixture: GatewayFixture) {
  const done = send("d8e21f03-fcb5-47ba-8b87-bcbaf08bd3de", output("first", "completed before shutdown"));
  const lost = send("e8e21f03-fcb5-47ba-8b87-bcbaf08bd3de", output("lost", "remote mutation with no receipt"));
  const newEffect = send("f8e21f03-fcb5-47ba-8b87-bcbaf08bd3de", output("third", "unrelated effect after restart"));
  const process = await fixture.start(1);
  await process.attach();
  assert.deepEqual(await process.call("/ready", { client: HEAD, body: {} }), success);
  const complete = await process.call("/effect", { client: HEAD, body: done });
  const cached = await process.call("/effect", { client: HEAD, body: done });
  const conflict = await process.call("/effect", {
    client: HEAD, body: send(done.id, { ...output("first", "changed argument") }),
  });
  const request = process.call("/effect", { client: HEAD, body: lost }).then(
    () => assert.fail("an interrupted request returned a response"),
    error => {
      assert.ok(error instanceof Error && "code" in error && error.code === "ECONNRESET",
        "the unfinished effect must lose its transport, not return an application error");
      return "connection-reset";
    },
  );
  assert.deepEqual(await process.announcement("outside effect recorded"), { remoteCommitted: "remote:lost" });
  const concurrent = await process.call("/effect", { client: HEAD, body: lost });
  await process.crash();
  const transport = await request;
  const restarted = await fixture.start(2);
  const head = await restarted.attach(REPLACEMENT);
  assert.deepEqual(await restarted.call("/ready", { client: REPLACEMENT, body: {} }), success);
  const afterRestart = await restarted.call("/effect", { client: REPLACEMENT, body: lost });
  const cachedAfterRestart = await restarted.call("/effect", { client: REPLACEMENT, body: done });
  const unrelated = await restarted.call("/effect", { client: REPLACEMENT, body: newEffect });
  assert.deepEqual(await restarted.call("/detach", { client: REPLACEMENT, body: {} }), success);
  await head.end();
  await restarted.close();
  const remote = await fixture.journal("remote");
  return { complete, cached, conflict, concurrent, transport, afterRestart, cachedAfterRestart, unrelated, remote };
}
