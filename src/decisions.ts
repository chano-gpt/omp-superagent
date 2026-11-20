import { createHash } from "node:crypto";
import type { Approval, ApprovalResolution, DialogResponse, StorePort } from "./contracts";
import { parseDialog, type Question } from "./dialog";
import { DeliveryError } from "./delivery";

interface Receipt {
  approvalId: string;
  createdAt: number;
  response?: DialogResponse;
  confirmedAt?: number;
  withdrawal?: { at: number; reason: string; sourceId?: string };
  headSelection?: ApprovalResolution["headSelection"];
}

export interface DecisionView {
  id: string;
  state: "pending" | "answering" | "answered" | "withdrawn";
  sourceId: string | null;
  target: string;
  sessionId: string;
  generation: number;
  approvalId: string;
  requestId: string;
  route: Approval["route"];
  remoteId: string | null;
  questions: Question[];
  createdAt: number;
  response?: DialogResponse;
  confirmedAt?: number;
  withdrawal?: Receipt["withdrawal"];
  headSelection?: Receipt["headSelection"];
}

export function decisionId(sessionId: string, toolCallId: string): string {
  return `d-${createHash("sha256").update(JSON.stringify([sessionId, toolCallId])).digest("hex").slice(0, 24)}`;
}

/** Only the managed decision tool's structured questions enter this ledger. */
export function decisionRequestId(request: Record<string, unknown>): string | null {
  if (request.method !== "ask" || request.secret === true || !Array.isArray(request.questions)) return null;
  const first = request.questions[0] as { id?: unknown } | undefined;
  const id = typeof first?.id === "string" ? /^(d-[a-f0-9]{24}):0$/.exec(first.id)?.[1] : undefined;
  if (!id) return null;
  const dialog = parseDialog(request);
  if (dialog.method !== "ask" || dialog.questions.some((question, index) => question.id !== `${id}:${index}`))
    throw new Error("Decision question identity changed");
  return id;
}

const metadataKey = "decisions:v1";
const receipts = (store: StorePort): Record<string, Receipt> => JSON.parse(store.getMeta(metadataKey) ?? "{}");

/** Called inside the approval insert transaction, never before native UI exists. */
export function captureDecision(store: StorePort, approval: Approval): void {
  if (!approval.decision) return;
  const { id } = approval.decision;
  if (decisionRequestId(approval.request) !== id) throw new Error("Decision does not match its native questions");
  const records = receipts(store);
  if (records[id]) throw new Error("Decision already has a native request; do not ask it again");
  records[id] = { approvalId: approval.id, createdAt: Date.now() };
  store.setMeta(metadataKey, JSON.stringify(records));
}

export function validateDecisionAnswer(question: Question, answer: NonNullable<DialogResponse["answers"]>[number]): void {
  if (!answer || answer.id !== question.id || !Array.isArray(answer.selectedOptions)
    || answer.selectedOptions.some(option => typeof option !== "string" || !question.options.some(choice => choice.label === option))
    || new Set(answer.selectedOptions).size !== answer.selectedOptions.length
    || answer.customInput !== undefined && typeof answer.customInput !== "string"
    || !question.multi && (answer.selectedOptions.length > 1 || answer.selectedOptions.length > 0 && !!answer.customInput?.trim()
      || answer.selectedOptions.length === 0 && !answer.customInput?.trim()))
    throw new DeliveryError("Decision response does not match its original question and choices", "rejected");
}

/** Validate before any final admission or partial-question advance. */
export function validateDecisionResponse(approval: Approval, response: DialogResponse | undefined): void {
  if (!approval.decision) return;
  const dialog = parseDialog(approval.request);
  if (!response || dialog.method !== "ask") throw new DeliveryError("Decision requires an exact owner answer", "rejected");
  if (response.cancelled === true) {
    if (response.answers !== undefined || response.value !== undefined || response.confirmed !== undefined)
      throw new DeliveryError("A cancelled decision cannot contain an answer", "rejected");
  } else {
    if (response.value !== undefined || response.confirmed !== undefined
      || !Array.isArray(response.answers) || response.answers.length !== dialog.questions.length)
      throw new DeliveryError("Decision response must answer the original questions", "rejected");
    for (const [index, question] of dialog.questions.entries()) validateDecisionAnswer(question, response.answers[index]!);
  }
}

/** Called within the approval compare-and-set transaction, before any native effect. */
export function resolveDecision(store: StorePort, approval: Approval, state: "answered" | "cancelled",
  resolution?: ApprovalResolution): void {
  if (!approval.decision) return;
  const records = receipts(store);
  const receipt = records[approval.decision.id];
  if (!receipt || receipt.approvalId !== approval.id) throw new Error("Decision receipt does not match its native request");
  if (state === "cancelled") {
    receipt.withdrawal = { at: Date.now(), reason: resolution?.reason ?? "Native question cancelled or no longer pending",
      ...(resolution?.sourceId ? { sourceId: resolution.sourceId } : {}) };
  } else {
    validateDecisionResponse(approval, resolution?.response);
    receipt.response = resolution!.response;
    if (resolution?.headSelection) receipt.headSelection = resolution.headSelection;
  }
  store.setMeta(metadataKey, JSON.stringify(records));
}

/** Absence is acknowledged only against the exact live native session/generation. */
export function acknowledgeDecision(store: StorePort, approval: Approval, sessionId: string): void {
  if (!approval.decision || approval.decision.sessionId !== sessionId || approval.state === "pending") return;
  const records = receipts(store);
  const receipt = records[approval.decision.id];
  if (!receipt || receipt.approvalId !== approval.id || approval.state === "answered" && !receipt.response)
    throw new Error("Decision answer receipt is missing");
  if (receipt.confirmedAt !== undefined) return;
  receipt.confirmedAt = Date.now();
  if (receipt.response?.cancelled) receipt.withdrawal = { at: receipt.confirmedAt, reason: "Owner cancelled the original question" };
  store.setMeta(metadataKey, JSON.stringify(records));
}

export function decisionViews(store: StorePort): DecisionView[] {
  return Object.entries(receipts(store)).map(([id, receipt]) => {
    const approval = store.approval(receipt.approvalId);
    if (!approval?.decision || approval.decision.id !== id || decisionRequestId(approval.request) !== id)
      throw new Error("Decision ledger is not bound to its original native question");
    const dialog = parseDialog(approval.request);
    if (dialog.method !== "ask") throw new Error("Decision question is unavailable");
    const state = approval.state === "pending" ? "pending"
      : approval.state === "cancelled" ? "withdrawn"
      : receipt.confirmedAt === undefined ? "answering"
      : receipt.response?.cancelled ? "withdrawn" : "answered";
    return { id, state, ...receipt, sourceId: approval.decision.sourceId, target: approval.target,
      sessionId: approval.decision.sessionId, generation: approval.generation, requestId: approval.requestId,
      route: approval.route, remoteId: approval.remoteId, questions: dialog.questions };
  });
}
