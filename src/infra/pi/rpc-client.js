"use strict";
/**
 * PiRpcClient —— 把 Pi（@earendil-works/pi-coding-agent）伪装成 Codex app-server
 *
 * 桥（domain/app/presentation 层）只认 infra/codex 那套 JSON-RPC 契约，
 * 换后端 = 实现同一套契约的另一个 infra 适配器，其余一行不用改。
 *
 * 契约（从 infra/codex/rpc-client.js + codex-event-service.js 扒出来的）：
 *   出：thread/start · thread/resume · turn/start · turn/steer · thread/list
 *       · model/list · turn/interrupt · approval response
 *   入：turn/started · item/started · item/agentMessage/delta · item/completed
 *       · turn/completed · turn/failed · turn/cancelled · <x>requestApproval
 *
 * Pi 侧用 `pi --mode rpc`（JSONL over stdio），把 Pi 事件流翻译成上面那套：
 *   Pi prompt/steer  → turn/start / turn/steer
 *   Pi message_update text_delta → item/agentMessage/delta
 *   Pi message_end（含完整文本）→ item/completed (agentMessage)
 *   Pi tool_execution_start/end → item/started / item/completed (commandExecution/mcpToolCall)
 *   Pi agent_settled → turn/completed；过程错误 → turn/failed
 *   Pi extension_ui_request（confirm/select/input/editor）→ <x>requestApproval（审批流）
 *
 * 设计：每轮一个 `pi --mode rpc` 子进程（仿 claude 后端），
 * threadId 即 Pi session-id，经 --session-dir 持久化，多线程互不干扰、崩溃自隔离。
 */
const { spawn, execFileSync } = require("child_process");
const { randomUUID } = require("crypto");
const path = require("path");
const os = require("os");
const fs = require("fs");

const DEFAULT_PI_COMMAND = path.join(os.homedir(), ".local/bin", "pi");
const DEFAULT_SESSION_DIR = path.join(
  os.homedir(),
  ".config",
  "agent-bridge",
  "pi-sessions"
);
const DEFAULT_PROVIDER = "maoge-dp4";
const MAX_IMAGE_BYTES = 10 * 1024 * 1024;

// 首个事件的等待上限：超过就认定后端没起来，主动失败而不是让人干等
const FIRST_EVENT_TIMEOUT_MS = Number(process.env.AGENT_BRIDGE_PI_FIRST_EVENT_MS || 60000);
// 整轮上限：Pi 干长活很正常，给宽一点，但不能无限
const TURN_TIMEOUT_MS = Number(process.env.AGENT_BRIDGE_PI_TURN_MS || 900000);

// Pi 支持的 thinking 等级（ultra→max 兜底，Pi 无 ultra）
const PI_THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);
const EFFORT_TO_THINKING = { ultra: "max" };

// Pi 模型目录里公认的 provider 名（直接 --provider 传，不走 --model 模糊匹配）
const KNOWN_PROVIDERS = new Set(["maoge-dp4", "terra", "sol", "deepseek-v4-flash"]);

// 静态兜底目录（`pi --list-models` 不可用时保证能启动）
const STATIC_MODEL_CATALOG = [
  { id: "maoge-dp4", displayName: "Maoge DP4 (deepseek-v4-flash)" },
  { id: "deepseek-v4-flash", displayName: "DeepSeek V4 Flash" },
  { id: "terra", displayName: "Terra (gpt-5.6-terra)" },
  { id: "sol", displayName: "Sol (gpt-5.6-sol)" },
];

// 群聊只读时允许保留的工具白名单（Pi --tools 接受逗号分隔 allowlist）
const GROUP_READONLY_TOOLS = [
  "read",
  "bash", // bash 仍保留用于纯查询，但配合群聊硬守卫提示约束（与 codex readOnly 沙箱同思路）
  "pgg_skill_search",
  "pgg_legal_kb_query",
  "describe_image",
  "ocr_image",
];

const MIME_BY_EXT = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".bmp": "image/bmp",
};

function normalizeNonEmptyString(value) {
  return typeof value === "string" && value.trim() ? value.trim() : "";
}

function isFullAccess(accessMode) {
  return normalizeNonEmptyString(accessMode).toLowerCase() === "full-access";
}

function isGroupReadonly(accessMode) {
  return normalizeNonEmptyString(accessMode).toLowerCase() === "group-readonly";
}

function normalizeEffort(value) {
  const raw = normalizeNonEmptyString(value).toLowerCase();
  if (!raw) {
    return "";
  }
  return EFFORT_TO_THINKING[raw] || raw;
}

class PiRpcClient {
  constructor(opts = {}) {
    this.env = opts.env || process.env;
    this.logLevel = opts.logLevel || "info";
    this.piCommand = normalizeNonEmptyString(opts.piCommand)
      || normalizeNonEmptyString(this.env.AGENT_BRIDGE_PI_COMMAND)
      || normalizeNonEmptyString(this.env.PI_BIN)
      || DEFAULT_PI_COMMAND;
    this.sessionDir = normalizeNonEmptyString(opts.sessionDir)
      || normalizeNonEmptyString(this.env.AGENT_BRIDGE_PI_SESSION_DIR)
      || DEFAULT_SESSION_DIR;
    this.defaultProvider = normalizeNonEmptyString(opts.defaultProvider)
      || normalizeNonEmptyString(this.env.AGENT_BRIDGE_PI_PROVIDER)
      || DEFAULT_PROVIDER;
    this.requestTimeoutMs = opts.requestTimeoutMs || 45000;
    this.turnStartTimeoutMs = opts.turnStartTimeoutMs || 300000;
    this.turnTimeoutMs = opts.turnTimeoutMs || TURN_TIMEOUT_MS;
    this.firstEventTimeoutMs = opts.firstEventTimeoutMs || FIRST_EVENT_TIMEOUT_MS;
    this.workspaceRoot = normalizeNonEmptyString(opts.workspaceRoot) || os.homedir();
    this.listeners = [];
    this.threads = new Map();   // threadId -> { sessionId, cwd }
    this.running = new Map();   // threadId -> { child, ctx }
    this.sessionStatsWaiters = new Map(); // threadId -> finish()（get_session_stats 响应等待）
    this.connected = false;
  }

  // ── 生命周期 ────────────────────────────────────────
  async connect() {
    this.connected = true;
    try {
      execFileSync(this.piCommand, ["--version"], {
        env: this.env,
        timeout: 10000,
        stdio: "pipe",
      });
      this.log(`pi backend ready: ${this.piCommand}`);
    } catch (error) {
      this.connected = false;
      throw new Error(
        `无法启动 Pi CLI（${this.piCommand}）。请确认已安装 pi-coding-agent，`
        + `或用 AGENT_BRIDGE_PI_COMMAND 指定路径。${error?.message || ""}`
      );
    }
    return true;
  }

  async connectSpawn() {
    return this.connect();
  }

  async connectWebSocket() {
    return this.connect();
  }

  async initialize() {
    return { protocolVersion: "1", serverInfo: { name: "pi-bridge", version: "0.1.0" } };
  }

  async restartSpawn() {
    this.killAll();
    return this.connect();
  }

  onMessage(listener) {
    if (typeof listener === "function") {
      this.listeners.push(listener);
    }
  }

  emit(method, params, requestId) {
    const message = { jsonrpc: "2.0", method, params };
    // 审批事件需要 message.id：桥的 approval-service 用 message.id 关联审批回复
    if (requestId !== undefined && requestId !== null && requestId !== "") {
      message.id = requestId;
    }
    for (const listener of this.listeners) {
      try {
        listener(message);
      } catch (error) {
        console.error(`[pi-im] listener error: ${error?.message}`);
      }
    }
  }

  log(message) {
    if (this.logLevel === "verbose") {
      console.log(`[pi-im] ${message}`);
    }
  }

  // ── 线程（threadId ↔ Pi session-id）────────────────
  async startThread({ cwd } = {}) {
    const threadId = randomUUID();
    this.threads.set(threadId, { sessionId: threadId, cwd: cwd || this.workspaceRoot });
    return this.threadResponse(threadId);
  }

  async resumeThread({ threadId }) {
    if (!threadId) {
      throw new Error("thread/resume requires a non-empty threadId");
    }
    if (!this.threads.has(threadId)) {
      this.threads.set(threadId, { sessionId: threadId, cwd: this.workspaceRoot });
    }
    return this.threadResponse(threadId);
  }

  threadResponse(threadId) {
    const thread = { id: threadId, threadId };
    return { result: { thread, threadId }, thread, threadId };
  }

  async listThreads() {
    const threads = [];
    const seen = new Set();
    for (const [id, entry] of this.threads.entries()) {
      seen.add(id);
      threads.push({
        id,
        threadId: id,
        cwd: entry.cwd || "",
        name: id.slice(0, 8),
        updatedAt: Date.now(),
        source: "runtime",
      });
    }
    // 补充 session-dir 里已持久化的 pi session（不读正文，只列元信息）
    try {
      fs.mkdirSync(this.sessionDir, { recursive: true });
      for (const name of fs.readdirSync(this.sessionDir)) {
        if (!name.endsWith(".jsonl")) {
          continue;
        }
        const id = name.slice(0, -".jsonl".length);
        if (seen.has(id)) {
          continue;
        }
        seen.add(id);
        const stat = fs.statSync(path.join(this.sessionDir, name));
        // Pi session 文件首行是 {type:"session",...,cwd:"..."}：恢复 cwd，
        // 让 workspace 过滤能匹配（否则每次桥重启 threadId 被清空 → 上下文断档）。
        let cwd = "";
        try {
          const firstLine = fs.readFileSync(path.join(this.sessionDir, name), "utf8")
            .split(/\r?\n/, 1)[0];
          if (firstLine) {
            const parsed = JSON.parse(firstLine);
            if (parsed && parsed.type === "session" && typeof parsed.cwd === "string") {
              cwd = parsed.cwd;
            }
          }
        } catch {
          // 首行不可解析时保持 cwd 为空
        }
        threads.push({
          id,
          threadId: id,
          cwd,
          name: id.slice(0, 8),
          updatedAt: stat.mtimeMs,
          source: "persisted",
        });
      }
    } catch {
      // session-dir 不可读时仅返回内存线程
    }
    threads.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
    return { result: { threads, data: threads }, threads, data: threads };
  }

  // ── 模型目录（真实读 pi --list-models）──────────────
  async listModels() {
    let models = null;
    try {
      const raw = execFileSync(this.piCommand, ["--list-models"], {
        env: this.env,
        timeout: 30000,
        encoding: "utf8",
      });
      models = parsePiModelList(raw);
    } catch (error) {
      this.log(`pi --list-models failed (${error?.message}), falling back to static catalog`);
    }

    const catalog = models && models.length ? models : STATIC_MODEL_CATALOG;
    // 以 provider 名为 model（dedupeKey），避免与真实模型名重复被 normalizeModelCatalog 去重吞掉；
    // 真实模型名放 displayName 展示。findModelByQuery("maoge-dp4") / ("sol") 均能匹配。
    const data = catalog.map((entry, index) => ({
      id: entry.provider || entry.id,
      model: entry.provider || entry.id,
      displayName: entry.displayName || `${entry.provider || entry.id} / ${entry.id}`,
      rawModel: entry.id,
      supportedReasoningEfforts: [...PI_THINKING_LEVELS],
      isDefault: entry.provider === this.defaultProvider || (index === 0 && !models),
    }));
    return { data, models: data };
  }

  // ── 核心：一轮对话 ──────────────────────────────────
  async sendUserMessage({
    threadId,
    text,
    attachments = [],
    model = null,
    effort = null,
    accessMode = null,
    workspaceRoot = "",
  }) {
    let tid = threadId;
    if (!tid) {
      ({ threadId: tid } = await this.startThread({ cwd: workspaceRoot }));
    }
    const st = this.threads.get(tid) || {
      sessionId: tid,
      cwd: workspaceRoot || this.workspaceRoot,
    };
    const turnId = randomUUID();

    // 同一线程不并发：连续催问时直接回一条占位，避免多个 pi 子进程抢 session。
    if (this.running.has(tid)) {
      this.emit("turn/started", { threadId: tid, turnId });
      this.emit("item/completed", {
        threadId: tid,
        turnId,
        item: {
          id: `busy-${turnId}`,
          type: "agentMessage",
          text: "⏳ 上一条还在处理中。为避免同一 Pi 会话并发抢占，本条没有重复发送；请等待当前回复，或先停止当前任务后再发。",
        },
      });
      this.emit("turn/completed", { threadId: tid, turnId });
      return { threadId: tid, turnId };
    }

    const cwd = st.cwd || workspaceRoot || this.workspaceRoot;
    const args = buildPiArgs({
      piCommand: this.piCommand,
      sessionId: st.sessionId || tid,
      sessionDir: this.sessionDir,
      model,
      effort,
      defaultProvider: this.defaultProvider,
      accessMode,
    });

    this.emit("turn/started", { threadId: tid, turnId });

    const child = spawn(this.piCommand, args, {
      cwd,
      env: this.env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    const ctx = {
      threadId: tid,
      turnId,
      child,
      openItems: new Map(),      // itemId -> type
      sawText: false,
      settled: false,
      sawFirstEvent: false,
      turnError: "",
      pendingFinalText: "",
      pendingApprovalCount: 0,
    };
    this.running.set(tid, ctx);

    const fail = (reason) => {
      if (ctx.settled) {
        return;
      }
      ctx.settled = true;
      try {
        child.kill("SIGTERM");
      } catch {
        // ignore
      }
      this.running.delete(tid);
      this.emit("turn/failed", {
        threadId: tid,
        turnId,
        error: { message: String(reason || "Pi 执行失败").slice(0, 600) },
      });
    };

    let firstTimer = setTimeout(() => {
      fail(`Pi 后端 ${Math.round(this.firstEventTimeoutMs / 1000)} 秒内没有任何响应，已中止。常见原因：模型渠道不可用、API key 缺失、进程启动失败。`);
    }, this.firstEventTimeoutMs);
    const turnTimer = setTimeout(() => {
      fail(`本轮超过 ${Math.round(this.turnTimeoutMs / 60000)} 分钟未完成，已中止。可发送 /stop 后重试。`);
    }, this.turnTimeoutMs);
    const clearFirst = () => {
      if (firstTimer) {
        clearTimeout(firstTimer);
        firstTimer = null;
      }
    };

    let buf = "";
    child.stdout.on("data", (chunk) => {
      buf += chunk.toString("utf8");
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        const trimmed = line.endsWith("\r") ? line.slice(0, -1) : line;
        if (!trimmed.trim()) {
          continue;
        }
        let event;
        try {
          event = JSON.parse(trimmed);
        } catch {
          continue;
        }
        if (!ctx.settled) {
          clearFirst();
          ctx.sawFirstEvent = true;
        }
        this.translate(event, ctx);
      }
    });

    let stderr = "";
    child.stderr.on("data", (chunk) => {
      const text = chunk.toString("utf8").trim();
      if (text) {
        stderr += text + "\n";
        console.error(`[pi-im] pi stderr: ${text.slice(0, 300)}`);
      }
    });

    child.on("close", (code) => {
      clearFirst();
      clearTimeout(turnTimer);
      if (ctx.settled) {
        return;
      }
      ctx.settled = true;
      this.running.delete(tid);
      for (const [id, type] of ctx.openItems.entries()) {
        this.emit("item/completed", {
          threadId: tid,
          turnId,
          item: { id, type },
        });
      }
      if (code === 0) {
        this.emit("turn/completed", { threadId: tid, turnId });
      } else {
        this.emit("turn/failed", {
          threadId: tid,
          turnId,
          error: { message: (stderr || `pi exited ${code}`).slice(0, 600) },
        });
      }
    });

    child.on("error", (error) => {
      clearFirst();
      clearTimeout(turnTimer);
      if (ctx.settled) {
        return;
      }
      ctx.settled = true;
      this.running.delete(tid);
      this.emit("turn/failed", {
        threadId: tid,
        turnId,
        error: { message: error?.message || "pi spawn failed" },
      });
    });

    // 发送 prompt（含图片附件 → base64）
    const images = collectImageAttachments(attachments);
    const promptPayload = { type: "prompt", message: text, streamingBehavior: "steer" };
    if (images.length) {
      promptPayload.images = images;
    }
    child.stdin.write(`${JSON.stringify(promptPayload)}\n`);

    return { threadId: tid, turnId };
  }

  /**
   * 运行中引导：桥在 activeTurn 存在时调用（群聊 / 连续催问）。
   * Pi 侧用 steer 命令注入，同一条消息只投递一次。
   */
  async steerTurn({ threadId, expectedTurnId, text, attachments = [], clientUserMessageId = "" }) {
    const tid = normalizeNonEmptyString(threadId);
    if (!tid) {
      throw new Error("turn/steer requires a non-empty threadId");
    }
    const active = this.running.get(tid);
    if (!active || !active.child || active.child.stdin.destroyed) {
      throw new Error("No active turn to steer: the Pi subprocess is not running");
    }
    const actualTurnId = active.turnId;
    if (expectedTurnId && actualTurnId && expectedTurnId !== actualTurnId) {
      throw new Error("Active turn changed before turn/steer was submitted");
    }
    const images = collectImageAttachments(attachments);
    const payload = { type: "steer", message: text };
    if (images.length) {
      payload.images = images;
    }
    active.child.stdin.write(`${JSON.stringify(payload)}\n`);
    return { threadId: tid, turnId: actualTurnId };
  }

  async sendRequest(method, params = {}) {
    const normalizedMethod = String(method || "").toLowerCase();
    if (normalizedMethod === "turn/interrupt") {
      return this.interruptTurn(params);
    }
    if (normalizedMethod === "thread/list" || normalizedMethod === "threads/list") {
      return this.listThreads();
    }
    this.log(`sendRequest(${method}) not implemented for pi backend, returning empty result`);
    return { result: {} };
  }

  async interruptTurn({ threadId, turnId } = {}) {
    const tid = normalizeNonEmptyString(threadId);
    const active = tid ? this.running.get(tid) : null;
    if (active && active.child && !active.child.stdin.destroyed) {
      active.child.stdin.write(`${JSON.stringify({ type: "abort" })}\n`);
    } else if (tid && !active) {
      // 没有运行中的子进程：把可能残留的 pi session 结束掉（killAll 兜底）
      this.killAll();
    }
    return { result: { ok: true } };
  }

  /**
   * 审批回复：桥的 approval-service 调 sendResponse(id, {decision})。
   * Pi 侧对应 extension_ui_response（confirm 语义：decision=accept → confirmed）。
   */
  async sendResponse(id, result = {}) {
    const decision = String(result?.decision || "").toLowerCase();
    const confirmed = decision === "accept";
    const requestId = String(id ?? "");
    let delivered = false;
    for (const ctx of this.running.values()) {
      if (!ctx.child || ctx.child.stdin.destroyed) {
        continue;
      }
      if (!requestId || requestId === ctx.pendingApprovalRequestId) {
        ctx.child.stdin.write(
          `${JSON.stringify({ type: "extension_ui_response", id: requestId, confirmed })}\n`
        );
        delivered = true;
        ctx.pendingApprovalRequestId = "";
        break;
      }
    }
    if (!delivered) {
      throw new Error("No pending Pi approval request to respond to");
    }
    return { result: { delivered } };
  }

  // ── Pi 事件 → Codex 事件翻译 ────────────────────────
  translate(event, ctx) {
    const type = event?.type;
    const tid = ctx.threadId;
    const turnId = ctx.turnId;

    if (type === "turn_start" || type === "agent_start") {
      // turn/started 已在 sendUserMessage 发出，这里不重复
      return;
    }

    if (type === "message_update") {
      const delta = event?.assistantMessageEvent || {};
      const deltaType = delta?.type;
      if (deltaType === "text_delta" && typeof delta?.delta === "string" && delta.delta) {
        ctx.sawText = true;
        this.emit("item/agentMessage/delta", {
          threadId: tid,
          turnId,
          delta: delta.delta,
        });
      }
      return;
    }

    if (type === "message_end") {
      // Pi 的 message_end 先于 text_delta 到达且带完整文本。
      // 策略：正文增量一律走 text_delta（打字机），全文只在“本轮没有 delta 输出”时兜底补发，
      // 避免飞书卡片出现“全文 + 增量”重复。
      const message = event?.message || {};
      const content = extractMessageText(message);
      if (content && !ctx.pendingFinalText) {
        ctx.pendingFinalText = content;
      }
      const reason = String(message?.stopReason || message?.reason || "").toLowerCase();
      if (reason === "error") {
        ctx.turnError = ctx.turnError || "模型返回错误（error）";
      }
      return;
    }

    if (type === "tool_execution_start") {
      const itemId = normalizeNonEmptyString(event?.toolCallId) || `tool-${turnId}-${ctx.openItems.size}`;
      const toolName = String(event?.toolName || "");
      const isShell = /^(bash|terminal|shell|run)$/i.test(toolName);
      const itemType = isShell ? "commandExecution" : "mcpToolCall";
      const item = { id: itemId, type: itemType, name: toolName };
      const args = event?.args;
      if (isShell) {
        item.command = String(args?.command || "").slice(0, 400);
      } else if (args && typeof args === "object") {
        item.arguments = args;
      }
      ctx.openItems.set(itemId, itemType);
      this.emit("item/started", { threadId: tid, turnId, item });
      return;
    }

    if (type === "tool_execution_end") {
      const itemId = normalizeNonEmptyString(event?.toolCallId) || "";
      const itemType = ctx.openItems.get(itemId) || "mcpToolCall";
      const result = event?.result;
      let output = "";
      try {
        const content = result?.content;
        if (Array.isArray(content)) {
          output = content.map((entry) => (typeof entry?.text === "string" ? entry.text : "")).join("\n");
        } else if (typeof result?.text === "string") {
          output = result.text;
        }
      } catch {
        output = "";
      }
      const item = { id: itemId || `tool-${turnId}`, type: itemType, output: output.slice(0, 4000) };
      if (event?.isError) {
        item.isError = true;
      }
      if (itemId) {
        ctx.openItems.delete(itemId);
      }
      this.emit("item/completed", { threadId: tid, turnId, item });
      return;
    }

    if (type === "extension_ui_request") {
      const method = String(event?.method || "");
      const isDialog = ["select", "confirm", "input", "editor"].includes(method);
      if (isDialog) {
        // 桥只认 method.endsWith("requestApproval") 的审批事件（codex 契约）。
        // 用 request id 关联回复：sendResponse(id, {decision}) → extension_ui_response。
        ctx.pendingApprovalRequestId = String(event?.id || "");
        const title = String(event?.title || "");
        const message = String(event?.message || "");
        const options = Array.isArray(event?.options) ? event.options : [];
        const commandSummary = buildApprovalSummary(method, title, message, options);
        this.emit("pi.requestApproval", {
          threadId: tid,
          turnId,
          reason: `${method}: ${title || message || "需要确认"}`,
          command: commandSummary,
          approvalKind: method,
          approvalOptions: options,
        }, ctx.pendingApprovalRequestId);
        return;
      }
      // fire-and-forget（notify/setStatus/setWidget/setTitle/set_editor_text）仅记录
      this.log(`extension ui (fire-and-forget): ${method}`);
      return;
    }

    if (type === "auto_retry_end" && event?.success === false) {
      ctx.turnError = ctx.turnError || String(event?.finalError || "自动重试耗尽，仍未成功");
      return;
    }

    if (type === "extension_error") {
      ctx.turnError = ctx.turnError || String(event?.error || "扩展执行出错");
      return;
    }

    if (type === "agent_settled") {
      // 定稿：一律补发 message_end 完整文本为 completed_snapshot。
      // 之前“有 delta 就跳过”会导致流式增量丢换行时最终卡片正文粘连
      // （Pi 的 text_delta 偶发丢失 \n，session 完整文本则保留换行）；
      // 下游 applyCompletedAssistantSnapshot 会以快照覆盖增量（更长才覆盖，不重复）。
      if (ctx.pendingFinalText) {
        this.emit("item/completed", {
          threadId: tid,
          turnId,
          item: { id: `msg-${turnId}`, type: "agentMessage", text: ctx.pendingFinalText },
        });
      }
      // 本轮定稿后、发出终态事件前，请求一次会话用量（token/上下文窗口），
      // 让卡片尾部能显示「📝 上下文 xx/xx · 进度条」（与 Codex/Claude/OpenCode 后端一致）。
      // 子进程可能已退出或超时：任何失败都静默降级，不影响终态交付。
      this._requestSessionStats(tid, ctx).then(() => {
        if (ctx.turnError) {
          this.emit("turn/failed", {
            threadId: tid,
            turnId,
            error: { message: ctx.turnError.slice(0, 600) },
          });
        } else {
          this.emit("turn/completed", { threadId: tid, turnId });
        }
        this.finishTurn(ctx);
      });
      return;
    }

    if (type === "response") {
      if (event?.command === "get_session_stats" && event?.success) {
        this._handleSessionStatsResponse(ctx, event);
        return;
      }
      if (event?.success === false) {
        // prompt 被拒绝（如模型错误）——从 response 里读错误
        ctx.turnError = ctx.turnError || String(event?.error || "Pi 拒绝了本次请求");
        return;
      }
      return;
    }
  }

  // ── 会话用量上报（卡片尾部「📝 上下文」）────────────────
  _requestSessionStats(threadId, ctx) {
    return new Promise((resolve) => {
      let done = false;
      const finish = () => {
        if (done) {
          return;
        }
        done = true;
        this.sessionStatsWaiters.delete(threadId);
        resolve();
      };
      const timer = setTimeout(finish, 1500);
      this.sessionStatsWaiters.set(threadId, finish);
      try {
        if (!ctx.child || ctx.child.stdin.destroyed) {
          clearTimeout(timer);
          finish();
          return;
        }
        ctx.child.stdin.write(`${JSON.stringify({ type: "get_session_stats" })}\n`);
      } catch {
        clearTimeout(timer);
        finish();
      }
    });
  }

  _resolveSessionStats(threadId) {
    const waiter = this.sessionStatsWaiters.get(threadId);
    if (typeof waiter === "function") {
      waiter();
    }
  }

  _handleSessionStatsResponse(ctx, event) {
    const data = event?.data || {};
    const ctxUsage = data?.contextUsage || {};
    const tokens = Number(ctxUsage?.tokens);
    const window = Number(ctxUsage?.contextWindow);
    if (Number.isFinite(tokens) && Number.isFinite(window) && tokens > 0 && window > 0) {
      this.emit("thread/tokenUsage/updated", {
        threadId: ctx.threadId,
        tokenUsage: {
          last: { totalTokens: tokens },
          modelContextWindow: window,
        },
      });
    }
    this._resolveSessionStats(ctx.threadId);
  }

  finishTurn(ctx) {
    if (ctx.settled) {
      return;
    }
    ctx.settled = true;
    try {
      if (ctx.child && !ctx.child.stdin.destroyed) {
        ctx.child.stdin.write(`${JSON.stringify({ type: "abort" })}\n`);
      }
    } catch {
      // ignore
    }
    setTimeout(() => {
      try {
        if (ctx.child && !ctx.child.killed) {
          ctx.child.kill("SIGTERM");
        }
      } catch {
        // ignore
      }
    }, 2000).unref();
    this.running.delete(ctx.threadId);
  }

  interrupt(threadId) {
    this.interruptTurn({ threadId }).catch(() => undefined);
  }

  killAll() {
    for (const ctx of this.running.values()) {
      try {
        if (ctx.child && !ctx.child.killed) {
          ctx.child.kill("SIGTERM");
        }
      } catch {
        // ignore
      }
    }
    this.running.clear();
  }

  rejectAllPending() {
    this.killAll();
  }

  getRequestTimeoutMs() {
    return this.turnTimeoutMs;
  }

  handleIncoming() {
    // 每轮独立子进程，无常驻 socket，这里无需处理
  }
}

// ── 辅助 ──────────────────────────────────────────────

function buildPiArgs({
  piCommand,
  sessionId,
  sessionDir,
  model,
  effort,
  defaultProvider,
  accessMode,
}) {
  const args = ["--mode", "rpc", "--session-id", sessionId, "--session-dir", sessionDir];
  args.push("--no-context-files"); // 不加载 cwd 的 AGENTS.md：飞书消息来自外部，避免把项目人格/规则当成不可信输入的一部分
  // 注意：模型上下文由 prompt 内注入，AGENTS.md 信任决策由桥层控制

  const rawModel = normalizeNonEmptyString(model);
  const provider = KNOWN_PROVIDERS.has(rawModel) ? rawModel : normalizeNonEmptyString(defaultProvider);
  if (provider) {
    args.push("--provider", provider);
  }
  if (rawModel && !KNOWN_PROVIDERS.has(rawModel)) {
    args.push("--model", rawModel);
  }

  const thinking = normalizeEffort(effort);
  if (thinking) {
    args.push("--thinking", thinking);
  }

  if (isGroupReadonly(accessMode)) {
    args.push("--no-builtin-tools", "--no-extensions");
    args.push("--tools", GROUP_READONLY_TOOLS.join(","));
    // --approve：只读场景不需要写权限，不追加
  } else {
    args.push("--approve"); // 信任项目本地文件（对应 codex workspaceWrite 语义；危险命令仍走 Pi 审批流）
  }

  // 可选：恢复加载绑定目录的 AGENTS.md（默认不加载，飞书外部消息优先安全）
  if (process.env.AGENT_BRIDGE_PI_CONTEXT_FILES === "1") {
    const idx = args.indexOf("--no-context-files");
    if (idx >= 0) {
      args.splice(idx, 1);
    }
  }

  return args;
}

function parsePiModelList(raw) {
  const lines = String(raw || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  const models = [];
  let started = false;
  for (const line of lines) {
    const normalized = line.replace(/\s+/g, " ").trim();
    if (!started) {
      if (/^provider\s/i.test(normalized)) {
        started = true;
      }
      continue;
    }
    const parts = normalized.split(" ");
    if (parts.length < 2) {
      continue;
    }
    const [provider, modelId] = parts;
    if (!provider || !modelId) {
      continue;
    }
    models.push({
      id: modelId,
      model: modelId,
      displayName: `${provider} / ${modelId}`,
      provider,
    });
  }
  return models;
}

function collectImageAttachments(attachments) {
  if (!Array.isArray(attachments)) {
    return [];
  }
  const images = [];
  for (const attachment of attachments) {
    const filePath = normalizeNonEmptyString(attachment?.filePath || attachment?.path);
    if (!filePath) {
      continue;
    }
    try {
      const stat = fs.statSync(filePath);
      if (!stat.isFile() || stat.size > MAX_IMAGE_BYTES) {
        continue;
      }
      const ext = path.extname(filePath).toLowerCase();
      const mimeType = MIME_BY_EXT[ext];
      if (!mimeType) {
        continue;
      }
      const data = fs.readFileSync(filePath).toString("base64");
      images.push({ type: "image", data, mimeType });
    } catch {
      // 附件读不到就跳过，不阻断整轮
    }
  }
  return images;
}

function extractMessageText(message) {
  if (!message || typeof message !== "object") {
    return "";
  }
  const content = message.content;
  if (typeof content === "string" && content.trim()) {
    return content.trim();
  }
  if (!Array.isArray(content)) {
    return "";
  }
  const parts = [];
  for (const entry of content) {
    if (typeof entry === "string" && entry.trim()) {
      parts.push(entry.trim());
      continue;
    }
    if (!entry || typeof entry !== "object") {
      continue;
    }
    const entryType = String(entry.type || "").toLowerCase();
    if (entryType === "text" && typeof entry.text === "string" && entry.text.trim()) {
      parts.push(entry.text.trim());
    }
  }
  return parts.join("\n").trim();
}

function buildApprovalSummary(method, title, message, options) {
  const parts = [];
  if (title) {
    parts.push(title);
  }
  if (message) {
    parts.push(message);
  }
  if (Array.isArray(options) && options.length) {
    parts.push(`选项：${options.map((entry) => String(entry ?? "")).join(" / ")}`);
  }
  const summary = parts.join("\n");
  if (summary) {
    return summary.slice(0, 400);
  }
  return `${method} 请求`;
}

module.exports = {
  PiRpcClient,
  // runtime 统一用 CodexRpcClient 解构（各后端同一接口），pi 模块兼容导出
  CodexRpcClient: PiRpcClient,
  buildPiArgs,
  parsePiModelList,
};
