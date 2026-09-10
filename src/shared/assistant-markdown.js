const ASSISTANT_REPLY_MAX_BYTES = 24 * 1024;
const DANGEROUS_HTML_TAG_RE = /<\/?(script|style|iframe|object|embed|meta|link)[^>]*>/gi;
const DANGEROUS_LINK_RE = /(\]\()\s*(javascript:|data:text\/html)[^)]+(\))/gi;
const THINK_TAG_RE = /<\/?think>/gi;

function sanitizeAssistantMarkdown(text, options = {}) {
  const preserveHeadings = Boolean(options.preserveHeadings);
  let normalized = String(text || "")
    .replace(/\r\n/g, "\n")
    .replace(/\u0000/g, "")
    .replace(THINK_TAG_RE, "")
    .replace(DANGEROUS_HTML_TAG_RE, "")
    .replace(DANGEROUS_LINK_RE, "$1about:blank$3")
    .replace(/\n{4,}/g, "\n\n\n");

  if (!preserveHeadings) {
    normalized = normalized.replace(/^\s{0,3}#{1,6}\s+(.+)$/gm, (_, title) => `**${String(title).trim()}**`);
  }

  normalized = normalized.trim();

  if (Buffer.byteLength(normalized, "utf8") <= ASSISTANT_REPLY_MAX_BYTES) {
    return normalized;
  }
  const suffix = "\n\n_内容过长，已截断显示。_";
  const budget = ASSISTANT_REPLY_MAX_BYTES - Buffer.byteLength(suffix, "utf8");
  if (budget <= 0) {
    return suffix.trim();
  }
  const clipped = clipUtf8ByBytes(normalized, budget);
  return `${clipped}${suffix}`;
}

function formatCardKitAssistantMarkdown(text) {
  const sanitized = sanitizeAssistantMarkdown(text, { preserveHeadings: true });
  return optimizeCardKitMarkdown(sanitized);
}

function splitAssistantReplyForDisplay(text) {
  const normalized = sanitizeAssistantMarkdown(text, { preserveHeadings: true });
  const marker = findFinalAnswerMarker(normalized);
  if (marker <= 0) {
    return {
      answerText: normalized,
      preAnswerText: "",
    };
  }

  const preAnswerText = normalized.slice(0, marker).trim();
  const answerText = normalized.slice(marker).trim();
  if (!answerText || answerText.length < 16) {
    return {
      answerText: normalized,
      preAnswerText: "",
    };
  }
  return {
    answerText,
    preAnswerText,
  };
}

function findFinalAnswerMarker(text) {
  const normalized = String(text || "");
  const markerRe = /(?:^|\n{2,})((?:完成了|已完成|处理好了|结论是|先说结论|答案是)[，,。；;：:\s])/g;
  let lastIndex = -1;
  let match;
  while ((match = markerRe.exec(normalized)) !== null) {
    const prefixLength = match[0].length - match[1].length;
    lastIndex = match.index + prefixLength;
  }
  return lastIndex;
}

function optimizeCardKitMarkdown(text) {
  const codeBlocks = [];
  const marker = "___CODEX_CARDKIT_CODE_BLOCK_";
  let normalized = String(text || "").replace(/```[\s\S]*?```/g, (match) => {
    const index = codeBlocks.push(match) - 1;
    return `${marker}${index}___`;
  });

  normalized = downgradeHeadingsForCardKit(normalized);
  normalized = repairMarkdownTables(normalized);

  // 飞书卡片 lark_md 渲染：单 \n 与 <br> 均会被压成空格（“显示太紧凑/没换行”），
  // 仅空行分段（\n\n）在卡片内有效（用户实测：段落式卡片显示正常）。
  // 代码块已用 marker 保护，此处只处理正文：
  //   1) 标题行后的单 \n 升级为段落空行（\n\n）；
  //   2) 其余单 \n → \n\n（段落化：列表项、伪表格行、逐行输出都逐行成段显示）；
  //   3) 已有 \n\n 段落分隔保持不变，连续空行压缩。
  normalized = normalized.replace(/^(#{4,6}[^\n]*)\n/gm, "$1\n\n");
  normalized = normalized.replace(/([^\n])\n([^\n])/g, "$1\n\n$2");

  codeBlocks.forEach((block, index) => {
    normalized = normalized.replace(`${marker}${index}___`, `\n\n${block}\n\n`);
  });

  return normalized.replace(/\n{4,}/g, "\n\n\n").trim();
}

function downgradeHeadingsForCardKit(text) {
  if (!/^#{1,3}\s+/m.test(text)) {
    return text;
  }
  return text
    .replace(/^#{2,6}\s+(.+)$/gm, "##### $1")
    .replace(/^#\s+(.+)$/gm, "#### $1");
}

function repairMarkdownTables(text) {
  // 飞书卡片 lark_md 不支持 GFM 表格语法（竖线行渲染不成表格），
  // 因此把 markdown 表格块转成 lark_md 可渲染的形式：
  //   表头行 → **单元格1 ｜ 单元格2**（加粗）
  //   数据行 → - 单元格1 ｜ 单元格2（无序列表）
  //   分隔行 → 丢弃
  // 分隔符用全角竖线（｜），避免与 markdown 列表/竖线语法冲突。
  const lines = String(text || "").split("\n");
  const output = [];
  let previousWasTable = false;
  let tableColumnCount = 0;

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const headerCells = parseMarkdownTableRow(line);
    const nextLine = lines[index + 1] || "";

    if (headerCells && headerCells.length >= 1 && looksLikeTableSeparator(nextLine)) {
      if (output.length && output[output.length - 1].trim() && !previousWasTable) {
        output.push("");
      }
      tableColumnCount = headerCells.length;
      output.push(formatTableHeaderLine(headerCells));
      previousWasTable = true;
      index += 1;
      continue;
    }

    const rowCells = previousWasTable ? parseLooseMarkdownTableRow(line) : null;
    if (rowCells && rowCells.length >= 1) {
      output.push(formatTableDataLine(padTableCells(rowCells, tableColumnCount)));
      previousWasTable = true;
      continue;
    }

    if (previousWasTable && line.trim()) {
      output.push("");
    }
    output.push(line);
    previousWasTable = false;
    tableColumnCount = 0;
  }

  return output.join("\n");
}

function parseMarkdownTableRow(line) {
  const trimmed = String(line || "").trim();
  if (!trimmed.startsWith("|") || !trimmed.endsWith("|")) {
    return null;
  }
  // 先保护转义竖线（\|），切分后再还原，避免单元格内容被拆开
  const protectedText = trimmed.replace(/\\\|/g, "\u0001");
  const cells = protectedText
    .slice(1, -1)
    .split("|")
    .map((cell) => String(cell || "").trim().replace(/\u0001/g, "|"));
  return cells.length >= 1 ? cells : null;
}

function parseLooseMarkdownTableRow(line) {
  const trimmed = String(line || "").trim();
  if (!trimmed.includes("|") || looksLikeTableSeparator(trimmed)) {
    return null;
  }
  const normalized = trimmed.startsWith("|") ? trimmed : `| ${trimmed}`;
  const withRightPipe = normalized.endsWith("|") ? normalized : `${normalized} |`;
  return parseMarkdownTableRow(withRightPipe);
}

function looksLikeTableSeparator(line) {
  const trimmed = String(line || "").trim();
  if (!trimmed || !trimmed.includes("-")) {
    return false;
  }
  const normalized = trimmed
    .replace(/^\|/, "")
    .replace(/\|$/, "");
  const cells = normalized.split("|").map((cell) => cell.trim()).filter(Boolean);
  return cells.length > 0 && cells.every((cell) => /^:?-{3,}:?$/.test(cell));
}

function formatTableHeaderLine(cells) {
  return `**${cells.map((cell) => String(cell || "").trim()).join(" ｜ ")}**`;
}

function formatTableDataLine(cells) {
  return `- ${cells.map((cell) => String(cell || "").trim()).join(" ｜ ")}`;
}

function padTableCells(cells, targetLength) {
  if (cells.length >= targetLength) {
    return cells;
  }
  return [...cells, ...Array.from({ length: targetLength - cells.length }, () => "")];
}

function clipUtf8ByBytes(input, maxBytes) {
  if (!input || maxBytes <= 0) {
    return "";
  }
  let bytes = 0;
  let endIndex = 0;
  for (const char of input) {
    const next = Buffer.byteLength(char, "utf8");
    if (bytes + next > maxBytes) {
      break;
    }
    bytes += next;
    endIndex += char.length;
  }
  return input.slice(0, endIndex);
}

module.exports = {
  formatCardKitAssistantMarkdown,
  sanitizeAssistantMarkdown,
  splitAssistantReplyForDisplay,
};
