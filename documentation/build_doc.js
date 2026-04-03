const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, HeadingLevel,
  AlignmentType, TabStopType, TabStopPosition,
  Table, TableRow, TableCell, WidthType, BorderStyle,
  ShadingType, PageBreak, Header, Footer,
  TableOfContents, StyleLevel,
} = require("docx");

// ─── Read markdown files ───
const docsDir = __dirname;
const readmeMd = fs.readFileSync(path.join(docsDir, "Project_README.md"), "utf8");
const runbookMd = fs.readFileSync(path.join(docsDir, "Architecture_and_Runbook.md"), "utf8");
const teamMd = fs.readFileSync(path.join(docsDir, "Team_Roles_and_Tasks.md"), "utf8");

// ─── Color palette ───
const COLORS = {
  primaryBlue: "1B4F72",
  darkBlue: "0D3B66",
  lightBlue: "D4E6F1",
  accentTeal: "148F77",
  headerBg: "1B4F72",
  headerText: "FFFFFF",
  tableBorder: "85929E",
  codeBlockBg: "F2F3F4",
  black: "000000",
  darkGray: "2C3E50",
  medGray: "5D6D7E",
  white: "FFFFFF",
};

// ─── Helpers ───
function heading(text, level) {
  const map = {
    1: HeadingLevel.HEADING_1,
    2: HeadingLevel.HEADING_2,
    3: HeadingLevel.HEADING_3,
    4: HeadingLevel.HEADING_4,
  };
  return new Paragraph({
    heading: map[level] || HeadingLevel.HEADING_3,
    spacing: { before: level === 1 ? 400 : 240, after: 120 },
    children: [
      new TextRun({
        text,
        bold: true,
        size: level === 1 ? 36 : level === 2 ? 28 : level === 3 ? 24 : 22,
        color: level <= 2 ? COLORS.primaryBlue : COLORS.darkGray,
        font: "Calibri",
      }),
    ],
  });
}

function para(text, opts = {}) {
  return new Paragraph({
    spacing: { after: opts.spacingAfter || 100 },
    indent: opts.indent ? { left: opts.indent } : undefined,
    children: [
      new TextRun({
        text,
        size: opts.size || 21,
        font: opts.font || "Calibri",
        bold: opts.bold || false,
        italics: opts.italics || false,
        color: opts.color || COLORS.darkGray,
      }),
    ],
  });
}

function bulletPara(text, level = 0) {
  // Clean checkbox markers
  let cleaned = text.replace(/^\s*-\s*\[[ x\/]\]\s*/, "").replace(/^\s*-\s*/, "").trim();
  return new Paragraph({
    spacing: { after: 60 },
    bullet: { level },
    children: [
      new TextRun({
        text: cleaned,
        size: 21,
        font: "Calibri",
        color: COLORS.darkGray,
      }),
    ],
  });
}

function codeBlock(lines) {
  return lines.map(
    (line) =>
      new Paragraph({
        spacing: { after: 0 },
        shading: { type: ShadingType.SOLID, color: COLORS.codeBlockBg },
        indent: { left: 360 },
        children: [
          new TextRun({
            text: line || " ",
            size: 18,
            font: "Consolas",
            color: COLORS.darkGray,
          }),
        ],
      })
  );
}

function makeTableFromMdRows(headerRow, dataRows) {
  const colCount = headerRow.length;
  const rows = [];

  // Header
  rows.push(
    new TableRow({
      tableHeader: true,
      children: headerRow.map(
        (cell) =>
          new TableCell({
            shading: { type: ShadingType.SOLID, color: COLORS.headerBg },
            children: [
              new Paragraph({
                spacing: { after: 40 },
                children: [
                  new TextRun({
                    text: cell.trim(),
                    bold: true,
                    size: 19,
                    font: "Calibri",
                    color: COLORS.white,
                  }),
                ],
              }),
            ],
          })
      ),
    })
  );

  // Data rows
  dataRows.forEach((row, ri) => {
    rows.push(
      new TableRow({
        children: row.map(
          (cell) =>
            new TableCell({
              shading: ri % 2 === 0 ? { type: ShadingType.SOLID, color: COLORS.lightBlue } : undefined,
              children: [
                new Paragraph({
                  spacing: { after: 40 },
                  children: [
                    new TextRun({
                      text: cell.trim(),
                      size: 19,
                      font: "Calibri",
                      color: COLORS.darkGray,
                    }),
                  ],
                }),
              ],
            })
        ),
      })
    );
  });

  return new Table({
    width: { size: 100, type: WidthType.PERCENTAGE },
    rows,
  });
}

function parseMdTable(lines) {
  // lines = array of "|col1|col2|..." strings
  const parsed = lines.map((l) =>
    l.split("|").filter((c, i, arr) => i > 0 && i < arr.length - 1).map((c) => c.trim())
  );
  if (parsed.length < 2) return null;
  const header = parsed[0];
  // skip separator row (index 1)
  const data = parsed.slice(2);
  return { header, data };
}

// ─── Main parser: converts MD text to docx Paragraph[] ───
function mdToDocx(mdText, sectionTitle) {
  const elements = [];
  const lines = mdText.split("\n");
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    // Skip empty lines
    if (line.trim() === "") {
      i++;
      continue;
    }

    // Skip horizontal rules
    if (/^---+\s*$/.test(line.trim())) {
      i++;
      continue;
    }

    // Headings
    const h4 = line.match(/^####\s+(.+)/);
    if (h4) {
      elements.push(heading(h4[1].replace(/\*\*/g, ""), 4));
      i++;
      continue;
    }
    const h3 = line.match(/^###\s+(.+)/);
    if (h3) {
      elements.push(heading(h3[1].replace(/\*\*/g, ""), 3));
      i++;
      continue;
    }
    const h2 = line.match(/^##\s+(.+)/);
    if (h2) {
      elements.push(heading(h2[1].replace(/\*\*/g, ""), 2));
      i++;
      continue;
    }
    const h1 = line.match(/^#\s+(.+)/);
    if (h1) {
      // skip the top-level title if it matches sectionTitle (we add it separately)
      i++;
      continue;
    }

    // Code blocks
    if (line.trim().startsWith("```")) {
      const codeLines = [];
      i++; // skip opening ```
      while (i < lines.length && !lines[i].trim().startsWith("```")) {
        codeLines.push(lines[i]);
        i++;
      }
      i++; // skip closing ```
      elements.push(...codeBlock(codeLines));
      elements.push(para("", { spacingAfter: 80 }));
      continue;
    }

    // Table blocks
    if (line.trim().startsWith("|")) {
      const tableLines = [];
      while (i < lines.length && lines[i].trim().startsWith("|")) {
        tableLines.push(lines[i]);
        i++;
      }
      const tbl = parseMdTable(tableLines);
      if (tbl && tbl.header.length > 0) {
        elements.push(makeTableFromMdRows(tbl.header, tbl.data));
        elements.push(para("", { spacingAfter: 80 }));
      }
      continue;
    }

    // Blockquotes / notes
    if (line.trim().startsWith(">")) {
      let text = line.replace(/^>\s*/, "").replace(/\*\*/g, "").replace(/💡\s*/, "").trim();
      // Skip alert markers like [!NOTE]
      if (/^\[!/.test(text)) { i++; continue; }
      elements.push(
        new Paragraph({
          spacing: { after: 80 },
          indent: { left: 360 },
          shading: { type: ShadingType.SOLID, color: "FEF9E7" },
          children: [
            new TextRun({
              text: "💡 " + text,
              size: 20,
              font: "Calibri",
              italics: true,
              color: COLORS.medGray,
            }),
          ],
        })
      );
      i++;
      continue;
    }

    // Bullet / checkbox
    if (/^\s*[-*]\s/.test(line)) {
      const indent = line.search(/\S/);
      const level = indent >= 4 ? 1 : 0;
      elements.push(bulletPara(line, level));
      i++;
      continue;
    }

    // Bold-only line (like **Deliverable:** ...)
    if (/^\*\*.+\*\*/.test(line.trim())) {
      const cleaned = line.replace(/\*\*/g, "").trim();
      elements.push(para(cleaned, { bold: true, size: 21 }));
      i++;
      continue;
    }

    // Regular paragraph
    let text = line
      .replace(/\*\*(.+?)\*\*/g, "$1")
      .replace(/`(.+?)`/g, "$1")
      .trim();
    if (text) {
      elements.push(para(text));
    }
    i++;
  }

  return elements;
}

// ─── Build each section ───
function buildCoverPage() {
  return [
    new Paragraph({ spacing: { before: 2400 }, children: [] }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { after: 200 },
      children: [
        new TextRun({
          text: "J2D HEALTHCARE",
          bold: true,
          size: 56,
          font: "Calibri",
          color: COLORS.primaryBlue,
        }),
      ],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { after: 100 },
      children: [
        new TextRun({
          text: "End-to-End Azure Data Engineering Project",
          size: 32,
          font: "Calibri",
          color: COLORS.accentTeal,
        }),
      ],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { after: 600 },
      children: [
        new TextRun({
          text: "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
          size: 20,
          color: COLORS.primaryBlue,
        }),
      ],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { after: 100 },
      children: [
        new TextRun({ text: "Medallion Architecture  |  Azure Databricks  |  Synapse Analytics  |  Power BI", size: 22, font: "Calibri", color: COLORS.medGray }),
      ],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { after: 100 },
      children: [
        new TextRun({ text: "MySQL  •  Azure SQL  •  Azure PostgreSQL  →  ADLS Gen2  →  Parquet", size: 20, font: "Calibri", color: COLORS.medGray }),
      ],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { after: 600 },
      children: [
        new TextRun({ text: "Weekly Pipeline with Manual & Auto Triggers", size: 22, font: "Calibri", bold: true, color: COLORS.accentTeal }),
      ],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { after: 100 },
      children: [
        new TextRun({ text: "Azure Free Trial Compatible", size: 24, font: "Calibri", bold: true, color: COLORS.primaryBlue }),
      ],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { after: 100 },
      children: [
        new TextRun({ text: `Document Version: 1.0  |  Date: ${new Date().toISOString().split("T")[0]}`, size: 18, font: "Calibri", color: COLORS.medGray }),
      ],
    }),
    new Paragraph({
      children: [new PageBreak()],
    }),
  ];
}

// ─── Table of Contents page ───
function buildTocPage() {
  const tocEntries = [
    { title: "PART 1 — Project Overview & Architecture", page: "", level: 0 },
    { title: "Project Overview", page: "", level: 1 },
    { title: "Dataset Overview", page: "", level: 1 },
    { title: "Architecture Diagram", page: "", level: 1 },
    { title: "Repository Structure", page: "", level: 1 },
    { title: "Silver Layer – Data Model", page: "", level: 1 },
    { title: "Gold Layer – Business Outputs", page: "", level: 1 },
    { title: "Azure Free Trial Cost Estimate", page: "", level: 1 },
    { title: "Power BI Dashboard Layout", page: "", level: 1 },
    { title: "Audit Trail", page: "", level: 1 },
    { title: "", page: "", level: 0 },
    { title: "PART 2 — Step-by-Step Architecture & Runbook", page: "", level: 0 },
    { title: "Phase 1: Azure Resource Creation", page: "", level: 1 },
    { title: "Phase 2: Azure Data Factory Setup", page: "", level: 1 },
    { title: "Phase 3: Upload Notebooks to Databricks", page: "", level: 1 },
    { title: "Phase 4: Databricks Job", page: "", level: 1 },
    { title: "Phase 5: Synapse Setup", page: "", level: 1 },
    { title: "Phase 6: Power BI Desktop", page: "", level: 1 },
    { title: "Phase 7: Trigger Testing – Manual & Automatic", page: "", level: 1 },
    { title: "Observability & Monitoring", page: "", level: 1 },
    { title: "Troubleshooting", page: "", level: 1 },
    { title: "", page: "", level: 0 },
    { title: "PART 3 — Team Roles & Task Breakdown", page: "", level: 0 },
    { title: "Role 1: Data Engineer – Ingestion", page: "", level: 1 },
    { title: "Role 2: Data Engineer – Processing", page: "", level: 1 },
    { title: "Role 3: Data Engineer – Gold Layer", page: "", level: 1 },
    { title: "Role 4: Data Warehouse Engineer", page: "", level: 1 },
    { title: "Role 5: BI Developer", page: "", level: 1 },
    { title: "Role 6: DevOps / Operations Engineer", page: "", level: 1 },
    { title: "Sprint Timeline", page: "", level: 1 },
    { title: "Industry Practices Demonstrated", page: "", level: 1 },
  ];

  const elements = [
    heading("Table of Contents", 1),
    para("", { spacingAfter: 200 }),
  ];

  for (const entry of tocEntries) {
    if (!entry.title) {
      elements.push(para("", { spacingAfter: 80 }));
      continue;
    }
    if (entry.level === 0) {
      elements.push(
        new Paragraph({
          spacing: { before: 160, after: 80 },
          children: [
            new TextRun({
              text: entry.title,
              bold: true,
              size: 24,
              font: "Calibri",
              color: COLORS.primaryBlue,
            }),
          ],
        })
      );
    } else {
      elements.push(
        new Paragraph({
          spacing: { after: 40 },
          indent: { left: 480 },
          children: [
            new TextRun({
              text: "▸  " + entry.title,
              size: 21,
              font: "Calibri",
              color: COLORS.darkGray,
            }),
          ],
        })
      );
    }
  }

  elements.push(new Paragraph({ children: [new PageBreak()] }));
  return elements;
}

// ─── Build full document ───
async function buildDocument() {
  const sections = [];

  // Cover page section
  const cover = buildCoverPage();
  const toc = buildTocPage();

  // Part 1: README
  const part1Title = [
    new Paragraph({
      spacing: { after: 80 },
      children: [
        new TextRun({
          text: "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
          size: 18,
          color: COLORS.primaryBlue,
        }),
      ],
    }),
    heading("PART 1 — Project Overview & Architecture", 1),
    new Paragraph({
      spacing: { after: 80 },
      children: [
        new TextRun({
          text: "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
          size: 18,
          color: COLORS.primaryBlue,
        }),
      ],
    }),
    para(""),
  ];
  const part1 = mdToDocx(readmeMd, "PART 1");

  // Part 2: Runbook
  const part2Title = [
    new Paragraph({ children: [new PageBreak()] }),
    new Paragraph({
      spacing: { after: 80 },
      children: [
        new TextRun({
          text: "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
          size: 18,
          color: COLORS.primaryBlue,
        }),
      ],
    }),
    heading("PART 2 — Step-by-Step Architecture & Runbook", 1),
    new Paragraph({
      spacing: { after: 80 },
      children: [
        new TextRun({
          text: "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
          size: 18,
          color: COLORS.primaryBlue,
        }),
      ],
    }),
    para(""),
  ];
  const part2 = mdToDocx(runbookMd, "PART 2");

  // Part 3: Team Roles
  const part3Title = [
    new Paragraph({ children: [new PageBreak()] }),
    new Paragraph({
      spacing: { after: 80 },
      children: [
        new TextRun({
          text: "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
          size: 18,
          color: COLORS.primaryBlue,
        }),
      ],
    }),
    heading("PART 3 — Team Roles & Task Breakdown", 1),
    new Paragraph({
      spacing: { after: 80 },
      children: [
        new TextRun({
          text: "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
          size: 18,
          color: COLORS.primaryBlue,
        }),
      ],
    }),
    para(""),
  ];
  const part3 = mdToDocx(teamMd, "PART 3");

  // Combine
  const allChildren = [
    ...cover,
    ...toc,
    ...part1Title,
    ...part1,
    ...part2Title,
    ...part2,
    ...part3Title,
    ...part3,
  ];

  const doc = new Document({
    styles: {
      default: {
        document: {
          run: {
            font: "Calibri",
            size: 21,
            color: COLORS.darkGray,
          },
        },
        heading1: {
          run: { font: "Calibri", size: 36, bold: true, color: COLORS.primaryBlue },
          paragraph: { spacing: { before: 400, after: 120 } },
        },
        heading2: {
          run: { font: "Calibri", size: 28, bold: true, color: COLORS.primaryBlue },
          paragraph: { spacing: { before: 280, after: 100 } },
        },
        heading3: {
          run: { font: "Calibri", size: 24, bold: true, color: COLORS.darkGray },
          paragraph: { spacing: { before: 200, after: 80 } },
        },
      },
    },
    sections: [
      {
        properties: {
          page: {
            margin: {
              top: 1134, // 0.79 inch
              bottom: 1134,
              left: 1134,
              right: 1134,
            },
          },
        },
        headers: {
          default: new Header({
            children: [
              new Paragraph({
                alignment: AlignmentType.RIGHT,
                children: [
                  new TextRun({
                    text: "J2D Healthcare – Azure Data Engineering Project",
                    size: 16,
                    font: "Calibri",
                    color: COLORS.medGray,
                    italics: true,
                  }),
                ],
              }),
            ],
          }),
        },
        footers: {
          default: new Footer({
            children: [
              new Paragraph({
                alignment: AlignmentType.CENTER,
                children: [
                  new TextRun({
                    text: "Confidential – J2D Healthcare Project Documentation",
                    size: 16,
                    font: "Calibri",
                    color: COLORS.medGray,
                  }),
                ],
              }),
            ],
          }),
        },
        children: allChildren,
      },
    ],
  });

  const buffer = await Packer.toBuffer(doc);
  const outPath = path.join(docsDir, "J2D_Healthcare_Project_Documentation_Updated.docx");
  fs.writeFileSync(outPath, buffer);
  console.log(`✅ Document generated: ${outPath}`);
  console.log(`   Size: ${(buffer.length / 1024).toFixed(1)} KB`);
}

buildDocument().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
