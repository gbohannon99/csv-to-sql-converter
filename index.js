const express = require('express');
const multer = require('multer');
const Papa = require('papaparse');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const app = express();

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

const upload = multer({ 
  dest: '/tmp/',
  limits: { fileSize: 50 * 1024 * 1024 }
});

app.use(express.static('public'));

// Helper functions
function detectDataType(values) {
  const validValues = values.filter(v => v !== null && v !== undefined && v !== '');
  if (validValues.length === 0) return 'VARCHAR(255)';
  
  const sampleSize = Math.min(validValues.length, 500);
  const sample = validValues.slice(0, sampleSize);
  
  let allIntegers = true;
  let allDecimals = true;
  let allDates = true;
  let maxLength = 0;

  // Strict date regex: YYYY-MM-DD with valid month/day ranges only
  const strictDateRegex = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/;
  
  for (const value of sample) {
    const str = String(value).trim();
    maxLength = Math.max(maxLength, str.length);
    if (!/^-?\d+$/.test(str)) allIntegers = false;
    if (!/^-?\d*\.?\d+$/.test(str)) allDecimals = false;
    if (!strictDateRegex.test(str)) allDates = false;
  }
  
  if (allIntegers) return 'INTEGER';
  if (allDecimals) return 'DECIMAL(10,2)';
  if (allDates) return 'DATE';
  
  const varcharSize = Math.min(Math.max(maxLength * 1.5, 50), 255);
  return `VARCHAR(${Math.ceil(varcharSize)})`;
}

function convertToDialect(genericType, dialect) {
  const typeMap = {
    postgresql: { 'INTEGER': 'INTEGER', 'DECIMAL(10,2)': 'NUMERIC(10,2)', 'DATE': 'DATE', 'VARCHAR': 'VARCHAR' },
    mysql: { 'INTEGER': 'INT', 'DECIMAL(10,2)': 'DECIMAL(10,2)', 'DATE': 'DATE', 'VARCHAR': 'VARCHAR' },
    sqlserver: { 'INTEGER': 'INT', 'DECIMAL(10,2)': 'DECIMAL(10,2)', 'DATE': 'DATE', 'VARCHAR': 'VARCHAR' },
    sqlite: { 'INTEGER': 'INTEGER', 'DECIMAL(10,2)': 'REAL', 'DATE': 'TEXT', 'VARCHAR': 'TEXT' },
    oracle: { 'INTEGER': 'NUMBER', 'DECIMAL(10,2)': 'NUMBER(10,2)', 'DATE': 'DATE', 'VARCHAR': 'VARCHAR2' }
  };
  
  const map = typeMap[dialect] || typeMap.postgresql;
  
  if (genericType.startsWith('VARCHAR')) {
    const size = genericType.match(/\((\d+)\)/)?.[1] || '255';
    if (dialect === 'oracle') return `VARCHAR2(${size})`;
    if (dialect === 'sqlite') return 'TEXT';
    return `VARCHAR(${size})`;
  }
  
  return map[genericType] || genericType;
}

function sanitizeColumnName(name) {
  return name.trim().toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/^(\d)/, '_$1').substring(0, 64);
}

// ── Phase 1 helpers ──────────────────────────────────────────────────────────

function checkWhitespace(values, header) {
  const issues = values.filter(v => v !== null && v !== undefined && v !== '' && String(v) !== String(v).trim());
  return issues.length > 0 ? { type: 'whitespace', column: header, message: `Column "${header}" has ${issues.length} value(s) with leading/trailing whitespace`, details: `Examples: ${issues.slice(0,3).map(v=>`"${v}"`).join(', ')}`, severity: 'warning', fixable: true, fixType: 'trim' } : null;
}

function checkDuplicateHeaders(headers) {
  const seen = {}; const dupes = [];
  headers.forEach(h => { const lower = h.toLowerCase().trim(); seen[lower] = (seen[lower]||0)+1; if (seen[lower]===2) dupes.push(h); });
  return dupes;
}

function checkNulls(values, header) {
  const nullCount = values.filter(v => v === null || v === undefined || v === '').length;
  if (nullCount === 0) return null;
  const pct = ((nullCount/values.length)*100).toFixed(1);
  return { type: 'nulls', column: header, message: `Column "${header}" has ${nullCount} NULL/empty value(s) (${pct}%)`, severity: pct > 50 ? 'warning' : 'info' };
}

const BOOL_SETS = [new Set(['true','false']),new Set(['0','1']),new Set(['y','n']),new Set(['yes','no']),new Set(['t','f'])];

function checkBoolean(values, header) {
  const nonEmpty = values.filter(v => v !== null && v !== undefined && v !== '').map(v => String(v).toLowerCase().trim());
  if (nonEmpty.length === 0) return null;
  const unique = new Set(nonEmpty);
  if (unique.size <= 2) return null; // clean, consistent — no issue
  // Flag only if every unique value belongs to some boolean vocabulary
  const anyBoolValue = [...unique].every(v => BOOL_SETS.some(s => s.has(v)));
  if (!anyBoolValue) return null;
  return { type: 'boolean', column: header, message: `Column "${header}" appears boolean but has mixed representations`, details: `Found values: ${[...unique].slice(0,6).map(v=>`"${v}"`).join(', ')}`, severity: 'warning', fixable: true, fixType: 'normalize_bool' };
}

const DATE_FORMATS = [
  { regex: /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/, name: 'YYYY-MM-DD' },
  { regex: /^(0[1-9]|[12]\d|3[01])\/(0[1-9]|1[0-2])\/\d{4}$/, name: 'DD/MM/YYYY' },
  { regex: /^\d{1,2}\/\d{1,2}\/\d{4}$/, name: 'M/D/YYYY' },
  { regex: /^(0[1-9]|[12]\d|3[01])-(0[1-9]|1[0-2])-\d{4}$/, name: 'DD-MM-YYYY' },
  { regex: /^\d{4}\/(0[1-9]|1[0-2])\/(0[1-9]|[12]\d|3[01])$/, name: 'YYYY/MM/DD' },
];

const looseDatePattern = /^\d{4}-\d{2}-\d{2}$|^\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}$|^\d{4}[\/]\d{2}[\/]\d{2}$/;

function checkDateFormats(values, header) {
  const nonEmpty = values.filter(v => v !== null && v !== undefined && v !== '');
  if (nonEmpty.length === 0) return [];
  const looseLike = nonEmpty.filter(v => looseDatePattern.test(String(v).trim())).length;
  if (looseLike < nonEmpty.length * 0.4) return [];
  const formatCounts = {}; const invalidValues = [];
  nonEmpty.forEach(v => {
    const str = String(v).trim();
    if (!looseDatePattern.test(str)) return;
    const match = DATE_FORMATS.find(f => f.regex.test(str));
    if (match) formatCounts[match.name]=(formatCounts[match.name]||0)+1;
    else invalidValues.push(str);
  });
  const issues = []; const formatNames = Object.keys(formatCounts);
  if (formatNames.length > 1) issues.push({ type: 'mixed_date_format', column: header, message: `Column "${header}" has mixed date formats`, details: `Formats found: ${formatNames.map(f=>`${f} (${formatCounts[f]})`).join(', ')}`, severity: 'warning' });
  if (invalidValues.length > 0) issues.push({ type: 'invalid_date', column: header, message: `Column "${header}" has ${invalidValues.length} invalid date value(s)`, details: `Examples: ${invalidValues.slice(0,3).map(v=>`"${v}"`).join(', ')}`, severity: 'error' });
  return issues;
}

// ── Phase 2 helpers ──────────────────────────────────────────────────────────

function checkStringLength(values, header, detectedType) {
  if (!detectedType.startsWith('VARCHAR')) return null;
  const sizeMatch = detectedType.match(/\((\d+)\)/);
  if (!sizeMatch) return null;
  const maxLen = parseInt(sizeMatch[1]);
  const overLimit = values.filter(v => v !== null && v !== undefined && v !== '' && String(v).length > maxLen);
  if (overLimit.length === 0) return null;
  const worst = overLimit.reduce((a, b) => String(a).length > String(b).length ? a : b);
  return {
    type: 'string_length', column: header,
    message: `Column "${header}" has ${overLimit.length} value(s) exceeding detected ${detectedType} limit (${maxLen} chars)`,
    details: `Longest value: ${String(worst).length} chars — "${String(worst).substring(0, 60)}${String(worst).length > 60 ? '...' : ''}"`,
    severity: 'warning'
  };
}

function checkPrimaryKey(values, header) {
  const pkPattern = /^(id|.*_id|.*_key|.*_pk|record_id|uuid|guid)$/i;
  if (!pkPattern.test(header.trim())) return null;
  const nonEmpty = values.filter(v => v !== null && v !== undefined && v !== '');
  if (nonEmpty.length === 0) return null;
  const seen = new Set(); const dupes = [];
  nonEmpty.forEach(v => { const key = String(v).trim(); if (seen.has(key)) dupes.push(key); else seen.add(key); });
  if (dupes.length === 0) return null;
  const examples = [...new Set(dupes)].slice(0, 3).map(v => `"${v}"`).join(', ');
  return {
    type: 'pk_uniqueness', column: header,
    message: `Column "${header}" looks like a primary key but has ${dupes.length} duplicate value(s)`,
    details: `Duplicate values: ${examples}`,
    severity: 'error'
  };
}


function buildSummaryReport(rows, headers, allIssues) {
  const colSummary = {};
  allIssues.forEach(issue => { if (issue.column) { if (!colSummary[issue.column]) colSummary[issue.column]=[]; colSummary[issue.column].push(issue.type); } });
  const badRows = [];
  rows.slice(0,5000).forEach((row,idx) => {
    const problems = [];
    headers.forEach(h => { const v=row[h]; if (v===null||v===undefined||v==='') problems.push(`${h}: NULL`); else if (String(v)!==String(v).trim()) problems.push(`${h}: whitespace`); });
    if (problems.length >= Math.ceil(headers.length*0.3)) badRows.push({ rowNum: idx+2, problems: problems.slice(0,4) });
  });
  return { colSummary, badRows: badRows.slice(0,5) };
}

function runValidation(rows, headers) {
  const results = { passed: [], warnings: [], errors: [], info: [], summary: null };
  const dupeHeaders = checkDuplicateHeaders(headers);
  if (dupeHeaders.length > 0) results.errors.push({ type: 'duplicate_headers', message: `Duplicate column headers detected: ${dupeHeaders.map(h=>`"${h}"`).join(', ')}`, severity: 'error' });
  else results.passed.push({ type: 'headers', message: 'No duplicate column headers' });
  const expectedColumns = headers.length; let inconsistentRows = 0;
  rows.forEach(row => { if (Object.keys(row).length !== expectedColumns) inconsistentRows++; });
  if (inconsistentRows === 0) results.passed.push({ type: 'consistency', message: 'All rows have consistent column count' });
  else results.warnings.push({ type: 'consistency', message: `${inconsistentRows} row(s) have inconsistent column counts`, severity: 'warning' });
  const allIssues = [];
  headers.forEach(header => {
    const values = rows.map(row => row[header]);
    const nonEmpty = values.filter(v => v !== null && v !== undefined && v !== '');
    const wsIssue = checkWhitespace(nonEmpty, header); if (wsIssue) { results.warnings.push(wsIssue); allIssues.push(wsIssue); }
    const nullIssue = checkNulls(values, header); if (nullIssue) { if (nullIssue.severity==='warning') results.warnings.push(nullIssue); else results.info.push(nullIssue); allIssues.push(nullIssue); }
    const boolIssue = checkBoolean(nonEmpty, header); if (boolIssue) { results.warnings.push(boolIssue); allIssues.push(boolIssue); }
    const dateIssues = checkDateFormats(nonEmpty, header); dateIssues.forEach(issue => { if (issue.severity==='error') results.errors.push(issue); else results.warnings.push(issue); allIssues.push(issue); });
    // Phase 2
    const detectedType = detectDataType(values);
    const lenIssue = checkStringLength(nonEmpty, header, detectedType); if (lenIssue) { results.warnings.push(lenIssue); allIssues.push(lenIssue); }
    const pkIssue = checkPrimaryKey(values, header); if (pkIssue) { results.errors.push(pkIssue); allIssues.push(pkIssue); }
  });
  results.summary = buildSummaryReport(rows, headers, allIssues);
  if (results.warnings.length===0 && results.errors.length===0) results.passed.push({ type: 'overall', message: 'No data quality issues detected! 🎉' });
  return results;
}

function escapeSQLValue(value, dataType) {
  if (value === null || value === undefined || value === '') return 'NULL';
  const str = String(value).trim();
  if (dataType === 'INTEGER' || dataType === 'INT' || dataType === 'NUMBER') return str;
  if (dataType.startsWith('DECIMAL') || dataType.startsWith('NUMERIC') || dataType === 'REAL') return str;
  return `'${str.replace(/'/g, "''")}'`;
}

// Preview - parse CSV and return data in response (no temp file dependency)
app.post('/preview', upload.single('csvFile'), async (req, res) => {
  try {
    const filePath = req.file.path;
    const fileSize = req.file.size;
    console.log(`Preview: ${(fileSize / 1024 / 1024).toFixed(2)}MB`);
    
    let csvData;
    if (fileSize < 10 * 1024 * 1024) {
      csvData = fs.readFileSync(filePath, 'utf8');
    } else {
      const lines = [];
      const fileStream = fs.createReadStream(filePath);
      const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
      let lineCount = 0;
      for await (const line of rl) {
        lines.push(line);
        lineCount++;
        if (lineCount >= 2001) break;
      }
      csvData = lines.join('\n');
      rl.close();
      fileStream.destroy();
    }

    // Clean up temp file immediately
    try { fs.unlinkSync(filePath); } catch(e) {}
    
    const parsed = Papa.parse(csvData, { header: true, skipEmptyLines: true, dynamicTyping: false });
    
    if (parsed.errors.length > 0) {
      return res.status(400).json({ error: 'Error parsing CSV: ' + parsed.errors[0].message });
    }
    
    const rows = parsed.data;
    const headers = parsed.meta.fields;
    
    if (!headers || headers.length === 0) return res.status(400).json({ error: 'No columns found in CSV' });
    
    const columns = headers.map(header => {
      const columnValues = rows.map(row => row[header]);
      const detectedType = detectDataType(columnValues);
      const samples = columnValues.filter(v => v !== null && v !== undefined && v !== '').slice(0, 3);
      return { originalName: header, sanitizedName: sanitizeColumnName(header), detectedType, sampleValues: samples };
    });
    
    const validationResults = runValidation(rows, headers);
    
    res.json({
      columns,
      rowCount: rows.length,
      headers,    // passed back on convert
      rows,       // passed back on convert
      validation: validationResults,
      largeFile: rows.length > 10000
    });
    
  } catch (error) {
    console.error('Preview error:', error);
    if (req.file?.path) { try { fs.unlinkSync(req.file.path); } catch(e) {} }
    res.status(500).json({ error: 'Server error: ' + error.message });
  }
});

// Convert endpoint - accepts rows/headers as JSON (no temp file needed)
app.post('/convert', (req, res) => {
  try {
    const tableName = req.body.tableName || 'my_table';
    const dialect = req.body.dialect || 'postgresql';
    const typeOverrides = req.body.typeOverrides || {};
    const rows = req.body.rows;
    const headers = req.body.headers;

    if (!rows || !headers || rows.length === 0) {
      return res.status(400).json({ error: 'No data provided' });
    }

    const maxRows = Math.min(rows.length, 10000);
    const columnTypes = {};
    headers.forEach(header => {
      const sanitizedName = sanitizeColumnName(header);
      if (typeOverrides[sanitizedName]) {
        columnTypes[header] = convertToDialect(typeOverrides[sanitizedName], dialect);
      } else {
        const columnValues = rows.map(row => row[header]);
        const genericType = detectDataType(columnValues);
        columnTypes[header] = convertToDialect(genericType, dialect);
      }
    });
    
    const sanitizedTableName = sanitizeColumnName(tableName);
    let createTableSQL = `CREATE TABLE ${sanitizedTableName} (\n`;
    const columnDefinitions = headers.map(header => `  ${sanitizeColumnName(header)} ${columnTypes[header]}`);
    createTableSQL += columnDefinitions.join(',\n') + '\n)';
    if (dialect === 'mysql') createTableSQL += ' ENGINE=InnoDB DEFAULT CHARSET=utf8mb4';
    createTableSQL += ';';
    
    let insertSQL = '';
    const batchSize = 500;
    for (let i = 0; i < maxRows; i += batchSize) {
      const batch = rows.slice(i, Math.min(i + batchSize, maxRows));
      insertSQL += `INSERT INTO ${sanitizedTableName} (${headers.map(h => sanitizeColumnName(h)).join(', ')}) VALUES\n`;
      insertSQL += batch.map(row => `  (${headers.map(h => escapeSQLValue(row[h], columnTypes[h])).join(', ')})`).join(',\n') + ';\n\n';
    }
    
    if (rows.length > 10000) {
      insertSQL += `-- Note: Free tier limited to first 10,000 rows.\n-- Your file contains ${rows.length.toLocaleString()} rows.\n`;
    }
    
    res.json({
      createTable: createTableSQL,
      insert: insertSQL,
      rowCount: maxRows,
      columnCount: headers.length,
      dialect,
      truncated: rows.length > 10000,
      actualRows: rows.length
    });
    
  } catch (error) {
    console.error('Convert error:', error);
    res.status(500).json({ error: 'Server error: ' + error.message });
  }
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

if (require.main === module) {
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`🚀 CSV to SQL Converter running at http://localhost:${PORT}`);
  });
}

module.exports = app;
