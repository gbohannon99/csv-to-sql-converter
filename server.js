const express = require('express');
const multer = require('multer');
const Papa = require('papaparse');
const fs = require('fs');
const path = require('path');

const app = express();
const upload = multer({ dest: '/tmp/uploads/' }); // Vercel uses /tmp for temp files

// Parse JSON bodies - 50mb limit to handle CSV row data passed from preview
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

// Serve static files from public directory
app.use(express.static('public'));

// Function to detect SQL data type from sample values (generic type)
function detectDataType(values) {
  const validValues = values.filter(v => v !== null && v !== undefined && v !== '');
  
  if (validValues.length === 0) return 'VARCHAR(255)';
  
  let allIntegers = true;
  let allDecimals = true;
  let allDates = true;
  let maxLength = 0;
  
  // Strict date regex: YYYY-MM-DD with valid month/day ranges
  const strictDateRegex = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/;

  for (const value of validValues) {
    const str = String(value).trim();
    maxLength = Math.max(maxLength, str.length);
    
    if (!/^-?\d+$/.test(str)) allIntegers = false;
    if (!/^-?\d*\.?\d+$/.test(str)) allDecimals = false;
    if (!strictDateRegex.test(str)) allDates = false;
  }
  
  if (allIntegers) return 'INTEGER';
  if (allDecimals) return 'DECIMAL(10,2)';
  if (allDates) return 'DATE';
  
  // Default to VARCHAR with appropriate length
  const varcharSize = Math.min(Math.max(maxLength * 1.5, 50), 255);
  return `VARCHAR(${Math.ceil(varcharSize)})`;
}

// Function to convert generic data type to database-specific type
function convertToDialect(genericType, dialect) {
  const typeMap = {
    postgresql: {
      'INTEGER': 'INTEGER',
      'DECIMAL(10,2)': 'NUMERIC(10,2)',
      'DATE': 'DATE',
      'VARCHAR': 'VARCHAR'
    },
    mysql: {
      'INTEGER': 'INT',
      'DECIMAL(10,2)': 'DECIMAL(10,2)',
      'DATE': 'DATE',
      'VARCHAR': 'VARCHAR'
    },
    sqlserver: {
      'INTEGER': 'INT',
      'DECIMAL(10,2)': 'DECIMAL(10,2)',
      'DATE': 'DATE',
      'VARCHAR': 'VARCHAR'
    },
    sqlite: {
      'INTEGER': 'INTEGER',
      'DECIMAL(10,2)': 'REAL',
      'DATE': 'TEXT',
      'VARCHAR': 'TEXT'
    },
    oracle: {
      'INTEGER': 'NUMBER',
      'DECIMAL(10,2)': 'NUMBER(10,2)',
      'DATE': 'DATE',
      'VARCHAR': 'VARCHAR2'
    }
  };
  
  const map = typeMap[dialect] || typeMap.postgresql;
  
  // Handle VARCHAR with size
  if (genericType.startsWith('VARCHAR')) {
    const size = genericType.match(/\((\d+)\)/)?.[1] || '255';
    if (dialect === 'oracle') {
      return `VARCHAR2(${size})`;
    } else if (dialect === 'sqlite') {
      return 'TEXT';
    }
    return `VARCHAR(${size})`;
  }
  
  return map[genericType] || genericType;
}

// Function to sanitize column names for SQL
function sanitizeColumnName(name) {
  return name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/^(\d)/, '_$1') // Can't start with number
    .substring(0, 64); // Max column name length
}

// Preview endpoint - analyze CSV and return column info
app.post('/preview', upload.fields([
  { name: 'csvFile', maxCount: 1 },
  { name: 'schemaFile', maxCount: 1 }
]), (req, res) => {
  try {
    const csvFile = req.files['csvFile']?.[0];
    if (!csvFile) return res.status(400).json({ error: 'No CSV file provided' });

    const csvData = fs.readFileSync(csvFile.path, 'utf8');
    try { fs.unlinkSync(csvFile.path); } catch(e) {}

    // Parse optional schema file
    let schema = null;
    let schemaError = null;
    const schemaFile = req.files['schemaFile']?.[0];
    if (schemaFile) {
      const schemaText = fs.readFileSync(schemaFile.path, 'utf8');
      try { fs.unlinkSync(schemaFile.path); } catch(e) {}
      const parsed = parseSchema(schemaText);
      if (parsed.error) schemaError = parsed.error;
      else schema = parsed;
    }

    const parsed = Papa.parse(csvData, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false
    });

    if (parsed.errors.length > 0) {
      return res.status(400).json({ error: 'Error parsing CSV: ' + parsed.errors[0].message });
    }

    const rows = parsed.data;
    const headers = parsed.meta.fields;

    if (!headers || headers.length === 0) return res.status(400).json({ error: 'No columns found in CSV' });
    if (rows.length === 0) return res.status(400).json({ error: 'No data rows found in CSV' });

    const columns = headers.map(header => {
      const columnValues = rows.map(row => row[header]);
      const detectedType = detectDataType(columnValues);
      const samples = columnValues.filter(v => v !== null && v !== undefined && v !== '').slice(0, 3);
      return {
        originalName: header,
        sanitizedName: sanitizeColumnName(header),
        detectedType,
        sampleValues: samples
      };
    });

    const validationResults = runValidation(rows, headers);

    // Run schema validation if schema was provided
    if (schema) {
      const schemaResults = runSchemaValidation(rows, headers, schema);
      validationResults.schemaErrors = schemaResults.errors;
      validationResults.schemaWarnings = schemaResults.warnings;
      validationResults.schemaPassed = schemaResults.passed;
      validationResults.schemaLoaded = true;
      validationResults.schemaTable = schema.table || null;
    }
    if (schemaError) {
      validationResults.schemaError = schemaError;
    }

    res.json({
      columns,
      rowCount: rows.length,
      headers,
      rows,
      validation: validationResults
    });

  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Server error: ' + error.message });
  }
});

// ── Phase 1 helpers ──────────────────────────────────────────────────────────

// Detect leading/trailing whitespace issues in a column's values
function checkWhitespace(values, header) {
  const issues = values.filter(v => v !== null && v !== undefined && v !== '' && String(v) !== String(v).trim());
  return issues.length > 0 ? {
    type: 'whitespace',
    column: header,
    message: `Column "${header}" has ${issues.length} value(s) with leading/trailing whitespace`,
    details: `Examples: ${issues.slice(0, 3).map(v => `"${v}"`).join(', ')}`,
    severity: 'warning',
    fixable: true,
    fixType: 'trim'
  } : null;
}

// Detect duplicate headers
function checkDuplicateHeaders(headers) {
  const seen = {};
  const dupes = [];
  headers.forEach(h => {
    const lower = h.toLowerCase().trim();
    seen[lower] = (seen[lower] || 0) + 1;
    if (seen[lower] === 2) dupes.push(h);
  });
  return dupes;
}

// Nullability check: flag columns with any missing values
function checkNulls(values, header) {
  const nullCount = values.filter(v => v === null || v === undefined || v === '').length;
  if (nullCount === 0) return null;
  const pct = ((nullCount / values.length) * 100).toFixed(1);
  return {
    type: 'nulls',
    column: header,
    message: `Column "${header}" has ${nullCount} NULL/empty value(s) (${pct}%)`,
    severity: pct > 50 ? 'warning' : 'info'
  };
}

// Boolean normalization check
const BOOL_SETS = [
  new Set(['true','false']),
  new Set(['0','1']),
  new Set(['y','n']),
  new Set(['yes','no']),
  new Set(['t','f']),
];

function checkBoolean(values, header) {
  const nonEmpty = values.filter(v => v !== null && v !== undefined && v !== '').map(v => String(v).toLowerCase().trim());
  if (nonEmpty.length === 0) return null;
  const unique = new Set(nonEmpty);

  // Must have more than 2 unique values to be "mixed" — a clean true/false column is fine
  if (unique.size <= 2) return null;

  // Check if ALL unique values belong to any single bool vocabulary
  const matchedSets = BOOL_SETS.filter(s => [...unique].every(v => s.has(v)));
  // If no single set covers all values, check if values span MULTIPLE bool sets (mixed representations)
  const anyBoolValue = [...unique].every(v => BOOL_SETS.some(s => s.has(v)));
  if (!anyBoolValue) return null; // not a boolean column at all

  return {
    type: 'boolean',
    column: header,
    message: `Column "${header}" appears boolean but has mixed representations`,
    details: `Found values: ${[...unique].slice(0, 6).map(v => `"${v}"`).join(', ')}`,
    severity: 'warning',
    fixable: true,
    fixType: 'normalize_bool'
  };
}

// Strict date formats — validate month and day ranges, not just shape
const DATE_FORMATS = [
  { regex: /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/, name: 'YYYY-MM-DD' },
  { regex: /^(0[1-9]|[12]\d|3[01])\/(0[1-9]|1[0-2])\/\d{4}$/, name: 'DD/MM/YYYY' },
  { regex: /^\d{1,2}\/\d{1,2}\/\d{4}$/, name: 'M/D/YYYY' },
  { regex: /^(0[1-9]|[12]\d|3[01])-(0[1-9]|1[0-2])-\d{4}$/, name: 'DD-MM-YYYY' },
  { regex: /^\d{4}\/(0[1-9]|1[0-2])\/(0[1-9]|[12]\d|3[01])$/, name: 'YYYY/MM/DD' },
];

// Loose pattern: looks date-like by shape but may have invalid month/day numbers
const looseDatePattern = /^\d{4}-\d{2}-\d{2}$|^\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}$|^\d{4}[\/]\d{2}[\/]\d{2}$/;

function checkDateFormats(values, header) {
  const nonEmpty = values.filter(v => v !== null && v !== undefined && v !== '');
  if (nonEmpty.length === 0) return [];

  // Count how many look date-like by loose shape
  const looseLike = nonEmpty.filter(v => looseDatePattern.test(String(v).trim())).length;
  if (looseLike < nonEmpty.length * 0.4) return []; // not a date column

  const formatCounts = {};
  const invalidValues = []; // matches loose pattern but fails strict validation

  nonEmpty.forEach(v => {
    const str = String(v).trim();
    if (!looseDatePattern.test(str)) return; // not date-shaped at all
    const match = DATE_FORMATS.find(f => f.regex.test(str));
    if (match) {
      formatCounts[match.name] = (formatCounts[match.name] || 0) + 1;
    } else {
      invalidValues.push(str); // date-shaped but invalid month/day numbers
    }
  });

  const issues = [];
  const formatNames = Object.keys(formatCounts);

  if (formatNames.length > 1) {
    issues.push({
      type: 'mixed_date_format',
      column: header,
      message: `Column "${header}" has mixed date formats`,
      details: `Formats found: ${formatNames.map(f => `${f} (${formatCounts[f]})`).join(', ')}`,
      severity: 'warning'
    });
  }

  if (invalidValues.length > 0) {
    issues.push({
      type: 'invalid_date',
      column: header,
      message: `Column "${header}" has ${invalidValues.length} invalid date value(s)`,
      details: `Examples: ${invalidValues.slice(0, 3).map(v => `"${v}"`).join(', ')}`,
      severity: 'error'
    });
  }

  return issues;
}

// ── Phase 2 helpers ──────────────────────────────────────────────────────────

// Max string length check — warn if values will be truncated by detected VARCHAR size
function checkStringLength(values, header, detectedType) {
  if (!detectedType.startsWith('VARCHAR')) return null;
  const sizeMatch = detectedType.match(/\((\d+)\)/);
  if (!sizeMatch) return null;
  const maxLen = parseInt(sizeMatch[1]);

  const overLimit = values.filter(v => v !== null && v !== undefined && v !== '' && String(v).length > maxLen);
  if (overLimit.length === 0) return null;

  const worst = overLimit.reduce((a, b) => String(a).length > String(b).length ? a : b);
  return {
    type: 'string_length',
    column: header,
    message: `Column "${header}" has ${overLimit.length} value(s) exceeding detected ${detectedType} limit (${maxLen} chars)`,
    details: `Longest value: ${String(worst).length} chars — "${String(worst).substring(0, 60)}${String(worst).length > 60 ? '...' : ''}"`,
    severity: 'warning'
  };
}

// Primary key uniqueness check — flag columns that look like PKs but have duplicates
function checkPrimaryKey(values, header) {
  // Only run on columns whose name suggests a PK
  const pkPattern = /^(id|.*_id|.*_key|.*_pk|record_id|uuid|guid)$/i;
  if (!pkPattern.test(header.trim())) return null;

  const nonEmpty = values.filter(v => v !== null && v !== undefined && v !== '');
  if (nonEmpty.length === 0) return null;

  const seen = new Set();
  const dupes = [];
  nonEmpty.forEach((v, idx) => {
    const key = String(v).trim();
    if (seen.has(key)) dupes.push(key);
    else seen.add(key);
  });

  if (dupes.length === 0) return null;

  const examples = [...new Set(dupes)].slice(0, 3).map(v => `"${v}"`).join(', ');
  return {
    type: 'pk_uniqueness',
    column: header,
    message: `Column "${header}" looks like a primary key but has ${dupes.length} duplicate value(s)`,
    details: `Duplicate values: ${examples}`,
    severity: 'error'
  };
}


// ── Phase 3: Schema validation ────────────────────────────────────────────────

function parseSchema(schemaText) {
  try {
    const schema = JSON.parse(schemaText);
    if (!schema.columns || !Array.isArray(schema.columns)) {
      return { error: 'Schema must have a "columns" array' };
    }
    // Normalise column names to lowercase trimmed
    schema.columns = schema.columns.map(col => ({
      ...col,
      name: col.name.trim().toLowerCase()
    }));
    return schema;
  } catch (e) {
    return { error: 'Invalid JSON: ' + e.message };
  }
}

function runSchemaValidation(rows, headers, schema) {
  const issues = { errors: [], warnings: [], passed: [] };
  const csvCols = new Set(headers.map(h => h.trim().toLowerCase()));
  const schemaCols = new Map(schema.columns.map(c => [c.name, c]));

  // 1. Missing columns — in schema but not in CSV
  schema.columns.forEach(col => {
    if (!csvCols.has(col.name)) {
      issues.errors.push({
        type: 'schema_missing_column',
        message: `Required column "${col.name}" is in schema but missing from CSV`,
        severity: 'error'
      });
    }
  });

  // 2. Extra columns — in CSV but not in schema
  headers.forEach(h => {
    const norm = h.trim().toLowerCase();
    if (!schemaCols.has(norm)) {
      issues.warnings.push({
        type: 'schema_extra_column',
        message: `Column "${h}" is in CSV but not defined in schema`,
        severity: 'warning'
      });
    }
  });

  // Per-column schema checks
  headers.forEach(header => {
    const norm = header.trim().toLowerCase();
    const col = schemaCols.get(norm);
    if (!col) return; // extra column, already flagged

    const values = rows.map(row => row[header]);
    const nonEmpty = values.filter(v => v !== null && v !== undefined && v !== '');

    // 3. Nullable check (schema-driven, more precise than Phase 1)
    if (col.nullable === false) {
      const nullCount = values.length - nonEmpty.length;
      if (nullCount > 0) {
        issues.errors.push({
          type: 'schema_not_nullable',
          column: header,
          message: `Column "${header}" is NOT NULL in schema but has ${nullCount} empty value(s)`,
          severity: 'error'
        });
      }
    }

    // 4. Type compatibility check
    if (col.type) {
      const schemaType = col.type.toUpperCase();
      const detectedType = detectDataType(values).toUpperCase();

      const typeCompatible = () => {
        if (schemaType === 'INTEGER' || schemaType === 'INT' || schemaType === 'BIGINT') {
          return detectedType === 'INTEGER';
        }
        if (schemaType === 'DECIMAL' || schemaType === 'NUMERIC' || schemaType === 'FLOAT' || schemaType === 'REAL') {
          return detectedType === 'INTEGER' || detectedType.startsWith('DECIMAL');
        }
        if (schemaType === 'DATE' || schemaType === 'DATETIME' || schemaType === 'TIMESTAMP') {
          return detectedType === 'DATE';
        }
        if (schemaType === 'VARCHAR' || schemaType === 'TEXT' || schemaType === 'CHAR') {
          return true; // anything can be a string
        }
        if (schemaType === 'BOOLEAN') {
          const uniqueVals = new Set(nonEmpty.map(v => String(v).toLowerCase().trim()));
          return [...uniqueVals].every(v => BOOL_SETS.some(s => s.has(v)));
        }
        return true;
      };

      if (!typeCompatible()) {
        issues.errors.push({
          type: 'schema_type_mismatch',
          column: header,
          message: `Column "${header}" schema type is ${col.type} but data looks like ${detectedType}`,
          details: `Check that the column contains the right data for a ${col.type} field`,
          severity: 'error'
        });
      }
    }

    // 5. maxLength check (schema-driven, exact limit)
    if (col.maxLength && (col.type || '').toUpperCase().includes('VARCHAR') || col.maxLength) {
      const overLimit = nonEmpty.filter(v => String(v).length > col.maxLength);
      if (overLimit.length > 0) {
        const worst = overLimit.reduce((a, b) => String(a).length > String(b).length ? a : b);
        issues.errors.push({
          type: 'schema_max_length',
          column: header,
          message: `Column "${header}" has ${overLimit.length} value(s) exceeding schema maxLength of ${col.maxLength}`,
          details: `Longest: ${String(worst).length} chars — "${String(worst).substring(0, 60)}${String(worst).length > 60 ? '...' : ''}"`,
          severity: 'error'
        });
      }
    }

    // 6. Numeric range check (min/max)
    if (col.min !== undefined || col.max !== undefined) {
      const numericVals = nonEmpty.map(v => parseFloat(String(v).replace(/,/g, '')));
      const validNums = numericVals.filter(n => !isNaN(n));

      if (validNums.length > 0) {
        const outOfRange = [];
        validNums.forEach((n, i) => {
          if ((col.min !== undefined && n < col.min) || (col.max !== undefined && n > col.max)) {
            outOfRange.push(n);
          }
        });

        if (outOfRange.length > 0) {
          const range = [col.min !== undefined ? `min: ${col.min}` : null, col.max !== undefined ? `max: ${col.max}` : null].filter(Boolean).join(', ');
          issues.errors.push({
            type: 'schema_range',
            column: header,
            message: `Column "${header}" has ${outOfRange.length} value(s) outside allowed range (${range})`,
            details: `Examples: ${outOfRange.slice(0, 3).join(', ')}`,
            severity: 'error'
          });
        }
      }
    }

    // 7. Primary key uniqueness (schema-driven)
    if (col.primaryKey === true) {
      const seen = new Set();
      const dupes = [];
      nonEmpty.forEach(v => {
        const key = String(v).trim();
        if (seen.has(key)) dupes.push(key);
        else seen.add(key);
      });
      if (dupes.length > 0) {
        issues.errors.push({
          type: 'schema_pk_uniqueness',
          column: header,
          message: `Column "${header}" is the primary key but has ${dupes.length} duplicate value(s)`,
          details: `Duplicates: ${[...new Set(dupes)].slice(0, 3).map(v => `"${v}"`).join(', ')}`,
          severity: 'error'
        });
      } else {
        issues.passed.push({ type: 'schema_pk_ok', message: `Primary key "${header}" — all values unique ✓` });
      }
    }
  });

  if (issues.errors.length === 0 && issues.warnings.length === 0) {
    issues.passed.push({ type: 'schema_overall', message: 'All schema constraints satisfied! 🎉' });
  }

  return issues;
}


function buildSummaryReport(rows, headers, allIssues) {
  const colSummary = {};
  allIssues.forEach(issue => {
    if (issue.column) {
      if (!colSummary[issue.column]) colSummary[issue.column] = [];
      colSummary[issue.column].push(issue.type);
    }
  });

  const badRows = [];
  rows.slice(0, 5000).forEach((row, idx) => {
    const problems = [];
    headers.forEach(h => {
      const v = row[h];
      if (v === null || v === undefined || v === '') problems.push(`${h}: NULL`);
      else if (String(v) !== String(v).trim()) problems.push(`${h}: whitespace`);
    });
    if (problems.length >= Math.ceil(headers.length * 0.3)) {
      badRows.push({ rowNum: idx + 2, problems: problems.slice(0, 4) });
    }
  });

  return { colSummary, badRows: badRows.slice(0, 5) };
}

// ── Main validation ───────────────────────────────────────────────────────────
function runValidation(rows, headers) {
  const results = {
    passed: [],
    warnings: [],
    errors: [],
    info: [],
    summary: null
  };

  // 1. Duplicate headers
  const dupeHeaders = checkDuplicateHeaders(headers);
  if (dupeHeaders.length > 0) {
    results.errors.push({
      type: 'duplicate_headers',
      message: `Duplicate column headers detected: ${dupeHeaders.map(h => `"${h}"`).join(', ')}`,
      severity: 'error'
    });
  } else {
    results.passed.push({ type: 'headers', message: 'No duplicate column headers' });
  }

  // 2. Row consistency
  const expectedColumns = headers.length;
  let inconsistentRows = 0;
  rows.forEach(row => { if (Object.keys(row).length !== expectedColumns) inconsistentRows++; });
  if (inconsistentRows === 0) {
    results.passed.push({ type: 'consistency', message: 'All rows have consistent column count' });
  } else {
    results.warnings.push({ type: 'consistency', message: `${inconsistentRows} row(s) have inconsistent column counts`, severity: 'warning' });
  }

  const allIssues = [];

  headers.forEach(header => {
    const values = rows.map(row => row[header]);
    const nonEmpty = values.filter(v => v !== null && v !== undefined && v !== '');

    // 3. Whitespace
    const wsIssue = checkWhitespace(nonEmpty, header);
    if (wsIssue) { results.warnings.push(wsIssue); allIssues.push(wsIssue); }

    // 4. Nullability
    const nullIssue = checkNulls(values, header);
    if (nullIssue) {
      if (nullIssue.severity === 'warning') results.warnings.push(nullIssue);
      else results.info.push(nullIssue);
      allIssues.push(nullIssue);
    }

    // 5. Boolean consistency
    const boolIssue = checkBoolean(nonEmpty, header);
    if (boolIssue) { results.warnings.push(boolIssue); allIssues.push(boolIssue); }

    // 6. Date format / invalid dates
    const dateIssues = checkDateFormats(nonEmpty, header);
    dateIssues.forEach(issue => {
      if (issue.severity === 'error') results.errors.push(issue);
      else results.warnings.push(issue);
      allIssues.push(issue);
    });

    // 7. String length vs detected VARCHAR size
    const detectedType = detectDataType(values);
    const lenIssue = checkStringLength(nonEmpty, header, detectedType);
    if (lenIssue) { results.warnings.push(lenIssue); allIssues.push(lenIssue); }

    // 8. Primary key uniqueness
    const pkIssue = checkPrimaryKey(values, header);
    if (pkIssue) { results.errors.push(pkIssue); allIssues.push(pkIssue); }
  });

  // 7. Build summary report
  results.summary = buildSummaryReport(rows, headers, allIssues);

  if (results.warnings.length === 0 && results.errors.length === 0) {
    results.passed.push({ type: 'overall', message: 'No data quality issues detected! 🎉' });
  }

  return results;
}

// Function to escape SQL values
function escapeSQLValue(value, dataType) {
  if (value === null || value === undefined || value === '') {
    return 'NULL';
  }
  
  const str = String(value).trim();
  
  if (dataType === 'INTEGER' || dataType === 'INT' || dataType === 'NUMBER') {
    return str;
  }
  
  if (dataType.startsWith('DECIMAL') || dataType.startsWith('NUMERIC') || dataType === 'REAL') {
    return str;
  }
  
  return `'${str.replace(/'/g, "''")}'`;
}

// Main endpoint to convert CSV to SQL
app.post('/convert', (req, res) => {
  try {
    const tableName = req.body.tableName || 'my_table';
    const dialect = req.body.dialect || 'postgresql';
    const typeOverrides = req.body.typeOverrides ? JSON.parse(req.body.typeOverrides) : {};
    const rows = req.body.rows;
    const headers = req.body.headers;

    if (!rows || !headers || rows.length === 0) {
      return res.status(400).json({ error: 'No data provided' });
    }

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
    const columnDefinitions = headers.map(header => {
      const sanitizedCol = sanitizeColumnName(header);
      const dataType = columnTypes[header];
      return `  ${sanitizedCol} ${dataType}`;
    });
    createTableSQL += columnDefinitions.join(',\n');
    createTableSQL += '\n)';
    if (dialect === 'mysql') createTableSQL += ' ENGINE=InnoDB DEFAULT CHARSET=utf8mb4';
    createTableSQL += ';';
    
    let insertSQL = '';
    const batchSize = 100;
    
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      insertSQL += `INSERT INTO ${sanitizedTableName} (`;
      insertSQL += headers.map(h => sanitizeColumnName(h)).join(', ');
      insertSQL += ') VALUES\n';
      const valueRows = batch.map(row => {
        const values = headers.map(header => escapeSQLValue(row[header], columnTypes[header]));
        return `  (${values.join(', ')})`;
      });
      insertSQL += valueRows.join(',\n');
      insertSQL += ';\n\n';
    }
    
    res.json({
      createTable: createTableSQL,
      insert: insertSQL,
      rowCount: rows.length,
      columnCount: headers.length,
      dialect: dialect
    });
    
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Server error: ' + error.message });
  }
});

// For local development
if (require.main === module) {
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`🚀 CSV to SQL Converter running at http://localhost:${PORT}`);
  });
}

// Export for Vercel
module.exports = app;
