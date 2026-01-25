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
  let maxLength = 0;
  
  for (const value of sample) {
    const str = String(value).trim();
    maxLength = Math.max(maxLength, str.length);
    if (!/^-?\d+$/.test(str)) allIntegers = false;
    if (!/^-?\d*\.?\d+$/.test(str)) allDecimals = false;
  }
  
  if (allIntegers) return 'INTEGER';
  if (allDecimals) return 'DECIMAL(10,2)';
  
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

function runValidation(rows, headers) {
  const results = { passed: [], warnings: [], errors: [] };
  
  const sampleSize = Math.min(rows.length, 1000);
  const sample = rows.slice(0, sampleSize);
  
  const expectedColumns = headers.length;
  let inconsistentRows = 0;
  sample.forEach(row => {
    if (Object.keys(row).length !== expectedColumns) inconsistentRows++;
  });
  
  if (inconsistentRows === 0) {
    results.passed.push({ type: 'consistency', message: 'All rows have consistent column count' });
  } else {
    results.warnings.push({ 
      type: 'consistency', 
      message: `${inconsistentRows} rows have inconsistent column counts (sampled ${sampleSize} rows)`, 
      severity: 'warning' 
    });
  }
  
  // Check for duplicates in sample
  headers.forEach(header => {
    const values = sample.map(row => row[header]);
    const nonEmptyValues = values.filter(v => v !== null && v !== undefined && v !== '');
    const uniqueValues = new Set(nonEmptyValues);
    const duplicateCount = nonEmptyValues.length - uniqueValues.size;
    
    if (duplicateCount > 0) {
      results.warnings.push({ 
        type: 'duplicates', 
        column: header, 
        message: `Column "${header}" has ${duplicateCount} duplicate values in sample`, 
        severity: 'warning' 
      });
    }
    
    const nullCount = values.filter(v => v === null || v === undefined || v === '').length;
    const nullPercentage = ((nullCount / values.length) * 100).toFixed(1);
    
    if (nullCount > 0 && nullPercentage > 50) {
      results.warnings.push({ 
        type: 'nulls', 
        column: header, 
        message: `Column "${header}" has ${nullPercentage}% NULL/empty values`, 
        severity: 'warning' 
      });
    }
  });
  
  if (results.warnings.length === 0 && results.errors.length === 0) {
    results.passed.push({ type: 'overall', message: 'No data quality issues detected! 🎉' });
  }
  
  return results;
}

function escapeSQLValue(value, dataType) {
  if (value === null || value === undefined || value === '') return 'NULL';
  const str = String(value).trim();
  if (dataType === 'INTEGER' || dataType === 'INT' || dataType === 'NUMBER') return str;
  if (dataType.startsWith('DECIMAL') || dataType.startsWith('NUMERIC') || dataType === 'REAL') return str;
  return `'${str.replace(/'/g, "''")}'`;
}

// FIXED Preview - reads complete rows, not partial buffer
app.post('/preview', upload.single('csvFile'), async (req, res) => {
  try {
    const filePath = req.file.path;
    const fileSize = req.file.size;
    
    console.log(`Preview: ${(fileSize / 1024 / 1024).toFixed(2)}MB`);
    
    // Read entire file for files under 10MB, otherwise read first 2000 complete rows
    let csvData;
    
    if (fileSize < 10 * 1024 * 1024) {
      // Small file - read all
      csvData = fs.readFileSync(filePath, 'utf8');
    } else {
      // Large file - read first N complete lines
      const lines = [];
      const fileStream = fs.createReadStream(filePath);
      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });
      
      let lineCount = 0;
      for await (const line of rl) {
        lines.push(line);
        lineCount++;
        if (lineCount >= 2001) break; // Header + 2000 rows
      }
      
      csvData = lines.join('\n');
      rl.close();
      fileStream.destroy();
    }
    
    const parsed = Papa.parse(csvData, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      preview: 2000
    });
    
    if (parsed.errors.length > 0) {
      fs.unlinkSync(filePath);
      return res.status(400).json({ error: 'Error parsing CSV: ' + parsed.errors[0].message });
    }
    
    const rows = parsed.data;
    const headers = parsed.meta.fields;
    
    if (!headers || headers.length === 0) {
      fs.unlinkSync(filePath);
      return res.status(400).json({ error: 'No columns found in CSV' });
    }
    
    // Fast column analysis
    const columns = headers.map(header => {
      const columnValues = rows.map(row => row[header]).slice(0, 100);
      const detectedType = detectDataType(columnValues);
      const samples = columnValues.filter(v => v !== null && v !== undefined && v !== '').slice(0, 3);
      
      return {
        originalName: header,
        sanitizedName: sanitizeColumnName(header),
        detectedType: detectedType,
        sampleValues: samples
      };
    });
    
    const validationResults = runValidation(rows, headers);
    
    // Get actual row count from file
    const totalLines = csvData.split('\n').length - 1; // Subtract header
    
    // IMPORTANT: Don't delete temp file - we need it for conversion
    // fs.unlinkSync(filePath); // REMOVED - keep file for convert endpoint
    
    res.json({
      columns: columns,
      rowCount: totalLines,
      rowsAnalyzed: rows.length,
      tempFilePath: path.basename(filePath),
      validation: validationResults,
      largeFile: totalLines > 10000
    });
    
  } catch (error) {
    console.error('Preview error:', error);
    if (req.file?.path) {
      try { fs.unlinkSync(req.file.path); } catch(e) {}
    }
    res.status(500).json({ error: 'Server error: ' + error.message });
  }
});

// Convert endpoint - limit to 10k rows for free tier
app.post('/convert', upload.single('csvFile'), (req, res) => {
  try {
    const tableName = req.body.tableName || 'my_table';
    const dialect = req.body.dialect || 'postgresql';
    const typeOverrides = req.body.typeOverrides ? JSON.parse(req.body.typeOverrides) : {};
    
    let filePath;
    if (req.body.tempFilePath) {
      filePath = path.join('/tmp', req.body.tempFilePath);
    } else if (req.file) {
      filePath = req.file.path;
    } else {
      return res.status(400).json({ error: 'No file provided' });
    }
    
    const fileSize = fs.statSync(filePath).size;
    console.log(`Convert: ${(fileSize / 1024 / 1024).toFixed(2)}MB`);
    
    const csvData = fs.readFileSync(filePath, 'utf8');
    
    const parsed = Papa.parse(csvData, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false
    });
    
    if (parsed.errors.length > 0) {
      fs.unlinkSync(filePath);
      return res.status(400).json({ error: 'Error parsing CSV: ' + parsed.errors[0].message });
    }
    
    const rows = parsed.data;
    const headers = parsed.meta.fields;
    
    const columnTypes = {};
    headers.forEach(header => {
      const sanitizedName = sanitizeColumnName(header);
      if (typeOverrides[sanitizedName]) {
        columnTypes[header] = convertToDialect(typeOverrides[sanitizedName], dialect);
      } else {
        const columnValues = rows.map(row => row[header]).slice(0, 100);
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
    const batchSize = 500;
    const maxRows = Math.min(rows.length, 10000); // Hard limit for free tier
    
    for (let i = 0; i < maxRows; i += batchSize) {
      const batch = rows.slice(i, Math.min(i + batchSize, maxRows));
      insertSQL += `INSERT INTO ${sanitizedTableName} (${headers.map(h => sanitizeColumnName(h)).join(', ')}) VALUES\n`;
      
      const valueRows = batch.map(row => {
        const values = headers.map(header => escapeSQLValue(row[header], columnTypes[header]));
        return `  (${values.join(', ')})`;
      });
      
      insertSQL += valueRows.join(',\n') + ';\n\n';
    }
    
    if (rows.length > 10000) {
      insertSQL += `-- Note: Free tier limited to first 10,000 rows.\n-- Your file contains ${rows.length.toLocaleString()} rows.\n-- Upgrade to Pro for files up to 500,000 rows.\n`;
    }
    
    fs.unlinkSync(filePath);
    
    res.json({
      createTable: createTableSQL,
      insert: insertSQL,
      rowCount: Math.min(rows.length, 10000),
      columnCount: headers.length,
      dialect: dialect,
      truncated: rows.length > 10000,
      actualRows: rows.length
    });
    
  } catch (error) {
    console.error('Convert error:', error);
    if (req.file?.path) {
      try { fs.unlinkSync(req.file.path); } catch(e) {}
    }
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
