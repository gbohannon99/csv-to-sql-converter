const express = require('express');
const multer = require('multer');
const Papa = require('papaparse');
const fs = require('fs');
const path = require('path');

const app = express();
const upload = multer({ dest: '/tmp/' });

app.use(express.json());
app.use(express.static('public'));

function detectDataType(values) {
  const validValues = values.filter(v => v !== null && v !== undefined && v !== '');
  if (validValues.length === 0) return 'VARCHAR(255)';
  
  let allIntegers = true;
  let allDecimals = true;
  let allDates = true;
  let maxLength = 0;
  
  for (const value of validValues) {
    const str = String(value).trim();
    maxLength = Math.max(maxLength, str.length);
    if (!/^-?\d+$/.test(str)) allIntegers = false;
    if (!/^-?\d*\.?\d+$/.test(str)) allDecimals = false;
    if (isNaN(Date.parse(str))) allDates = false;
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

function runValidation(rows, headers) {
  const results = { passed: [], warnings: [], errors: [] };
  
  const expectedColumns = headers.length;
  let inconsistentRows = 0;
  rows.forEach(row => {
    if (Object.keys(row).length !== expectedColumns) inconsistentRows++;
  });
  
  if (inconsistentRows === 0) {
    results.passed.push({ type: 'consistency', message: 'All rows have consistent column count' });
  } else {
    results.warnings.push({ type: 'consistency', message: `${inconsistentRows} rows have inconsistent column counts`, severity: 'warning' });
  }
  
  headers.forEach(header => {
    const values = rows.map(row => row[header]);
    const nonEmptyValues = values.filter(v => v !== null && v !== undefined && v !== '');
    const uniqueValues = new Set(nonEmptyValues);
    const duplicateCount = nonEmptyValues.length - uniqueValues.size;
    
    if (duplicateCount > 0) {
      results.warnings.push({ type: 'duplicates', column: header, message: `Column "${header}" has ${duplicateCount} duplicate values`, severity: 'warning' });
    }
    
    const nullCount = values.filter(v => v === null || v === undefined || v === '').length;
    const nullPercentage = ((nullCount / values.length) * 100).toFixed(1);
    
    if (nullCount > 0 && nullPercentage > 50) {
      results.warnings.push({ type: 'nulls', column: header, message: `Column "${header}" has ${nullCount} NULL/empty values (${nullPercentage}%)`, severity: 'warning' });
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

app.post('/preview', upload.single('csvFile'), (req, res) => {
  try {
    const csvData = fs.readFileSync(req.file.path, 'utf8');
    const parsed = Papa.parse(csvData, { header: true, skipEmptyLines: true, dynamicTyping: false });
    
    if (parsed.errors.length > 0) {
      fs.unlinkSync(req.file.path);
      return res.status(400).json({ error: 'Error parsing CSV: ' + parsed.errors[0].message });
    }
    
    const rows = parsed.data;
    const headers = parsed.meta.fields;
    
    if (!headers || headers.length === 0) {
      fs.unlinkSync(req.file.path);
      return res.status(400).json({ error: 'No columns found in CSV' });
    }
    
    const columns = headers.map(header => {
      const columnValues = rows.map(row => row[header]);
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
    
    res.json({
      columns: columns,
      rowCount: rows.length,
      tempFilePath: path.basename(req.file.path),
      validation: validationResults
    });
  } catch (error) {
    if (req.file?.path) fs.unlinkSync(req.file.path);
    res.status(500).json({ error: 'Server error: ' + error.message });
  }
});

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
    
    const csvData = fs.readFileSync(filePath, 'utf8');
    const parsed = Papa.parse(csvData, { header: true, skipEmptyLines: true, dynamicTyping: false });
    
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
      insertSQL += `INSERT INTO ${sanitizedTableName} (${headers.map(h => sanitizeColumnName(h)).join(', ')}) VALUES\n`;
      
      const valueRows = batch.map(row => {
        const values = headers.map(header => escapeSQLValue(row[header], columnTypes[header]));
        return `  (${values.join(', ')})`;
      });
      
      insertSQL += valueRows.join(',\n') + ';\n\n';
    }
    
    fs.unlinkSync(filePath);
    
    res.json({
      createTable: createTableSQL,
      insert: insertSQL,
      rowCount: rows.length,
      columnCount: headers.length,
      dialect: dialect
    });
  } catch (error) {
    if (req.file?.path) fs.unlinkSync(req.file.path);
    res.status(500).json({ error: 'Server error: ' + error.message });
  }
});

module.exports = app;
