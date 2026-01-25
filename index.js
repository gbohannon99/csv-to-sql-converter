const express = require('express');
const multer = require('multer');
const Papa = require('papaparse');
const fs = require('fs');
const path = require('path');

const app = express();

// Increase payload limits for large files
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

// Configure multer with size limit
const upload = multer({ 
  dest: '/tmp/',
  limits: {
    fileSize: 50 * 1024 * 1024 // 50MB limit
  }
});

app.use(express.static('public'));

// Helper functions
function detectDataType(values) {
  const validValues = values.filter(v => v !== null && v !== undefined && v !== '');
  if (validValues.length === 0) return 'VARCHAR(255)';
  
  // Sample only first 1000 values for large datasets
  const sampleSize = Math.min(validValues.length, 1000);
  const sample = validValues.slice(0, sampleSize);
  
  let allIntegers = true;
  let allDecimals = true;
  let allDates = true;
  let maxLength = 0;
  
  for (const value of sample) {
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
  
  // Limit validation to first 10,000 rows for large datasets
  const sampleSize = Math.min(rows.length, 10000);
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
      message: `${inconsistentRows} rows have inconsistent column counts${sampleSize < rows.length ? ' (sampled first 10k rows)' : ''}`, 
      severity: 'warning' 
    });
  }
  
  headers.forEach(header => {
    const values = sample.map(row => row[header]);
    const nonEmptyValues = values.filter(v => v !== null && v !== undefined && v !== '');
    const uniqueValues = new Set(nonEmptyValues);
    const duplicateCount = nonEmptyValues.length - uniqueValues.size;
    
    if (duplicateCount > 0) {
      results.warnings.push({ 
        type: 'duplicates', 
        column: header, 
        message: `Column "${header}" has ${duplicateCount} duplicate values${sampleSize < rows.length ? ' (in sample)' : ''}`, 
        severity: 'warning' 
      });
    }
    
    const nullCount = values.filter(v => v === null || v === undefined || v === '').length;
    const nullPercentage = ((nullCount / values.length) * 100).toFixed(1);
    
    if (nullCount > 0 && nullPercentage > 50) {
      results.warnings.push({ 
        type: 'nulls', 
        column: header, 
        message: `Column "${header}" has ${nullCount} NULL/empty values (${nullPercentage}%)`, 
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

// Preview endpoint - optimized for large files
app.post('/preview', upload.single('csvFile'), (req, res) => {
  try {
    const filePath = req.file.path;
    const fileSize = req.file.size;
    
    // For files over 10MB, warn about potential slowness
    if (fileSize > 10 * 1024 * 1024) {
      console.log(`Large file detected: ${(fileSize / 1024 / 1024).toFixed(2)}MB`);
    }
    
    const csvData = fs.readFileSync(filePath, 'utf8');
    
    const parsed = Papa.parse(csvData, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      preview: 10000 // Only parse first 10k rows for preview
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
    
    if (rows.length === 0) {
      fs.unlinkSync(filePath);
      return res.status(400).json({ error: 'No data rows found in CSV' });
    }
    
    // Analyze columns (sample-based for large files)
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
    
    // Get actual row count from file
    const lineCount = csvData.split('\n').length - 1; // Subtract header row
    
    res.json({
      columns: columns,
      rowCount: lineCount,
      rowsAnalyzed: rows.length,
      tempFilePath: path.basename(filePath),
      validation: validationResults,
      largeFile: lineCount > 10000
    });
    
  } catch (error) {
    console.error('Preview error:', error);
    if (req.file?.path) {
      try { fs.unlinkSync(req.file.path); } catch(e) {}
    }
    res.status(500).json({ error: 'Server error: ' + error.message });
  }
});

// Convert endpoint - optimized with streaming for large files
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
    console.log(`Converting file: ${(fileSize / 1024 / 1024).toFixed(2)}MB`);
    
    const csvData = fs.readFileSync(filePath, 'utf8');
    
    // For very large files, parse in chunks
    const isLargeFile = fileSize > 5 * 1024 * 1024; // 5MB
    
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
    
    // Detect column types
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
    
    // Generate CREATE TABLE
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
    
    // Generate INSERT statements in batches
    let insertSQL = '';
    const batchSize = isLargeFile ? 500 : 100; // Larger batches for big files
    
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      insertSQL += `INSERT INTO ${sanitizedTableName} (${headers.map(h => sanitizeColumnName(h)).join(', ')}) VALUES\n`;
      
      const valueRows = batch.map(row => {
        const values = headers.map(header => escapeSQLValue(row[header], columnTypes[header]));
        return `  (${values.join(', ')})`;
      });
      
      insertSQL += valueRows.join(',\n') + ';\n\n';
      
      // For very large files, limit to first 50k rows and warn user
      if (i > 50000 && isLargeFile) {
        insertSQL += `-- Note: Showing first 50,000 rows only.\n-- File contains ${rows.length} total rows.\n-- Consider splitting into multiple files for better performance.\n`;
        break;
      }
    }
    
    fs.unlinkSync(filePath);
    
    res.json({
      createTable: createTableSQL,
      insert: insertSQL,
      rowCount: rows.length,
      columnCount: headers.length,
      dialect: dialect,
      truncated: rows.length > 50000
    });
    
  } catch (error) {
    console.error('Convert error:', error);
    if (req.file?.path) {
      try { fs.unlinkSync(req.file.path); } catch(e) {}
    }
    res.status(500).json({ error: 'Server error: ' + error.message });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
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
