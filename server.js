const express = require('express');
const multer = require('multer');
const Papa = require('papaparse');
const fs = require('fs');
const path = require('path');

const app = express();
const upload = multer({ dest: 'uploads/' });

// Parse JSON bodies
app.use(express.json());

// Serve static files from public directory
app.use(express.static('public'));

// Function to detect SQL data type from sample values (generic type)
function detectDataType(values) {
  // Remove null/empty values
  const validValues = values.filter(v => v !== null && v !== undefined && v !== '');
  
  if (validValues.length === 0) return 'VARCHAR(255)';
  
  let allIntegers = true;
  let allDecimals = true;
  let allDates = true;
  let maxLength = 0;
  
  for (const value of validValues) {
    const str = String(value).trim();
    maxLength = Math.max(maxLength, str.length);
    
    // Check if integer
    if (!/^-?\d+$/.test(str)) {
      allIntegers = false;
    }
    
    // Check if decimal/float
    if (!/^-?\d*\.?\d+$/.test(str)) {
      allDecimals = false;
    }
    
    // Check if date
    if (isNaN(Date.parse(str))) {
      allDates = false;
    }
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
app.post('/preview', upload.single('csvFile'), (req, res) => {
  try {
    const filePath = req.file.path;
    
    // Read the uploaded file
    const csvData = fs.readFileSync(filePath, 'utf8');
    
    // Parse CSV
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
    
    if (!headers || headers.length === 0) {
      fs.unlinkSync(filePath);
      return res.status(400).json({ error: 'No columns found in CSV' });
    }
    
    if (rows.length === 0) {
      fs.unlinkSync(filePath);
      return res.status(400).json({ error: 'No data rows found in CSV' });
    }
    
    // Analyze each column
    const columns = headers.map(header => {
      const columnValues = rows.map(row => row[header]);
      const detectedType = detectDataType(columnValues);
      
      // Get sample values (first 3 non-empty)
      const samples = columnValues
        .filter(v => v !== null && v !== undefined && v !== '')
        .slice(0, 3);
      
      return {
        originalName: header,
        sanitizedName: sanitizeColumnName(header),
        detectedType: detectedType,
        sampleValues: samples
      };
    });
    
    // Run validation checks
    const validationResults = runValidation(rows, headers);
    
    // Keep the file temporarily - we'll need it for the actual conversion
    // Store the file path in the response so frontend can send it back
    res.json({
      columns: columns,
      rowCount: rows.length,
      tempFilePath: path.basename(filePath),
      validation: validationResults
    });
    
  } catch (error) {
    console.error('Error:', error);
    if (req.file && req.file.path) {
      fs.unlinkSync(req.file.path);
    }
    res.status(500).json({ error: 'Server error: ' + error.message });
  }
});

// Validation function - checks for common data quality issues
function runValidation(rows, headers) {
  const results = {
    passed: [],
    warnings: [],
    errors: []
  };
  
  // Check 1: Column consistency (all rows have same columns)
  const expectedColumns = headers.length;
  let inconsistentRows = 0;
  rows.forEach((row, idx) => {
    const actualColumns = Object.keys(row).length;
    if (actualColumns !== expectedColumns) {
      inconsistentRows++;
    }
  });
  
  if (inconsistentRows === 0) {
    results.passed.push({
      type: 'consistency',
      message: 'All rows have consistent column count'
    });
  } else {
    results.warnings.push({
      type: 'consistency',
      message: `${inconsistentRows} rows have inconsistent column counts`,
      severity: 'warning'
    });
  }
  
  // Check 2: Duplicate detection per column
  headers.forEach(header => {
    const values = rows.map(row => row[header]);
    const nonEmptyValues = values.filter(v => v !== null && v !== undefined && v !== '');
    const uniqueValues = new Set(nonEmptyValues);
    const duplicateCount = nonEmptyValues.length - uniqueValues.size;
    
    if (duplicateCount > 0) {
      // Find which rows have duplicates
      const valueCounts = {};
      const duplicateRows = [];
      
      values.forEach((val, idx) => {
        if (val !== null && val !== undefined && val !== '') {
          if (!valueCounts[val]) {
            valueCounts[val] = [];
          }
          valueCounts[val].push(idx + 1); // 1-indexed for user display
        }
      });
      
      // Get examples of duplicate rows (limit to 3 examples)
      const duplicateExamples = Object.entries(valueCounts)
        .filter(([val, rows]) => rows.length > 1)
        .slice(0, 3)
        .map(([val, rows]) => `"${val}" in rows ${rows.slice(0, 3).join(', ')}${rows.length > 3 ? '...' : ''}`);
      
      results.warnings.push({
        type: 'duplicates',
        column: header,
        message: `Column "${header}" has ${duplicateCount} duplicate values`,
        details: duplicateExamples.join('; '),
        severity: 'warning'
      });
    }
  });
  
  // Check 3: NULL/empty value detection
  headers.forEach(header => {
    const values = rows.map(row => row[header]);
    const nullCount = values.filter(v => v === null || v === undefined || v === '').length;
    const nullPercentage = ((nullCount / values.length) * 100).toFixed(1);
    
    if (nullCount > 0) {
      if (nullPercentage > 50) {
        results.warnings.push({
          type: 'nulls',
          column: header,
          message: `Column "${header}" has ${nullCount} NULL/empty values (${nullPercentage}%)`,
          severity: 'warning'
        });
      } else if (nullCount > 0) {
        results.warnings.push({
          type: 'nulls',
          column: header,
          message: `Column "${header}" has ${nullCount} NULL/empty values (${nullPercentage}%)`,
          severity: 'info'
        });
      }
    }
  });
  
  // Check 4: Date format consistency
  headers.forEach(header => {
    const values = rows.map(row => row[header]).filter(v => v !== null && v !== undefined && v !== '');
    
    if (values.length === 0) return;
    
    // Check if column looks like it contains dates (not just parseable as dates)
    // A real date column should have dashes, slashes, or date-like patterns
    const datePatternCount = values.filter(v => {
      const str = String(v);
      // Look for actual date patterns: contains -, /, or words like Jan, January, etc
      return /\d{4}[-\/]\d{1,2}[-\/]\d{1,2}/.test(str) || // YYYY-MM-DD or YYYY/MM/DD
             /\d{1,2}[-\/]\d{1,2}[-\/]\d{2,4}/.test(str) || // MM-DD-YYYY or DD-MM-YYYY
             /(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/i.test(str); // Month names
    }).length;
    
    // Only treat as date column if >50% have actual date patterns
    if (datePatternCount > values.length * 0.5) {
      // This looks like a date column - check for invalid dates
      const invalidDates = values.filter(v => {
        const str = String(v);
        // Has date pattern but can't be parsed
        const hasPattern = /\d{4}[-\/]\d{1,2}[-\/]\d{1,2}/.test(str) || 
                          /\d{1,2}[-\/]\d{1,2}[-\/]\d{2,4}/.test(str) ||
                          /(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/i.test(str);
        return hasPattern && isNaN(Date.parse(v));
      });
      
      if (invalidDates.length > 0) {
        const invalidExamples = invalidDates.slice(0, 3).map(v => `"${v}"`).join(', ');
        results.errors.push({
          type: 'date_format',
          column: header,
          message: `Column "${header}" has ${invalidDates.length} invalid date values`,
          details: `Examples: ${invalidExamples}`,
          severity: 'error'
        });
      }
    }
  });
  
  // Check 5: Data type inconsistency within column
  headers.forEach(header => {
    const values = rows.map(row => row[header]).filter(v => v !== null && v !== undefined && v !== '');
    
    if (values.length === 0) return;
    
    let numberCount = 0;
    let textCount = 0;
    
    values.forEach(v => {
      if (/^-?\d*\.?\d+$/.test(String(v).trim())) {
        numberCount++;
      } else {
        textCount++;
      }
    });
    
    // If column is mixed (has both numbers and text)
    if (numberCount > 0 && textCount > 0 && numberCount > values.length * 0.1 && textCount > values.length * 0.1) {
      results.warnings.push({
        type: 'mixed_types',
        column: header,
        message: `Column "${header}" has mixed data types (${numberCount} numbers, ${textCount} text values)`,
        severity: 'warning'
      });
    }
  });
  
  // Check 6: Unusually long values
  headers.forEach(header => {
    const values = rows.map(row => row[header]).filter(v => v !== null && v !== undefined && v !== '');
    const maxLength = Math.max(...values.map(v => String(v).length));
    
    if (maxLength > 1000) {
      results.warnings.push({
        type: 'length',
        column: header,
        message: `Column "${header}" has very long values (max: ${maxLength} characters)`,
        details: 'Consider using TEXT type instead of VARCHAR',
        severity: 'info'
      });
    }
  });
  
  // Check 7: Suspicious values (common issues)
  headers.forEach(header => {
    const values = rows.map(row => row[header]).filter(v => v !== null && v !== undefined && v !== '');
    
    // Check for common placeholder values
    const placeholders = ['N/A', 'n/a', 'null', 'NULL', 'None', 'none', '#N/A', 'TBD', 'tbd'];
    const placeholderCount = values.filter(v => placeholders.includes(String(v).trim())).length;
    
    if (placeholderCount > 0) {
      results.warnings.push({
        type: 'placeholders',
        column: header,
        message: `Column "${header}" has ${placeholderCount} placeholder values (N/A, null, etc.)`,
        details: 'These will be treated as text, not NULL values',
        severity: 'info'
      });
    }
  });
  
  // Summary check - if no issues found
  if (results.warnings.length === 0 && results.errors.length === 0) {
    results.passed.push({
      type: 'overall',
      message: 'No data quality issues detected! 🎉'
    });
  }
  
  return results;
}

// Function to escape SQL values
function escapeSQLValue(value, dataType) {
  if (value === null || value === undefined || value === '') {
    return 'NULL';
  }
  
  const str = String(value).trim();
  
  if (dataType === 'INTEGER') {
    return str;
  }
  
  if (dataType.startsWith('DECIMAL')) {
    return str;
  }
  
  // For strings and dates, escape single quotes
  return `'${str.replace(/'/g, "''")}'`;
}

// Main endpoint to convert CSV to SQL (with optional type overrides)
app.post('/convert', upload.single('csvFile'), (req, res) => {
  try {
    const tableName = req.body.tableName || 'my_table';
    const dialect = req.body.dialect || 'postgresql';
    const typeOverrides = req.body.typeOverrides ? JSON.parse(req.body.typeOverrides) : {};
    
    let filePath;
    
    // Check if this is from preview (using temp file) or direct upload
    if (req.body.tempFilePath) {
      filePath = path.join('uploads', req.body.tempFilePath);
    } else if (req.file) {
      filePath = req.file.path;
    } else {
      return res.status(400).json({ error: 'No file provided' });
    }
    
    // Read the uploaded file
    const csvData = fs.readFileSync(filePath, 'utf8');
    
    // Parse CSV
    const parsed = Papa.parse(csvData, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false // Keep as strings for type detection
    });
    
    if (parsed.errors.length > 0) {
      fs.unlinkSync(filePath); // Clean up
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
    
    // Detect data types for each column (or use overrides)
    const columnTypes = {};
    headers.forEach(header => {
      const sanitizedName = sanitizeColumnName(header);
      
      // Check if user provided an override for this column
      if (typeOverrides[sanitizedName]) {
        columnTypes[header] = convertToDialect(typeOverrides[sanitizedName], dialect);
      } else {
        // Auto-detect
        const columnValues = rows.map(row => row[header]);
        const genericType = detectDataType(columnValues);
        columnTypes[header] = convertToDialect(genericType, dialect);
      }
    });
    
    // Generate CREATE TABLE statement with dialect-specific syntax
    const sanitizedTableName = sanitizeColumnName(tableName);
    let createTableSQL = `CREATE TABLE ${sanitizedTableName} (\n`;
    
    const columnDefinitions = headers.map(header => {
      const sanitizedCol = sanitizeColumnName(header);
      const dataType = columnTypes[header];
      return `  ${sanitizedCol} ${dataType}`;
    });
    
    createTableSQL += columnDefinitions.join(',\n');
    createTableSQL += '\n)';
    
    // Add dialect-specific table options
    if (dialect === 'mysql') {
      createTableSQL += ' ENGINE=InnoDB DEFAULT CHARSET=utf8mb4';
    }
    createTableSQL += ';';
    
    // Generate INSERT statements
    let insertSQL = '';
    const batchSize = 100; // Insert 100 rows at a time
    
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      
      insertSQL += `INSERT INTO ${sanitizedTableName} (`;
      insertSQL += headers.map(h => sanitizeColumnName(h)).join(', ');
      insertSQL += ') VALUES\n';
      
      const valueRows = batch.map(row => {
        const values = headers.map(header => {
          const dataType = columnTypes[header];
          return escapeSQLValue(row[header], dataType);
        });
        return `  (${values.join(', ')})`;
      });
      
      insertSQL += valueRows.join(',\n');
      insertSQL += ';\n\n';
    }
    
    // Clean up uploaded file
    fs.unlinkSync(filePath);
    
    // Return the SQL
    res.json({
      createTable: createTableSQL,
      insert: insertSQL,
      rowCount: rows.length,
      columnCount: headers.length,
      dialect: dialect
    });
    
  } catch (error) {
    console.error('Error:', error);
    if (req.file && req.file.path) {
      fs.unlinkSync(req.file.path);
    }
    res.status(500).json({ error: 'Server error: ' + error.message });
  }
});

// Create uploads directory if it doesn't exist
if (!fs.existsSync('uploads')) {
  fs.mkdirSync('uploads');
}

// Create public directory if it doesn't exist
if (!fs.existsSync('public')) {
  fs.mkdirSync('public');
}

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`🚀 CSV to SQL Converter running at http://localhost:${PORT}`);
  console.log(`📁 Upload a CSV file and get SQL statements instantly!`);
});
