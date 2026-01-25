# 📊 CSV to SQL Converter

> Professional CSV to SQL conversion tool with comprehensive data validation and multi-database support.

[**Live Demo**](https://csv-to-sql-converter-tau.vercel.app) | [Report Bug](https://github.com/gbohannon99/csv-to-sql-converter/issues) | [Request Feature](https://github.com/gbohannon99/csv-to-sql-converter/issues)

---

## ✨ Features

### Core Functionality
- **🗄️ Multi-Database Support** - PostgreSQL, MySQL, SQL Server, SQLite, and Oracle
- **🔍 Smart Type Detection** - Automatically detects INTEGER, DECIMAL, DATE, and VARCHAR types
- **✏️ Manual Type Override** - Preview and override any detected column type
- **📋 Batch INSERT Statements** - Optimized batching (500-1000 rows per statement)
- **🛡️ SQL Sanitization** - Proper escaping, NULL handling, and injection prevention

### Data Validation (7 Checks)
- ✅ **Column Consistency** - Ensures all rows have the same number of columns
- ✅ **Duplicate Detection** - Identifies duplicate values in each column
- ✅ **NULL Analysis** - Counts and reports missing/empty values
- ✅ **Date Format Validation** - Catches invalid date formats before import
- ✅ **Mixed Type Detection** - Finds columns with inconsistent data types
- ✅ **Length Analysis** - Warns about unusually long values
- ✅ **Placeholder Detection** - Identifies "N/A", "null", "TBD" text values

### User Experience
- 🌙 **Dark Developer Theme** - Professional VS Code/GitHub-inspired UI
- 📥 **Copy or Download** - Get SQL as text or download as .sql file
- 📊 **Real-time Validation** - See issues before generating SQL
- 🎨 **Sample Data Preview** - View first 3 values per column

---

## 🚀 Quick Start

### Try It Online
Visit [csv-to-sql-converter-tau.vercel.app](https://csv-to-sql-converter-tau.vercel.app)

### Run Locally

```bash
# Clone the repository
git clone https://github.com/gbohannon99/csv-to-sql-converter.git

# Navigate to directory
cd csv-to-sql-converter

# Install dependencies
npm install

# Start development server
npm run dev

# Open browser to http://localhost:3000
```

---

## 💡 Usage

### 1. Upload CSV
- Ensure your CSV has headers in the first row
- Supports files up to 50MB

### 2. Choose Database
Select your target database from the dropdown:
- PostgreSQL
- MySQL
- SQL Server
- SQLite
- Oracle

### 3. Review Validation
The tool automatically checks for:
- Data quality issues
- Potential import problems
- Type detection accuracy

### 4. Override Types (Optional)
Click on any detected type to override it. Common use cases:
- ZIP codes: Change from `INTEGER` to `VARCHAR(10)` to preserve leading zeros
- Phone numbers: Change to `VARCHAR(15)` to keep formatting
- Large text: Change to `TEXT` for unlimited length

### 5. Generate SQL
- Creates `CREATE TABLE` statement with proper types
- Generates optimized `INSERT` statements
- Copy to clipboard or download as .sql file

---

## 🏗️ Tech Stack

### Backend
- **Node.js** - JavaScript runtime
- **Express** - Web framework
- **Multer** - File upload handling
- **PapaParse** - Fast CSV parsing

### Frontend
- **Vanilla JavaScript** - No framework dependencies
- **Custom CSS** - Dark theme with monospace typography
- **Responsive Design** - Works on desktop and mobile

### Deployment
- **Vercel** - Serverless hosting
- **GitHub Actions** - CI/CD pipeline
- **Google Analytics** - Usage tracking

---

## 📦 Project Structure

```
csv-to-sql-converter/
├── index.js              # Vercel serverless function
├── server.js             # Local development server
├── package.json          # Dependencies
├── vercel.json           # Vercel configuration
├── public/
│   ├── index.html        # Frontend interface
│   └── styles.css        # Dark theme styling
└── README.md
```

---

## 🔧 Configuration

### Environment Variables
No environment variables required for basic usage.

### Vercel Deployment
The project is configured for zero-config deployment on Vercel:

```json
{
  "version": 2,
  "builds": [
    { "src": "index.js", "use": "@vercel/node" }
  ]
}
```

---

## 🎯 Use Cases

### Data Migration
- Move data from spreadsheets to databases
- Convert legacy CSV exports to modern SQL

### Data Analysis
- Validate CSV files before importing
- Detect data quality issues early

### Development
- Generate test data SQL scripts
- Create database seed files

### Business Intelligence
- Prepare data for analytics platforms
- Clean and validate exported reports

---

## 🐛 Common Issues

### ZIP Codes Losing Leading Zeros
**Problem:** `07001` becomes `7001`  
**Solution:** Override type from `INTEGER` to `VARCHAR(10)`

### Dates Not Importing
**Problem:** Invalid date formats like `2024-13-45`  
**Solution:** Check validation warnings before generating SQL

### File Too Large
**Problem:** File exceeds limits  
**Current Limit:** 10,000 rows (free tier)  
**Solution:** Split file or contact for larger file support

---

## 🚦 Limitations

### Current Version (Beta)
- **Row Limit:** 10,000 rows per file
- **File Size:** 50MB maximum
- **Processing Time:** 10 seconds (Vercel free tier)

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development Setup

```bash
# Fork the repository
# Clone your fork
git clone https://github.com/YOUR_USERNAME/csv-to-sql-converter.git

# Create a feature branch
git checkout -b feature/amazing-feature

# Make your changes and commit
git commit -m "Add amazing feature"

# Push to your fork
git push origin feature/amazing-feature

# Open a Pull Request
```

### Contribution Guidelines
- Follow existing code style
- Add tests for new features
- Update documentation as needed
- Keep commits atomic and well-described

---

## 🙏 Acknowledgments

- **PapaParse** - Excellent CSV parsing library
- **Vercel** - Seamless deployment platform
- **Express.js** - Minimal web framework
- **Community** - Thanks to all users providing feedback!

---

## Contact & Support

### Found a Bug?
[Report it here](https://github.com/gbohannon99/csv-to-sql-converter/issues)

### Have a Feature Request?
[Submit it here](https://github.com/gbohannon99/csv-to-sql-converter/issues)

### Need Help?
- 📧 Email: feedback@csvtosql.app
- 💬 GitHub Issues: [Ask a question](https://github.com/gbohannon99/csv-to-sql-converter/issues)

---

## 🌟 Show Your Support

Give a ⭐️ if this project helped you!

---

<p align="center">
  Made with ❤️ for data analysts who deserve better tools
</p>

<p align="center">
  <a href="https://csv-to-sql-converter-tau.vercel.app">View Live Demo</a> •
  <a href="https://github.com/gbohannon99/csv-to-sql-converter/issues">Report Bug</a> •
  <a href="https://github.com/gbohannon99/csv-to-sql-converter/issues">Request Feature</a>
</p>
