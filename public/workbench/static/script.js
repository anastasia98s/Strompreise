
new Vue({
    el: '#app',
    data: {
        schema: null,
        sqlQuery: '',
        queryResult: null,
        displayedResults: null,
        queryError: null,
        loadingSchema: false,
        executing: false,
        showSchema: true,
        queryEditorHeight: 0,
        queryEditorExpanded: true,
        rowsPerPage: 50,
        currentPage: 1,
        showModal: false,
        modalData: null,
        rawModalData: null,
        modalColumnName: '',
        queryExamples: [
    { label: 'Information Schema', query: `SELECT 
    COLUMN_NAME, 
    DATA_TYPE, 
    CHARACTER_MAXIMUM_LENGTH, 
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'table_name';` },
    { label: 'List Tables', query: "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dbo';" },
    { label: 'Database Size', query: "SELECT CAST(SUM(size) * 8.0 / 1024 AS DECIMAL(10,2)) AS 'Database Size (MB)' FROM sys.database_files;" },
    { label: 'Active Connections', query: 'SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process = 1;' }
],
queryShortcut: [
    { label: 'Country', query: 'SELECT * FROM t_country;' },
    { label: 'Province', query: 'SELECT * FROM t_province;' },
    { label: 'City', query: 'SELECT * FROM t_city;' },
    { label: 'Postal Area', query: 'SELECT TOP 100 * FROM t_postal_area;' },
    { label: 'Date', query: `SELECT * FROM t_date ORDER BY d_date;` },
    { label: 'Hour', query: `SELECT * FROM t_hour ORDER BY h_hour;` },
    { label: 'Component', query: 'SELECT * FROM t_component;' },
    { label: 'Count Value', query: `SELECT COUNT(*) FROM t_value;` },
    { label: 'Country, Province, City, Postal', query: `SELECT TOP 100 c_name, p_name, ci_name, pa_name, pa_code, pa_data 
FROM t_postal_area 
JOIN t_city ON t_city.ci_id = t_postal_area.ci_id 
JOIN t_province ON t_province.p_id = t_city.p_id
JOIN t_country ON t_country.c_id = t_province.c_id;` },
    { label: 'Electricity data/hour', query: `SELECT TOP 100 
    JSON_QUERY(pa_data, '$.energy.todayHours') AS "Electricity data/hour", 
    pa_code AS "Zip Code", 
    ci_name AS "City", 
    c_name AS "Country"
FROM t_postal_area 
JOIN t_city ON t_city.ci_id = t_postal_area.ci_id 
JOIN t_province ON t_province.p_id = t_city.p_id
JOIN t_country ON t_country.c_id = t_province.c_id
WHERE c_name = 'Deutschland' AND pa_data IS NOT NULL;
` },
    { label: 'Total Electricity Data/Country', query: `SELECT c_name AS "Country", COUNT(*) AS "Total Data"
FROM t_postal_area
JOIN t_city ON t_city.ci_id = t_postal_area.ci_id 
JOIN t_province ON t_province.p_id = t_city.p_id
JOIN t_country ON t_country.c_id = t_province.c_id
WHERE pa_status_code = 200
GROUP BY c_name;
` },
    { label: 'Electricity Price Components in Hour (JSON)', query: `-- formula for germany region, for other regions there may be missing components!

SELECT
  hourly_data.value AS "Data",
  JSON_VALUE(pa_data, '$.currency') AS "Currency",
  JSON_VALUE(hourly_data.value, '$.date') AS "Date", 
  JSON_VALUE(hourly_data.value, '$.hour') AS "Hour", 

  -- Taxes
  (SELECT TOP 1 CAST(JSON_VALUE(pc.value, '$.priceExcludingVat') AS DECIMAL(10,4))
   FROM OPENJSON(JSON_QUERY(hourly_data.value, '$.priceComponents')) AS pc
   WHERE JSON_VALUE(pc.value, '$.type') = 'taxes') AS "taxes_ex_vat",

  (SELECT TOP 1 CAST(JSON_VALUE(pc.value, '$.priceIncludingVat') AS DECIMAL(10,4))
   FROM OPENJSON(JSON_QUERY(hourly_data.value, '$.priceComponents')) AS pc
   WHERE JSON_VALUE(pc.value, '$.type') = 'taxes') AS "taxes_in_vat",

  -- Power
  (SELECT TOP 1 CAST(JSON_VALUE(pc.value, '$.priceExcludingVat') AS DECIMAL(10,4))
   FROM OPENJSON(JSON_QUERY(hourly_data.value, '$.priceComponents')) AS pc
   WHERE JSON_VALUE(pc.value, '$.type') = 'power') AS "power_ex_vat",

  (SELECT TOP 1 CAST(JSON_VALUE(pc.value, '$.priceIncludingVat') AS DECIMAL(10,4))
   FROM OPENJSON(JSON_QUERY(hourly_data.value, '$.priceComponents')) AS pc
   WHERE JSON_VALUE(pc.value, '$.type') = 'power') AS "power_in_vat",

  -- Grid
  (SELECT TOP 1 CAST(JSON_VALUE(pc.value, '$.priceExcludingVat') AS DECIMAL(10,4))
   FROM OPENJSON(JSON_QUERY(hourly_data.value, '$.priceComponents')) AS pc
   WHERE JSON_VALUE(pc.value, '$.type') = 'grid') AS "grid_ex_vat",

  (SELECT TOP 1 CAST(JSON_VALUE(pc.value, '$.priceIncludingVat') AS DECIMAL(10,4))
   FROM OPENJSON(JSON_QUERY(hourly_data.value, '$.priceComponents')) AS pc
   WHERE JSON_VALUE(pc.value, '$.type') = 'grid') AS "grid_in_vat",

  pa_code AS "Zip Code", 
  ci_name AS "City", 
  c_name AS "Country"

FROM t_postal_area 
JOIN t_city     ON t_city.ci_id = t_postal_area.ci_id 
JOIN t_province ON t_province.p_id = t_city.p_id
JOIN t_country  ON t_country.c_id = t_province.c_id
CROSS APPLY OPENJSON(JSON_QUERY(pa_data, '$.energy.todayHours')) AS hourly_data

WHERE pa_code = '01307';
` },
    { label: 'Electricity Price in Hour (JSON)', query: `-- formula for germany region, for other regions there may be missing components!

SELECT
  JSON_VALUE(hourly_data.value, '$.date') AS "Date", 
  JSON_VALUE(hourly_data.value, '$.hour') AS "Hour", 
  CAST(JSON_VALUE(hourly_data.value, '$.priceIncludingVat') AS DECIMAL(10,4)) AS "Gesamtpreis",

  (SELECT TOP 1 CAST(JSON_VALUE(pc.value, '$.priceExcludingVat') AS DECIMAL(10,4))
   FROM OPENJSON(JSON_QUERY(hourly_data.value, '$.priceComponents')) AS pc
   WHERE JSON_VALUE(pc.value, '$.type') = 'power') AS "Nettostrompreis",

  (CAST(JSON_VALUE(hourly_data.value, '$.priceIncludingVat') AS DECIMAL(10,4)) - 
   (SELECT TOP 1 CAST(JSON_VALUE(pc.value, '$.priceExcludingVat') AS DECIMAL(10,4))
    FROM OPENJSON(JSON_QUERY(hourly_data.value, '$.priceComponents')) AS pc
    WHERE JSON_VALUE(pc.value, '$.type') = 'power')) AS "Steuern_und_Abgaben",

  pa_code AS "Zip Code", 
  ci_name AS "City", 
  c_name AS "Country",
  JSON_VALUE(pa_data, '$.currency') AS "Currency",
  hourly_data.value AS "Data"

FROM t_postal_area 
JOIN t_city     ON t_city.ci_id = t_postal_area.ci_id 
JOIN t_province ON t_province.p_id = t_city.p_id
JOIN t_country  ON t_country.c_id = t_province.c_id
CROSS APPLY OPENJSON(JSON_QUERY(pa_data, '$.energy.todayHours')) AS hourly_data

WHERE pa_code = '01307';
` },
    { label: 'Electricity Net Price per Hour (TABULAR)', query: `SELECT t_component.co_name, t_hour.h_hour, t_date.d_date, t_value.v_value, t_postal_area.pa_code, t_postal_area.pa_data
FROM t_value
JOIN t_hour ON t_hour.h_id = t_value.h_id
JOIN t_date ON t_date.d_id = t_value.d_id
JOIN t_component ON t_component.co_id = t_value.co_id 
JOIN t_postal_area ON t_postal_area.pa_id = t_value.pa_id
WHERE t_postal_area.pa_code = '01307'
ORDER BY t_postal_area.pa_code, t_date.d_date, t_hour.h_hour;` },
]
    },
    computed: {
        totalRows() {
            return this.queryResult ? this.queryResult.length : 0;
        },
        displayedRows() {
            return this.displayedResults ? this.displayedResults.length : 0;
        },
        hasMoreData() {
            return this.queryResult && this.displayedResults && 
                    this.displayedResults.length < this.queryResult.length;
        },
        remainingRows() {
            return this.totalRows - this.displayedRows;
        }
    },
    mounted() {
        this.loadSchema();
        // Add keyboard shortcut for query execution
        document.addEventListener('keydown', (e) => {
            if (e.ctrlKey && e.key === 'Enter') {
                e.preventDefault();
                this.executeQuery();
            }
            if (e.key === 'Escape' && this.showModal) {
                this.closeModal();
            }
        });
        
        // Handle window resize
        //window.addEventListener('resize', this.handleResize);
        //this.handleResize();
        this.toggleQueryHeight()
    },
    methods: {
        toggleSchema() {
            this.showSchema = !this.showSchema;
        },
        toggleQueryHeight() {
            this.queryEditorExpanded = !this.queryEditorExpanded;
            this.queryEditorHeight = this.queryEditorExpanded ? 450 : 250;
        },
        /* handleResize() {
            console.log(window.innerHeight)
            // Adjust query editor height based on screen size
            const vh = window.innerHeight;
            if (vh < 700) {
                this.queryEditorHeight = this.queryEditorExpanded ? 150 : 80;
            } else {
                this.queryEditorHeight = this.queryEditorExpanded ? 200 : 100;
            }
        }, */
        async loadSchema() {
            this.loadingSchema = true;
            try {
                const response = await fetch('/api/workbench/schema');
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                const data = await response.json();
                this.schema = data.data;
            } catch (error) {
                console.error('Error loading schema:', error);
                this.schema = 'Error loading schema: ' + error.message;
            } finally {
                this.loadingSchema = false;
            }
        },
        async executeQuery() {
            if (!this.sqlQuery.trim() || this.executing) return;
            
            this.executing = true;
            this.queryResult = null;
            this.displayedResults = null;
            this.queryError = null;
            this.currentPage = 1;
            
            try {
                const response = await fetch('/api/workbench/query', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        query: this.sqlQuery
                    })
                });
                
                const data = await response.json();
                
                if (!response.ok) {
                    throw new Error(data.detail || 'Query execution failed');
                }
                
                this.queryResult = data.data;
                this.loadInitialData();
            } catch (error) {
                console.error('Error executing query:', error);
                this.queryError = error.message;
            } finally {
                this.executing = false;
            }
        },
        loadInitialData() {
            if (this.queryResult && this.queryResult.length > 0) {
                this.displayedResults = this.queryResult.slice(0, this.rowsPerPage);
            } else {
                this.displayedResults = this.queryResult;
            }
        },
        loadMoreData() {
            if (!this.hasMoreData) return;
            
            const startIndex = this.displayedResults.length;
            const endIndex = Math.min(startIndex + this.rowsPerPage, this.queryResult.length);
            const newRows = this.queryResult.slice(startIndex, endIndex);
            
            this.displayedResults = [...this.displayedResults, ...newRows];
        },
        clearQuery() {
            this.sqlQuery = '';
            this.queryResult = null;
            this.displayedResults = null;
            this.queryError = null;
            this.currentPage = 1;
        },
        formatValue(value) {
            if (value === null || value === undefined) {
                return 'NULL';
            }
            if (typeof value === 'object') {
                return JSON.stringify(value);
            }
            const str = String(value);
            // Truncate very long values for display
            return str.length > 100 ? str.substring(0, 100) + '...' : str;
        },
        showCellData(data, columnName) {
            if (this.isJsonString(data)){
                this.modalData = JSON.parse(data);
            }else{
                this.modalData = data;
            }
            this.rawModalData = data === null || data === undefined ? 'NULL' : String(data);
            this.modalColumnName = columnName;
            this.showModal = true;
        },
        closeModal() {
            this.showModal = false;
            this.modalData = null;
            this.rawModalData = null;
            this.modalColumnName = '';
        },
        isJsonString(str) {
            if (typeof str !== 'string') return false;
            try {
                const parsed = JSON.parse(str);
                return typeof parsed === 'object' && parsed !== null;
            } catch (e) {
                return false;
            }
        },
        syntaxHighlight(json) {
            json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
            return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
                var cls = 'json-number';
                if (/^"/.test(match)) {
                    if (/:$/.test(match)) {
                        cls = 'json-key';
                    } else {
                        cls = 'json-string';
                    }
                } else if (/true|false/.test(match)) {
                    cls = 'json-boolean';
                } else if (/null/.test(match)) {
                    cls = 'json-null';
                }
                return '<span class="' + cls + '">' + match + '</span>';
            });
        },
        getDataType(data) {
            if (data === null || data === undefined) return 'NULL';
            if (this.isJsonString(data)) return 'JSON';
            return typeof data;
        },
        getDataLength(data) {
            if (data === null || data === undefined) return '0';
            return String(data).length;
        },
        async copyToClipboard(text) {
            if (typeof text === 'object' && text !== null) {
                text = JSON.stringify(text, null, 2);
            }
            try {
                await navigator.clipboard.writeText(text);
            } catch (err) {
                console.error('Failed to copy text: ', err);
                // Fallback for older browsers
                const textArea = document.createElement('textarea');
                textArea.value = text;
                document.body.appendChild(textArea);
                textArea.select();
                document.execCommand('copy');
                document.body.removeChild(textArea);
            }
        }
    }
});