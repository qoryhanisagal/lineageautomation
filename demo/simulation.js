// ===================================================================
// Frontend Simulation Logic for Lineage Automation Framework
// Perfect for live demo with Kevin
// ===================================================================

class LineageSimulation {
    constructor() {
        this.currentStep = 0;
        this.discoveredFiles = [];
        this.generatedJSON = null;
        this.isRunning = false;
        this.currentApiResponse = null;

        // Initialize column-level lineage tracker
        this.columnTracker = new ColumnLevelLineageTracker({
            purviewAccount: 'demo-purview',
            tenantId: 'demo-tenant'
        });

        this.initializeEventListeners();
        this.initializeResponseTabs();
        this.initializeColumnLineage();
    }

    initializeEventListeners() {
        document.getElementById('startScan').addEventListener('click', () => this.startDirectoryScan());
        document.getElementById('generateJSON').addEventListener('click', () => this.generateJSONPayload());
        document.getElementById('registerLineage').addEventListener('click', () => this.registerInPurview());
        document.getElementById('reset').addEventListener('click', () => this.resetDemo());
        
        document.getElementById('copyJSON').addEventListener('click', () => this.copyJSONToClipboard());
        document.getElementById('downloadJSON').addEventListener('click', () => this.downloadJSON());
    }

    initializeResponseTabs() {
        // Initialize all response tabs
        const responseTab = document.getElementById('responseTab');
        const headersTab = document.getElementById('headersTab');
        const cookiesTab = document.getElementById('cookiesTab');
        
        if (responseTab) {
            responseTab.addEventListener('click', () => this.showResponseContent());
        }
        
        if (headersTab) {
            headersTab.addEventListener('click', () => this.showResponseHeaders());
        }
        
        if (cookiesTab) {
            cookiesTab.addEventListener('click', () => this.showResponseCookies());
        }
    }

    showResponseContent() {
        // Switch to Response tab
        this.switchResponseTab('responseTab');
        
        const apiResponse = document.getElementById('apiResponse');
        if (apiResponse && this.currentApiResponse) {
            apiResponse.textContent = this.currentApiResponse;
        }
        this.log('Showing Purview API response body', 'info');
    }

    showResponseHeaders() {
        // Switch to Headers tab
        this.switchResponseTab('headersTab');
        
        const apiResponse = document.getElementById('apiResponse');
        if (apiResponse) {
            const headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIs...',
                'X-MS-Request-ID': '12345678-1234-1234-1234-123456789012',
                'X-MS-Correlation-ID': '87654321-4321-4321-4321-210987654321',
                'Cache-Control': 'no-cache',
                'Date': new Date().toUTCString(),
                'Server': 'Microsoft-HTTPAPI/2.0',
                'Access-Control-Allow-Origin': '*',
                'X-Content-Type-Options': 'nosniff'
            };
            
            apiResponse.textContent = JSON.stringify(headers, null, 2);
        }
        this.log('Showing Purview API response headers', 'info');
    }

    showResponseCookies() {
        // Switch to Cookies tab
        this.switchResponseTab('cookiesTab');
        
        const apiResponse = document.getElementById('apiResponse');
        if (apiResponse) {
            const cookies = {
                'session_id': 'abcd1234-5678-9012-3456-789012345678',
                'auth_token': 'secure_token_12345',
                'request_context': 'purview_lineage_api',
                'expires': new Date(Date.now() + 3600000).toISOString(),
                'secure': true,
                'httpOnly': true,
                'sameSite': 'Strict'
            };
            
            apiResponse.textContent = JSON.stringify(cookies, null, 2);
        }
        this.log('Showing Purview API response cookies', 'info');
    }

    switchResponseTab(activeTabId) {
        // Remove active class from all tabs
        ['responseTab', 'headersTab', 'cookiesTab'].forEach(tabId => {
            const tab = document.getElementById(tabId);
            if (tab) {
                tab.classList.remove('tab-active');
            }
        });
        
        // Add active class to selected tab
        const activeTab = document.getElementById(activeTabId);
        if (activeTab) {
            activeTab.classList.add('tab-active');
        }
    }

    initializeColumnLineage() {
        // Initialize column lineage mode toggle
        const columnLineageToggle = document.getElementById('columnLineageToggle');
        if (columnLineageToggle) {
            columnLineageToggle.addEventListener('change', () => {
                this.updateColumnLineageMode();
            });
        }
    }

    updateColumnLineageMode() {
        const isEnabled = document.getElementById('columnLineageToggle').checked;
        if (isEnabled) {
            this.log('🟥 Column-level lineage tracking enabled for Azure SQL tables', 'info');
            this.showColumnLineageAnalysis();
        } else {
            this.log('Column-level lineage tracking disabled', 'info');
        }
    }

    showColumnLineageAnalysis() {
        // Display Azure SQL column lineage mapping table
        const columnLineageSection = document.getElementById('columnLineageMapping');
        if (columnLineageSection) {
            columnLineageSection.style.display = 'block';
            this.populateColumnLineageTable();
        }
    }

    populateColumnLineageTable() {
        const tableBody = document.getElementById('columnMappingTableBody');
        if (!tableBody) return;

        const columnMappings = [
            {
                sourceColumn: 'claim_id',
                targetColumn: 'claim_identifier', 
                transformation: 'RENAME',
                impact: 'HIGH',
                azureSqlDataType: 'VARCHAR(50)',
                azureSqlConstraints: 'PRIMARY KEY, NOT NULL'
            },
            {
                sourceColumn: 'amount',
                targetColumn: 'claim_amount_usd',
                transformation: 'CURRENCY_CONVERSION', 
                impact: 'MEDIUM',
                azureSqlDataType: 'DECIMAL(10,2)',
                azureSqlConstraints: 'NOT NULL, CHECK (claim_amount_usd >= 0)'
            },
            {
                sourceColumn: 'patient_id',
                targetColumn: 'patient_reference_id',
                transformation: 'ANONYMIZATION',
                impact: 'HIGH', 
                azureSqlDataType: 'VARCHAR(64)',
                azureSqlConstraints: 'NOT NULL, INDEX IX_patient_reference'
            }
        ];

        tableBody.innerHTML = '';
        columnMappings.forEach(mapping => {
            const row = document.createElement('tr');
            const impactEmoji = this.getImpactEmoji(mapping.impact);
            
            row.innerHTML = `
                <td class="text-sm font-medium">${mapping.sourceColumn}</td>
                <td class="text-sm font-medium text-primary">${mapping.targetColumn}</td>
                <td class="text-sm"><span class="badge badge-outline badge-sm">${mapping.transformation}</span></td>
                <td class="text-sm">${impactEmoji} <span class="badge badge-outline badge-sm">${mapping.impact}</span></td>
                <td class="text-sm font-mono text-xs">${mapping.azureSqlDataType}</td>
                <td class="text-sm text-xs">${mapping.azureSqlConstraints}</td>
            `;
            tableBody.appendChild(row);
        });

        this.log('🟥 Azure SQL column mappings populated with data types and constraints', 'success');
    }

    getImpactEmoji(impact) {
        const emojis = {
            'LOW': '🟢',
            'MEDIUM': '🟡', 
            'HIGH': '🔴',
            'CRITICAL': '🟥'
        };
        return emojis[impact] || '⚪';
    }

    updateJsonValidationBadge(status, message = '') {
        const badge = document.getElementById('jsonValidationBadge');
        const icon = document.getElementById('jsonValidationIcon');
        const text = document.getElementById('jsonValidationText');
        
        if (!badge || !icon || !text) return;
        
        switch (status) {
            case 'validating':
                badge.className = 'badge badge-warning badge-lg';
                icon.className = 'fas fa-spinner fa-spin mr-1';
                text.textContent = 'Validating...';
                break;
            case 'valid':
                badge.className = 'badge badge-success badge-lg';
                icon.className = 'fas fa-check mr-1';
                text.textContent = 'Valid JSON';
                break;
            case 'invalid':
                badge.className = 'badge badge-error badge-lg';
                icon.className = 'fas fa-times mr-1';
                text.textContent = message || 'Invalid JSON';
                break;
            default:
                badge.className = 'badge badge-secondary badge-lg';
                icon.className = 'fas fa-code mr-1';
                text.textContent = 'No JSON Generated';
                break;
        }
    }

    updateApiStatus(status, description) {
        // Update main status text
        document.getElementById('apiStatus').textContent = status;
        document.getElementById('apiStatusDesc').textContent = description;
        
        // Update badge
        const badge = document.getElementById('apiStatusBadge');
        const statusCard = document.getElementById('apiStatusCard');
        const statusTitle = document.getElementById('apiStatusTitle');
        const statusValue = document.getElementById('apiStatus');
        const statusDesc = document.getElementById('apiStatusDesc');
        
        // Color coding based on status
        if (status === 'Ready') {
            // Green for ready
            badge.className = 'badge badge-success shadow-sm';
            statusCard.className = 'stats stats-vertical shadow-lg border bg-gradient-to-br from-success/10 to-success/5 border-success/20';
            statusTitle.className = 'stat-title text-xs text-success/80';
            statusValue.className = 'stat-value text-xl font-bold text-success';
            statusDesc.className = 'stat-desc text-success/70';
        } else if (status === '200' || status === '201') {
            // Blue for active/success
            badge.className = 'badge badge-info shadow-sm';
            statusCard.className = 'stats stats-vertical shadow-lg border bg-gradient-to-br from-info/10 to-info/5 border-info/20';
            statusTitle.className = 'stat-title text-xs text-info/80';
            statusValue.className = 'stat-value text-xl font-bold text-info';
            statusDesc.className = 'stat-desc text-info/70';
        } else if (status.startsWith('4') || status.startsWith('5')) {
            // Red for errors
            badge.className = 'badge badge-error shadow-sm';
            statusCard.className = 'stats stats-vertical shadow-lg border bg-gradient-to-br from-error/10 to-error/5 border-error/20';
            statusTitle.className = 'stat-title text-xs text-error/80';
            statusValue.className = 'stat-value text-xl font-bold text-error';
            statusDesc.className = 'stat-desc text-error/70';
        } else {
            // Default orange for processing/warning
            badge.className = 'badge badge-warning shadow-sm';
            statusCard.className = 'stats stats-vertical shadow-lg border bg-gradient-to-br from-warning/10 to-warning/5 border-warning/20';
            statusTitle.className = 'stat-title text-xs text-warning/80';
            statusValue.className = 'stat-value text-xl font-bold text-warning';
            statusDesc.className = 'stat-desc text-warning/70';
        }
    }


    log(message, type = 'info') {
        const activityLog = document.getElementById('activityLog');
        const timestamp = new Date().toLocaleTimeString();
        
        // Custom emoji system for activity feed
        const emojis = {
            info: '🔷',
            success: '🟢', 
            warning: '🟡',
            error: '🔴',
            processing: '🔸',
            automation: '🟦',
            completed: '🟧',
            azure: '🟥'
        };
        
        const icons = {
            info: `<span class="text-info">${emojis.info}</span>`,
            success: `<span class="text-success">${emojis.success}</span>`,
            warning: `<span class="text-warning">${emojis.warning}</span>`,
            error: `<span class="text-error">${emojis.error}</span>`
        };

        const logEntry = document.createElement('div');
        logEntry.className = 'mb-2 p-2 rounded bg-base-100';
        logEntry.innerHTML = `
            <span class="text-xs text-base-content/50">[${timestamp}]</span>
            ${icons[type]} 
            <span class="ml-2">${message}</span>
        `;

        activityLog.appendChild(logEntry);
        activityLog.scrollTop = activityLog.scrollHeight;

        // Clear initial placeholder
        const placeholder = activityLog.querySelector('.text-base-content\\/50');
        if (placeholder && activityLog.children.length > 1) {
            placeholder.remove();
        }
    }

    showToast(message, type = 'info') {
        const toastContainer = document.getElementById('toastContainer');
        const toast = document.createElement('div');
        
        const alertClasses = {
            info: 'alert-info',
            success: 'alert-success', 
            warning: 'alert-warning',
            error: 'alert-error'
        };

        toast.className = `alert ${alertClasses[type]} shadow-lg mb-2`;
        toast.innerHTML = `
            <div>
                <span>${message}</span>
            </div>
        `;

        toastContainer.appendChild(toast);

        setTimeout(() => {
            toast.remove();
        }, 3000);
    }

    async startDirectoryScan() {
        if (this.isRunning) return;
        
        this.isRunning = true;
        this.currentStep = 1;

        // Update UI
        document.getElementById('startScan').disabled = true;
        document.getElementById('scannerReady').classList.add('hidden');
        document.getElementById('scannerLoading').classList.remove('hidden');

        this.log('Starting directory scan on ADLS Gen2 container: claims-data');
        this.log('Connecting to Azure Storage Account...');

        await this.delay(1500);

        this.log('Connection established, scanning for new files...');
        
        await this.delay(2000);

        // Pseudo-dynamic file discovery
        this.discoveredFiles = this.generateRandomFiles();
        
        this.log(`Scan complete! Found ${this.discoveredFiles.length} new files for processing`, 'success');
        
        // Update scanner status
        document.getElementById('scannerLoading').classList.add('hidden');
        document.getElementById('scannerComplete').classList.remove('hidden');
        document.getElementById('filesFound').classList.remove('hidden');
        document.getElementById('fileCount').textContent = this.discoveredFiles.length;
        
        // Update hero stats
        document.getElementById('heroFileCount').textContent = this.discoveredFiles.length;
        document.getElementById('totalFiles').textContent = this.discoveredFiles.length;
        document.getElementById('successRate').textContent = '33%'; // Files discovered successfully
        
        // Calculate and update average processing time based on file count
        const avgTime = (this.discoveredFiles.length * 2.5).toFixed(1); // 2.5s per file average
        document.getElementById('avgProcessingTime').textContent = `${avgTime}s`;

        // Populate file list
        this.populateFileList();

        // IMPORTANT: Update JSON Generator status to show it's ready
        document.getElementById('generatorReady').innerHTML = '<i class="fas fa-check-circle text-success mr-2"></i>Ready to generate';
        document.getElementById('generatorReady').classList.remove('text-base-content/70');
        document.getElementById('generatorReady').classList.add('text-success');
        
        // Enable next step
        document.getElementById('generateJSON').disabled = false;
        
        this.showToast('Directory scan completed successfully!', 'success');
        
        this.isRunning = false;

        // Auto-progress to next step if auto-mode is enabled
        const autoMode = document.getElementById('autoModeToggle').checked;
        if (autoMode) {
            this.log('🟦 Auto-mode enabled - continuing to JSON generation...', 'info');
            setTimeout(() => {
                this.generateJSONPayload();
            }, 1000);
        } else {
            this.log('Manual mode - click "Generate JSON" to continue', 'info');
        }
    }

    populateFileList() {
        const fileList = document.getElementById('fileList');
        fileList.innerHTML = '';

        this.discoveredFiles.forEach((file) => {
            const fileCard = document.createElement('div');
            fileCard.className = 'card bg-base-200 shadow-sm';
            fileCard.innerHTML = `
                <div class="card-body p-4">
                    <div class="flex items-center justify-between">
                        <div>
                            <h4 class="font-semibold text-sm">${file.fileName}</h4>
                            <p class="text-xs text-base-content/70">${file.fileSize.toLocaleString()} bytes</p>
                        </div>
                        <div class="badge badge-sm bg-success text-success-content">CSV</div>
                    </div>
                    <div class="text-xs text-base-content/60 mt-2">
                        Schema: ${file.schema.slice(0, 3).join(', ')}${file.schema.length > 3 ? '...' : ''}
                    </div>
                </div>
            `;
            fileList.appendChild(fileCard);
        });
    }

    async generateJSONPayload() {
        if (this.isRunning) return;
        
        this.isRunning = true;
        this.currentStep = 2;

        // Update UI
        document.getElementById('generateJSON').disabled = true;
        document.getElementById('generatorReady').classList.add('hidden');
        document.getElementById('generatorLoading').classList.remove('hidden');

        this.log('Starting JSON payload generation...');
        this.log('Mapping files to transformation pipelines...');

        await this.delay(1000);

        let totalEntities = 0;

        for (const file of this.discoveredFiles) {
            const pipelineConfig = this.mapFileToPipeline(file.fileName);
            const fileComplexity = this.getIntelligentComplexity(file.fileName);
            
            this.log(`Processing: ${file.fileName}`);
            this.log(`Mapped to pipeline: ${pipelineConfig.pipelineName}`);
            this.log(`Complexity level: ${fileComplexity} entities per file`);
            
            await this.delay(800);
            
            totalEntities += fileComplexity; // Use intelligent complexity
        }

        this.log('Generating JSON payload for Purview registration...');
        
        // Show validating status
        this.updateJsonValidationBadge('validating');
        
        await this.delay(1500);

        try {
            // Generate the actual JSON
            this.generatedJSON = this.createLineageJSON();
            
            // Validate the generated JSON
            const jsonString = JSON.stringify(this.generatedJSON, null, 2);
            
            // Parse it back to ensure it's valid JSON
            JSON.parse(jsonString);
            
            // Additional validation checks
            if (!this.generatedJSON.entities || this.generatedJSON.entities.length === 0) {
                throw new Error('No entities generated');
            }
            
            // Validation successful
            this.updateJsonValidationBadge('valid');
            this.log(`JSON payload generated successfully! Created ${this.generatedJSON.entities.length} entities`, 'success');
            
        } catch (error) {
            // Validation failed
            this.updateJsonValidationBadge('invalid', `JSON Error: ${error.message}`);
            this.log(`JSON generation failed: ${error.message}`, 'error');
            this.showToast('JSON generation failed', 'error');
            this.isRunning = false;
            return;
        }
        
        // Update generator status  
        document.getElementById('generatorLoading').classList.add('hidden');
        document.getElementById('generatorComplete').classList.remove('hidden');
        document.getElementById('entitiesGenerated').classList.remove('hidden');
        document.getElementById('entityCount').textContent = this.generatedJSON.entities.length;
        
        // Update hero stats
        document.getElementById('lineageObjects').textContent = this.generatedJSON.entities.length;
        document.getElementById('successRate').textContent = '66%'; // JSON generated successfully
        
        // Update processing time based on entity complexity
        const processingTime = (this.generatedJSON.entities.length * 0.8).toFixed(1);
        document.getElementById('avgProcessingTime').textContent = `${processingTime}s`;

        // Show JSON viewer
        document.getElementById('jsonViewer').style.display = 'block';
        const jsonString = JSON.stringify(this.generatedJSON, null, 2);
        document.getElementById('jsonPayload').textContent = jsonString;

        // Update JSON stats
        const jsonSizeBytes = new Blob([jsonString]).size;
        const jsonSizeKB = (jsonSizeBytes / 1024).toFixed(1);
        document.getElementById('jsonEntityCount').textContent = this.generatedJSON.entities.length;
        document.getElementById('jsonPayloadSize').textContent = `${jsonSizeKB} KB`;
        document.getElementById('jsonSize').textContent = `${jsonSizeBytes} bytes`;

        // IMPORTANT: Update Purview Registration status to show it's ready
        document.getElementById('purviewReady').innerHTML = '<i class="fas fa-check-circle text-success mr-2"></i>Ready to register';
        document.getElementById('purviewReady').classList.remove('text-base-content/70');
        document.getElementById('purviewReady').classList.add('text-success');
        
        // Update API status
        this.updateApiStatus('Ready', 'Purview API ready for registration');
        
        // Enable next step
        document.getElementById('registerLineage').disabled = false;
        
        this.showToast('JSON payload generated successfully!', 'success');
        
        this.isRunning = false;

        // Auto-progress to final step if auto-mode is enabled
        const autoMode = document.getElementById('autoModeToggle').checked;
        if (autoMode) {
            this.log('🟦 Auto-mode enabled - proceeding to Purview registration...', 'info');
            setTimeout(() => {
                this.registerInPurview();
            }, 1500);
        } else {
            this.log('Manual mode - click "Register in Purview" to continue', 'info');
        }
    }

    mapFileToPipeline(fileName) {
        const pipelineMapping = {
            'claims_': {
                pipelineName: 'transform_claims_pipeline',
                destinationTable: 'processed_claims',
                transformationType: 'standardization_and_validation'
            },
            'providers_': {
                pipelineName: 'transform_providers_pipeline',
                destinationTable: 'processed_providers', 
                transformationType: 'deduplication_and_enrichment'
            },
            'patients_': {
                pipelineName: 'transform_patients_pipeline',
                destinationTable: 'processed_patients',
                transformationType: 'privacy_and_anonymization'
            }
        };

        for (const [prefix, config] of Object.entries(pipelineMapping)) {
            if (fileName.startsWith(prefix)) {
                return config;
            }
        }

        return pipelineMapping['claims_']; // Default fallback
    }

    createLineageJSON() {
        const entities = [];
        
        this.discoveredFiles.forEach(file => {
            const pipelineConfig = this.mapFileToPipeline(file.fileName);
            const fileComplexity = this.getIntelligentComplexity(file.fileName);
            const timestamp = new Date().toISOString();
            
            // Create entities based on complexity level
            for (let i = 0; i < fileComplexity; i++) {
                const entitySuffix = i === 0 ? '' : `_${i}`;
                
                if (i === 0) {
                    // Source Dataset (always first entity)
                    entities.push({
                        typeName: "azure_sql_table",
                        attributes: {
                            qualifiedName: `mssql://healthcare-sql-server.database.windows.net/ClaimsDB/dbo/${pipelineConfig.destinationTable}${entitySuffix}`,
                            name: `${pipelineConfig.destinationTable}${entitySuffix}`,
                            description: `Azure SQL table for ${pipelineConfig.transformationType}`,
                            owner: "Data_Engineering_Team",
                            createTime: timestamp,
                            fileSize: file.fileSize,
                            format: "Azure SQL",
                            schema: file.schema,
                            azureSqlServer: "healthcare-sql-server.database.windows.net",
                            azureSqlDatabase: "ClaimsDB"
                        },
                        guid: this.generateGuid(),
                        status: "ACTIVE"
                    });
                } else if (i === 1) {
                    // Process (second entity)
                    entities.push({
                        typeName: "Process",
                        attributes: {
                            qualifiedName: `adf://pipelines/${pipelineConfig.pipelineName}${entitySuffix}`,
                            name: `${pipelineConfig.pipelineName}${entitySuffix}`,
                            description: `Azure Data Factory pipeline - ${pipelineConfig.transformationType}`,
                            processType: "ETL_PIPELINE",
                            inputs: [file.filePath],
                            outputs: [`mssql://healthcare-sql-server.database.windows.net/ClaimsDB/dbo/${pipelineConfig.destinationTable}`],
                            createTime: timestamp,
                            azureService: "Azure Data Factory"
                        },
                        guid: this.generateGuid(),
                        status: "ACTIVE"
                    });
                } else {
                    // Additional entities for higher complexity
                    const entityTypes = ["Column", "Schema", "Index", "View"];
                    const entityType = entityTypes[(i - 2) % entityTypes.length];
                    
                    entities.push({
                        typeName: entityType,
                        attributes: {
                            qualifiedName: `mssql://healthcare-sql-server.database.windows.net/ClaimsDB/dbo/${pipelineConfig.destinationTable}_${entityType.toLowerCase()}${entitySuffix}`,
                            name: `${pipelineConfig.destinationTable}_${entityType.toLowerCase()}${entitySuffix}`,
                            description: `${entityType} entity for ${file.fileName}`,
                            owner: "Data_Architecture_Team",
                            createTime: timestamp,
                            parentTable: pipelineConfig.destinationTable,
                            azureService: "Azure SQL Database"
                        },
                        guid: this.generateGuid(),
                        status: "ACTIVE"
                    });
                }
            }
        });

        return {
            entities,
            referredEntities: {},
            guidAssignments: {}
        };
    }

    async registerInPurview() {
        if (this.isRunning) return;
        
        this.isRunning = true;
        this.currentStep = 3;

        // Update UI
        document.getElementById('registerLineage').disabled = true;
        document.getElementById('purviewReady').classList.add('hidden');
        document.getElementById('purviewLoading').classList.remove('hidden');

        this.log('Authenticating with Microsoft Purview...');
        this.log('Using service principal authentication');
        
        await this.delay(1200);
        
        this.log('Authentication successful, token acquired', 'success');
        this.log('Posting JSON payload to Purview REST API...');
        this.log('POST https://your-purview.purview.azure.com/datamap/api/atlas/v2/entity/bulk');
        
        // Update API status
        this.updateApiStatus('200', 'Successfully connected to Purview API');
        
        await this.delay(2000);
        
        this.log('Processing lineage relationships...');
        
        await this.delay(1500);
        
        this.log('Lineage successfully registered in Microsoft Purview!', 'success');
        
        // Update purview status
        document.getElementById('purviewLoading').classList.add('hidden');
        document.getElementById('purviewComplete').classList.remove('hidden');
        document.getElementById('lineageRegistered').classList.remove('hidden');

        // Show API response
        const mockResponse = {
            "guidAssignments": {},
            "mutatedEntities": {
                "CREATE": this.generatedJSON.entities.map(e => ({
                    guid: e.guid,
                    typeName: e.typeName
                }))
            },
            "partialUpdatedEntities": [],
            "status": "SUCCESS",
            "timestamp": new Date().toISOString()
        };

        // Populate guid assignments
        this.generatedJSON.entities.forEach(entity => {
            mockResponse.guidAssignments[entity.guid] = entity.guid;
        });

        document.getElementById('responseViewer').style.display = 'block';
        
        // Store the response and display it
        this.currentApiResponse = JSON.stringify(mockResponse, null, 2);
        document.getElementById('apiResponse').textContent = this.currentApiResponse;

        // Update the stats with actual entity count
        document.getElementById('entitiesCreated').textContent = this.generatedJSON.entities.length;
        
        // Complete success - 100%
        document.getElementById('successRate').textContent = '100%';
        
        // Update affected systems count (simulate based on entities)
        const affectedSystems = Math.min(this.generatedJSON.entities.length, 4);
        document.getElementById('affectedSystemsCount').textContent = affectedSystems;
        
        this.showToast('Lineage registration completed!', 'success');
        this.log('Lineage relationships now visible in Purview Data Map', 'success');
        
        // Check if this was part of auto mode
        const autoMode = document.getElementById('autoModeToggle').checked;
        if (autoMode) {
            this.log('🟧 Automation pipeline completed successfully!', 'success');
            this.showToast('Full automation completed! Lineage registered in Purview.', 'success');
        }
        
        this.isRunning = false;
    }

    resetDemo() {
        this.currentStep = 0;
        this.discoveredFiles = [];
        this.generatedJSON = null;
        this.isRunning = false;
        this.currentApiResponse = null;

        // Reset buttons
        document.getElementById('startScan').disabled = false;
        document.getElementById('generateJSON').disabled = true;
        document.getElementById('registerLineage').disabled = true;

        // Reset status indicators
        ['scannerComplete', 'generatorComplete', 'purviewComplete'].forEach(id => {
            document.getElementById(id).classList.add('hidden');
        });
        
        ['scannerReady', 'generatorReady', 'purviewReady'].forEach(id => {
            document.getElementById(id).classList.remove('hidden');
        });

        ['filesFound', 'entitiesGenerated', 'lineageRegistered'].forEach(id => {
            document.getElementById(id).classList.add('hidden');
        });

        // Reset stats
        document.getElementById('heroFileCount').textContent = '0';
        document.getElementById('totalFiles').textContent = '0';
        document.getElementById('lineageObjects').textContent = '0';
        document.getElementById('successRate').textContent = '0%';
        document.getElementById('fileCount').textContent = '0';
        document.getElementById('entityCount').textContent = '0';
        document.getElementById('entitiesCreated').textContent = '0';
        document.getElementById('avgProcessingTime').textContent = '0s';
        this.updateApiStatus('Ready', 'Awaiting connection');
        document.getElementById('affectedSystemsCount').textContent = '0';

        // Reset JSON validation badge
        this.updateJsonValidationBadge('default');

        // Clear displays
        document.getElementById('activityLog').innerHTML = '<div class="text-base-content/50">Waiting for automation to start...</div>';
        document.getElementById('fileList').innerHTML = '<div class="flex flex-col items-center justify-center h-full text-base-content/50"><i class="fas fa-folder-open text-4xl mb-3 opacity-30"></i><p class="text-center">No files scanned yet</p><p class="text-xs text-center mt-1">Click \'Start Scan\' to discover files</p></div>';
        document.getElementById('jsonViewer').style.display = 'none';
        document.getElementById('responseViewer').style.display = 'none';

        this.showToast('Demo reset successfully', 'info');
    }

    copyJSONToClipboard() {
        const jsonText = document.getElementById('jsonPayload').textContent;
        navigator.clipboard.writeText(jsonText).then(() => {
            this.showToast('JSON copied to clipboard!', 'success');
        }).catch(() => {
            this.showToast('Failed to copy JSON', 'error');
        });
    }

    downloadJSON() {
        if (!this.generatedJSON) {
            this.showToast('No JSON payload to download', 'error');
            return;
        }

        const jsonString = JSON.stringify(this.generatedJSON, null, 2);
        const blob = new Blob([jsonString], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        
        const a = document.createElement('a');
        a.href = url;
        a.download = `lineage-payload-${new Date().toISOString().split('T')[0]}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        
        this.showToast('JSON file downloaded successfully!', 'success');
        this.log('JSON payload downloaded to local file system', 'success');
    }

    generateGuid() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }

    generateRandomFiles() {
        const fileTypes = ['claims', 'providers', 'patients', 'procedures', 'medications', 'lab_results'];
        const fileSuffixes = ['daily', 'weekly', 'monthly', 'delta', 'full'];
        
        // Random file discovery (3-8 files) - not tied to complexity dropdown
        const fileCount = Math.floor(Math.random() * 6) + 3; // 3-8 files
        const today = new Date().toISOString().split('T')[0].replace(/-/g, '_');
        
        const files = [];
        const usedNames = new Set();
        
        for (let i = 0; i < fileCount; i++) {
            let fileName;
            let attempts = 0;
            
            do {
                const fileType = fileTypes[Math.floor(Math.random() * fileTypes.length)];
                const suffix = fileSuffixes[Math.floor(Math.random() * fileSuffixes.length)];
                fileName = `${fileType}_${suffix}_${today}.csv`;
                attempts++;
            } while (usedNames.has(fileName) && attempts < 10);
            
            usedNames.add(fileName);
            
            const fileSize = Math.floor(Math.random() * 4900000) + 100000;
            const schema = this.getSchemaForFileType(fileName);
            
            files.push({
                fileName: fileName,
                filePath: `adls://claimsdata/incoming/${fileName}`,
                fileSize: fileSize,
                lastModified: new Date(Date.now() - Math.random() * 3600000).toISOString(),
                schema: schema
            });
        }
        
        return files;
    }

    getIntelligentComplexity(fileName) {
        // Base complexity levels based on file types
        const complexityMap = {
            'claims': 5,
            'providers': 3,
            'patients': 5,
            'procedures': 2,
            'medications': 3,
            'lab_results': 2
        };

        // Get base complexity for file type
        let baseComplexity = 3; // Default
        for (const [fileType, complexity] of Object.entries(complexityMap)) {
            if (fileName.includes(fileType)) {
                baseComplexity = complexity;
                break;
            }
        }

        // Apply complexity multiplier from dropdown
        const complexitySelector = document.getElementById('complexityLevel');
        let multiplier = 1;
        
        if (complexitySelector) {
            const selectedValue = complexitySelector.value;
            switch (selectedValue) {
                case 'simple':
                    multiplier = 1; // Base entities (2 entities setting)
                    break;
                case 'standard':
                    multiplier = 1.5; // 1.5x multiplier (3 entities setting)
                    break;
                case 'complex':
                    multiplier = 2.5; // 2.5x multiplier (5 entities setting)
                    break;
                case 'enterprise':
                    multiplier = 3.5; // 3.5x multiplier (7 entities setting)
                    break;
                case 'auto':
                default:
                    multiplier = 1.5 + (Math.random() * 1); // Random 1.5x-2.5x
                    break;
            }
        }

        return Math.round(baseComplexity * multiplier);
    }
    
    getSchemaForFileType(fileName) {
        if (fileName.includes('claims')) {
            return ['claim_id', 'patient_id', 'provider_id', 'service_date', 'diagnosis_code', 'procedure_code', 'claim_amount', 'insurance_type', 'status'];
        } else if (fileName.includes('providers')) {
            return ['provider_id', 'provider_name', 'npi_number', 'specialty', 'address', 'city', 'state', 'zip_code', 'phone'];
        } else if (fileName.includes('patients')) {
            return ['patient_id', 'first_name', 'last_name', 'date_of_birth', 'gender', 'address', 'city', 'state', 'zip_code', 'insurance_id'];
        } else if (fileName.includes('procedures')) {
            return ['procedure_id', 'procedure_code', 'procedure_name', 'category', 'cost', 'duration_minutes'];
        } else if (fileName.includes('medications')) {
            return ['medication_id', 'drug_name', 'dosage', 'frequency', 'prescribed_date', 'prescriber_id'];
        } else if (fileName.includes('lab_results')) {
            return ['test_id', 'patient_id', 'test_type', 'result_value', 'reference_range', 'test_date'];
        } else {
            return ['id', 'data', 'timestamp', 'source'];
        }
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// Initialize simulation when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.lineageSimulation = new LineageSimulation();
});