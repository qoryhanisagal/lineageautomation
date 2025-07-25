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
        this.autoMode = true;
        this.columnLineageMode = false;
        this.columnTracker = null;
        this.schemaDriftDetected = false;
        this.columnMappings = [];

        this.initializeEventListeners();
    }

    initializeEventListeners() {
        document.getElementById('startScan').addEventListener('click', () => this.startAutomation());
        document.getElementById('generateJSON').addEventListener('click', () => this.generateJSONPayload());
        document.getElementById('registerLineage').addEventListener('click', () => this.registerInPurview());
        
        // Copy JSON button
        const copyButton = document.getElementById('copyJSON');
        if (copyButton) {
            copyButton.addEventListener('click', () => this.copyJSONToClipboard());
        }
        
        // Auto mode toggle
        const autoModeToggle = document.getElementById('autoModeToggle');
        if (autoModeToggle) {
            autoModeToggle.addEventListener('change', (e) => {
                this.autoMode = e.target.checked;
            });
        }
        
        // Column lineage toggle
        const columnLineageToggle = document.getElementById('columnLineageToggle');
        if (columnLineageToggle) {
            columnLineageToggle.addEventListener('change', (e) => {
                this.columnLineageMode = e.target.checked;
                const columnMappingCard = document.getElementById('columnMappingCard');
                
                if (this.columnLineageMode && window.ColumnLevelLineageTracker) {
                    this.columnTracker = new window.ColumnLevelLineageTracker({});
                    this.log('Column-level lineage tracking enabled', 'info');
                    
                    if (columnMappingCard) {
                        columnMappingCard.style.display = 'block';
                    }
                } else {
                    this.columnTracker = null;
                    this.log('Standard lineage tracking mode', 'info');
                    
                    if (columnMappingCard) {
                        columnMappingCard.style.display = 'none';
                    }
                }
            });
        }
    }

    async startAutomation() {
        if (this.autoMode) {
            await this.runFullAutomation();
        } else {
            await this.startDirectoryScan();
        }
    }

    async runFullAutomation() {
        if (this.isRunning) return;
        
        this.log('Starting full automation pipeline...', 'info');
        this.log('Mode: Automated Lineage Registration', 'info');
        
        // Run all steps automatically
        await this.startDirectoryScan();
        
        // Wait for scan to complete then auto-generate JSON
        if (this.discoveredFiles.length > 0 && this.autoMode) {
            await this.delay(1000);
            await this.generateJSONPayload();
            
            // Wait for JSON generation then auto-register
            if (this.generatedJSON && this.autoMode) {
                await this.delay(1000);
                await this.registerInPurview();
                
                this.log('Automation pipeline completed successfully!', 'success');
                this.showToast('Full automation completed! Lineage registered in Purview.', 'success');
            }
        }
    }

    log(message, type = 'info') {
        const activityLog = document.getElementById('activityLog');
        const timestamp = new Date().toLocaleTimeString();
        
        const icons = {
            info: '<i class="fas fa-info-circle text-info"></i>',
            success: '<i class="fas fa-check-circle text-success"></i>',
            warning: '<i class="fas fa-exclamation-triangle text-warning"></i>',
            error: '<i class="fas fa-times-circle text-error"></i>'
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

        // Simulate file discovery
        this.discoveredFiles = window.mockData.sampleFiles;
        
        this.log(`Scan complete! Found ${this.discoveredFiles.length} new files for processing`, 'success');
        
        // Update scanner status
        document.getElementById('scannerLoading').classList.add('hidden');
        document.getElementById('scannerComplete').classList.remove('hidden');
        document.getElementById('filesFound').classList.remove('hidden');
        document.getElementById('fileCount').textContent = this.discoveredFiles.length;
        
        // Update counters
        if (document.getElementById('heroFileCount')) {
            document.getElementById('heroFileCount').textContent = this.discoveredFiles.length;
        }
        if (document.getElementById('totalFiles')) {
            document.getElementById('totalFiles').textContent = this.discoveredFiles.length;
        }

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
    }

    populateFileList() {
        const fileList = document.getElementById('fileList');
        fileList.innerHTML = '';

        this.discoveredFiles.forEach((file, index) => {
            const fileCard = document.createElement('div');
            fileCard.className = 'card bg-base-200 shadow-sm';
            fileCard.innerHTML = `
                <div class="card-body p-4">
                    <div class="flex items-center justify-between">
                        <div>
                            <h4 class="font-semibold text-sm">${file.fileName}</h4>
                            <p class="text-xs text-base-content/70">${file.fileSize.toLocaleString()} bytes</p>
                        </div>
                        <div class="badge badge-sm badge-primary">CSV</div>
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
            
            this.log(`Processing: ${file.fileName}`);
            this.log(`Mapped to pipeline: ${pipelineConfig.pipelineName}`);
            
            await this.delay(800);
            
            totalEntities += 3; // Source + Process + Destination
        }

        this.log('Generating JSON payload for Purview registration...');
        
        await this.delay(1500);

        // Generate the actual JSON
        this.generatedJSON = this.createLineageJSON();
        
        this.log(`JSON payload generated successfully! Created ${totalEntities} entities`, 'success');
        
        // Update generator status  
        document.getElementById('generatorLoading').classList.add('hidden');
        document.getElementById('generatorComplete').classList.remove('hidden');
        document.getElementById('entitiesGenerated').classList.remove('hidden');
        document.getElementById('entityCount').textContent = totalEntities;
        
        // Update hero counter
        if (document.getElementById('lineageObjects')) {
            document.getElementById('lineageObjects').textContent = totalEntities;
        }

        // Show JSON viewer
        document.getElementById('jsonViewer').style.display = 'block';
        document.getElementById('jsonPayload').textContent = JSON.stringify(this.generatedJSON, null, 2);

        // IMPORTANT: Update Purview Registration status to show it's ready
        document.getElementById('purviewReady').innerHTML = '<i class="fas fa-check-circle text-success mr-2"></i>Ready to register';
        document.getElementById('purviewReady').classList.remove('text-base-content/70');
        document.getElementById('purviewReady').classList.add('text-success');
        
        // Enable next step
        document.getElementById('registerLineage').disabled = false;
        
        this.showToast('JSON payload generated successfully!', 'success');
        
        this.isRunning = false;
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
            const timestamp = new Date().toISOString();
            
            // Source Dataset
            entities.push({
                typeName: "DataSet",
                attributes: {
                    qualifiedName: file.filePath,
                    name: file.fileName,
                    description: `Source file for ${pipelineConfig.transformationType}`,
                    owner: "External_Data_Provider",
                    createTime: timestamp,
                    fileSize: file.fileSize,
                    format: "CSV",
                    schema: file.schema
                },
                guid: this.generateGuid(),
                status: "ACTIVE"
            });

            // Process
            entities.push({
                typeName: "Process",
                attributes: {
                    qualifiedName: `adf://pipelines/${pipelineConfig.pipelineName}`,
                    name: pipelineConfig.pipelineName,
                    description: `Automated ${pipelineConfig.transformationType} process`,
                    processType: "ETL_PIPELINE",
                    inputs: [file.filePath],
                    outputs: [`sql://server.database.windows.net/database/dbo/${pipelineConfig.destinationTable}`],
                    createTime: timestamp
                },
                guid: this.generateGuid(),
                status: "ACTIVE"
            });

            // Destination Table
            entities.push({
                typeName: "Table",
                attributes: {
                    qualifiedName: `sql://server.database.windows.net/database/dbo/${pipelineConfig.destinationTable}`,
                    name: pipelineConfig.destinationTable,
                    description: `Processed data from ${file.fileName}`,
                    schema: "dbo",
                    tableType: "MANAGED_TABLE",
                    createTime: timestamp
                },
                guid: this.generateGuid(),
                status: "ACTIVE"
            });
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
        document.getElementById('apiResponse').textContent = JSON.stringify(mockResponse, null, 2);
        
        this.showToast('Lineage registration completed!', 'success');
        this.log('Lineage relationships now visible in Purview Data Map', 'success');
        
        this.isRunning = false;
    }

    copyJSONToClipboard() {
        const jsonText = document.getElementById('jsonPayload').textContent;
        navigator.clipboard.writeText(jsonText).then(() => {
            this.showToast('JSON copied to clipboard!', 'success');
        });
    }

    generateGuid() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// Initialize simulation when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.lineageSimulation = new LineageSimulation();
});