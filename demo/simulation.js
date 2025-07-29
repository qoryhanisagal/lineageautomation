// ===================================================================
// Frontend Simulation Logic for Lineage Automation Framework
// Enterprise demonstration platform
// ===================================================================

class LineageSimulation {
    constructor() {
        this.currentStep = 0;
        this.discoveredFiles = [];
        this.generatedJSON = null;
        this.isRunning = false;
        this.currentApiResponse = null;
        this.detectedSchemaDrift = [];
        this.baselineSchemas = this.initializeBaselineSchemas();
        
        // Stakeholder notification system
        this.stakeholderMap = this.initializeStakeholderMapping();
        this.notificationQueue = [];
        this.notificationStatus = new Map(); // Track delivery status

        // Initialize column-level lineage tracker
        this.columnTracker = null;
        if (typeof ColumnLevelLineageTracker !== 'undefined') {
            this.columnTracker = new ColumnLevelLineageTracker({
                purviewAccount: 'demo-purview',
                tenantId: 'demo-tenant'
            });
        }

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
        
        // Initialize column tracker if not already done
        if (!this.columnTracker && typeof ColumnLevelLineageTracker !== 'undefined') {
            this.columnTracker = new ColumnLevelLineageTracker({
                purviewAccount: 'demo-purview',
                tenantId: 'demo-tenant'
            });
        }
    }

    updateColumnLineageMode() {
        const isEnabled = document.getElementById('columnLineageToggle').checked;
        const columnMappingCard = document.getElementById('columnMappingCard');
        
        if (isEnabled) {
            this.log('🟥 Column-level lineage tracking enabled for Azure SQL tables', 'info');
            this.showColumnLineageAnalysis();
        } else {
            this.log('Column-level lineage tracking disabled', 'info');
            if (columnMappingCard) {
                columnMappingCard.style.display = 'none';
            }
            // Hide schema drift section when column lineage is disabled
            this.hideSchemaDriftSection();
            this.detectedSchemaDrift = []; // Clear drift data
        }
    }

    async showColumnLineageAnalysis() {
        // Display Azure SQL column lineage mapping table
        const columnMappingCard = document.getElementById('columnMappingCard');
        if (columnMappingCard) {
            columnMappingCard.style.display = 'block';
            this.populateColumnLineageTable();
            
            // Schema drift happens during file discovery, not here
            // Column lineage can enhance drift analysis but doesn't trigger it
            if (this.detectedSchemaDrift.length > 0) {
                this.log('📊 Column mappings established - enhancing existing drift analysis with mapping context', 'info');
                // Add additional drift analysis specific to column mappings
            } else {
                this.log('📊 Column mappings established - no schema drift detected during discovery', 'info');
            }
        }
    }

    populateColumnLineageTable() {
        const tableBody = document.getElementById('columnMappingTable');
        if (!tableBody) return;

        // Generate dynamic column mappings based on discovered files
        const columnMappings = this.generateDynamicColumnMappings();
        
        tableBody.innerHTML = '';
        columnMappings.forEach((mapping, index) => {
            const row = document.createElement('tr');
            const impactEmoji = this.getImpactEmoji(mapping.impact);
            const transformationColor = this.getTransformationColor(mapping.transformation);
            const impactColor = this.getImpactColor(mapping.impact);
            
            row.className = 'hover:bg-base-200 transition-colors';
            row.innerHTML = `
                <td class="text-sm font-medium">${mapping.sourceColumn}</td>
                <td class="text-sm"><span class="badge ${transformationColor} badge-sm">${mapping.transformation}</span></td>
                <td class="text-sm font-medium text-primary">${mapping.targetColumn}</td>
                <td class="text-sm">
                    <div class="font-mono text-xs text-info">${mapping.azureSqlDataType}</div>
                    <div class="text-xs text-base-content/60">${mapping.azureSqlConstraints}</div>
                </td>
                <td class="text-sm">
                    <div class="tooltip tooltip-left" data-tip="${mapping.businessRule}">
                        <div class="text-xs cursor-help">${mapping.businessRuleShort}</div>
                    </div>
                </td>
                <td class="text-sm">
                    ${impactEmoji} <span class="badge ${impactColor} badge-xs">${mapping.impact}</span>
                    <div class="text-xs text-base-content/50 mt-1">
                        <button class="btn btn-ghost btn-xs" onclick="window.lineageSimulation.showColumnDetails('${mapping.sourceColumn}')">
                            <i class="fas fa-info-circle text-xs"></i>
                        </button>
                    </div>
                </td>
            `;
            tableBody.appendChild(row);
        });

        // Update Azure SQL connection stats
        this.updateAzureSqlStats(columnMappings.length);
        
        this.log(`🟥 Generated ${columnMappings.length} dynamic column mappings from discovered files`, 'success');
    }

    generateDynamicColumnMappings() {
        if (!this.discoveredFiles || this.discoveredFiles.length === 0) {
            return this.getDefaultColumnMappings();
        }

        const mappings = [];
        
        this.discoveredFiles.forEach(file => {
            const fileType = this.getFileType(file.fileName);
            const fileColumns = this.getSchemaForFileType(file.fileName);
            const targetMappings = this.getAzureSqlMappings(fileType);
            
            fileColumns.slice(0, 3).forEach((sourceColumn, index) => {
                if (targetMappings[index]) {
                    mappings.push({
                        sourceColumn: sourceColumn,
                        targetColumn: targetMappings[index].target,
                        transformation: targetMappings[index].transformation,
                        impact: targetMappings[index].impact,
                        azureSqlDataType: targetMappings[index].dataType,
                        azureSqlConstraints: targetMappings[index].constraints,
                        businessRule: targetMappings[index].businessRule,
                        businessRuleShort: targetMappings[index].businessRuleShort,
                        fileSource: file.fileName
                    });
                }
            });
        });

        return mappings.length > 0 ? mappings : this.getDefaultColumnMappings();
    }

    getAzureSqlMappings(fileType) {
        const mappings = {
            'claims': [
                {
                    target: 'claim_identifier',
                    transformation: 'RENAME_VALIDATE',
                    impact: 'CRITICAL',
                    dataType: 'VARCHAR(50) NOT NULL',
                    constraints: 'PRIMARY KEY, UNIQUE INDEX',
                    businessRule: 'Claims must have unique identifiers for tracking through the healthcare system',
                    businessRuleShort: 'Unique claim tracking'
                },
                {
                    target: 'patient_reference_id',
                    transformation: 'ANONYMIZATION',
                    impact: 'HIGH',
                    dataType: 'VARCHAR(64) NOT NULL',
                    constraints: 'FOREIGN KEY, INDEX IX_patient',
                    businessRule: 'Patient IDs are anonymized for HIPAA compliance while maintaining referential integrity',
                    businessRuleShort: 'HIPAA anonymization'
                },
                {
                    target: 'provider_network_id',
                    transformation: 'LOOKUP_ENRICHMENT',
                    impact: 'MEDIUM',
                    dataType: 'VARCHAR(32)',
                    constraints: 'FOREIGN KEY, INDEX IX_provider',
                    businessRule: 'Provider IDs are enriched with network information for contract validation',
                    businessRuleShort: 'Network validation'
                }
            ],
            'providers': [
                {
                    target: 'provider_master_id',
                    transformation: 'DEDUPLICATION',
                    impact: 'HIGH',
                    dataType: 'VARCHAR(32) NOT NULL',
                    constraints: 'PRIMARY KEY, UNIQUE',
                    businessRule: 'Provider deduplication ensures single source of truth for billing',
                    businessRuleShort: 'Master record creation'
                },
                {
                    target: 'npi_validated',
                    transformation: 'EXTERNAL_VALIDATION',
                    impact: 'CRITICAL',
                    dataType: 'VARCHAR(10) NOT NULL',
                    constraints: 'CHECK (LEN(npi_validated) = 10)',
                    businessRule: 'NPI numbers validated against CMS registry for compliance',
                    businessRuleShort: 'CMS validation'
                },
                {
                    target: 'specialty_standardized',
                    transformation: 'STANDARDIZATION',
                    impact: 'MEDIUM',
                    dataType: 'VARCHAR(100)',
                    constraints: 'FOREIGN KEY specialty_codes',
                    businessRule: 'Specialty codes standardized to industry taxonomy',
                    businessRuleShort: 'Taxonomy mapping'
                }
            ],
            'patients': [
                {
                    target: 'patient_master_key',
                    transformation: 'HASH_GENERATION',
                    impact: 'CRITICAL',
                    dataType: 'VARCHAR(64) NOT NULL',
                    constraints: 'PRIMARY KEY, UNIQUE',
                    businessRule: 'Patient master key generated from hashed PII for privacy protection',
                    businessRuleShort: 'Privacy protection'
                },
                {
                    target: 'demographics_encrypted',
                    transformation: 'FIELD_ENCRYPTION',
                    impact: 'HIGH',
                    dataType: 'VARBINARY(MAX)',
                    constraints: 'ENCRYPTED, NOT NULL',
                    businessRule: 'Demographics encrypted at rest for enhanced security',
                    businessRuleShort: 'Data encryption'
                },
                {
                    target: 'address_standardized',
                    transformation: 'ADDRESS_CLEANSING',
                    impact: 'MEDIUM',
                    dataType: 'NVARCHAR(200)',
                    constraints: 'INDEX IX_address_lookup',
                    businessRule: 'Addresses standardized using USPS validation service',
                    businessRuleShort: 'USPS validation'
                }
            ]
        };

        return mappings[fileType] || mappings['claims'];
    }

    getDefaultColumnMappings() {
        return [
            {
                sourceColumn: 'claim_id',
                targetColumn: 'claim_identifier',
                transformation: 'RENAME_VALIDATE',
                impact: 'CRITICAL',
                azureSqlDataType: 'VARCHAR(50) NOT NULL',
                azureSqlConstraints: 'PRIMARY KEY, UNIQUE INDEX',
                businessRule: 'Claims must have unique identifiers for tracking',
                businessRuleShort: 'Unique tracking'
            }
        ];
    }

    updateAzureSqlStats(columnCount) {
        // Update the Azure SQL connection stats with real-time info
        const serverElement = document.getElementById('azureSqlServer');
        const databaseElement = document.getElementById('azureSqlDatabase');
        const schemaVersionElement = document.getElementById('azureSqlSchemaVersion');
        const resourceGroupElement = document.getElementById('azureResourceGroup');

        if (serverElement) serverElement.textContent = 'healthcare-sql-server.database.windows.net';
        if (databaseElement) databaseElement.textContent = 'ClaimsDB';
        if (schemaVersionElement) schemaVersionElement.textContent = '2.1';
        if (resourceGroupElement) resourceGroupElement.textContent = 'rg-healthcare-prod';
    }

    showColumnDetails(columnName) {
        this.log(`📋 Showing detailed lineage for column: ${columnName}`, 'info');
        this.showToast(`Column Details: ${columnName} - See activity log for lineage trace`, 'info');

        // Logging with column-specific details
        this.log(`  └─ Source: CSV file column`, 'info');
        this.log(`  └─ Transformations: Validation → Cleansing → Type conversion`, 'info');
        this.log(`  └─ Target: Azure SQL table column with constraints`, 'info');
        this.log(`  └─ Data lineage tracked through Purview`, 'success');
    }

    exportColumnMappings() {
        const columnMappings = this.generateDynamicColumnMappings();
        
        if (columnMappings.length === 0) {
            this.showToast('No column mappings to export', 'warning');
            return;
        }

        // Create CSV content
        const csvHeader = 'Source Column,Transformation,Target Column,Data Type,Constraints,Business Rule,Impact,File Source\n';
        const csvContent = columnMappings.map(mapping => 
            `"${mapping.sourceColumn}","${mapping.transformation}","${mapping.targetColumn}","${mapping.azureSqlDataType}","${mapping.azureSqlConstraints}","${mapping.businessRule}","${mapping.impact}","${mapping.fileSource || 'N/A'}"`
        ).join('\n');

        const fullCsv = csvHeader + csvContent;
        
        // Create and download file
        const blob = new Blob([fullCsv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        
        const a = document.createElement('a');
        a.href = url;
        a.download = `azure-sql-column-mappings-${new Date().toISOString().split('T')[0]}.csv`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        
        this.showToast('Column mappings exported successfully!', 'success');
        this.log(`🟢 Exported ${columnMappings.length} column mappings to CSV file`, 'success');
    }

    initializeBaselineSchemas() {
        // Baseline schemas for drift detection
        return {
            'claims': {
                version: '2.1',
                lastUpdated: '2024-07-01',
                columns: {
                    'claim_id': { type: 'VARCHAR', length: 50, nullable: false, constraints: ['PRIMARY_KEY'] },
                    'patient_id': { type: 'VARCHAR', length: 32, nullable: false, constraints: ['FOREIGN_KEY'] },
                    'provider_id': { type: 'VARCHAR', length: 32, nullable: false, constraints: ['FOREIGN_KEY'] },
                    'service_date': { type: 'DATE', nullable: false, constraints: [] },
                    'diagnosis_code': { type: 'VARCHAR', length: 10, nullable: false, constraints: ['ICD10_VALID'] },
                    'procedure_code': { type: 'VARCHAR', length: 10, nullable: true, constraints: ['CPT_VALID'] },
                    'claim_amount': { type: 'DECIMAL', precision: 10, scale: 2, nullable: false, constraints: ['POSITIVE'] },
                    'insurance_type': { type: 'VARCHAR', length: 20, nullable: false, constraints: ['ENUM'] },
                    'status': { type: 'VARCHAR', length: 15, nullable: false, constraints: ['ENUM'] }
                },
                businessRules: {
                    'claim_amount': 'Must be positive and less than $1,000,000',
                    'diagnosis_code': 'Must be valid ICD-10 code',
                    'service_date': 'Cannot be future date or older than 2 years'
                }
            },
            'providers': {
                version: '1.8',
                lastUpdated: '2024-06-15',
                columns: {
                    'provider_id': { type: 'VARCHAR', length: 32, nullable: false, constraints: ['PRIMARY_KEY'] },
                    'provider_name': { type: 'VARCHAR', length: 200, nullable: false, constraints: [] },
                    'npi_number': { type: 'VARCHAR', length: 10, nullable: false, constraints: ['NPI_VALID', 'UNIQUE'] },
                    'specialty': { type: 'VARCHAR', length: 100, nullable: false, constraints: ['TAXONOMY_VALID'] },
                    'address': { type: 'VARCHAR', length: 300, nullable: false, constraints: [] },
                    'city': { type: 'VARCHAR', length: 100, nullable: false, constraints: [] },
                    'state': { type: 'VARCHAR', length: 2, nullable: false, constraints: ['STATE_CODE'] },
                    'zip_code': { type: 'VARCHAR', length: 10, nullable: false, constraints: ['ZIP_VALID'] },
                    'phone': { type: 'VARCHAR', length: 15, nullable: true, constraints: ['PHONE_FORMAT'] }
                },
                businessRules: {
                    'npi_number': 'Must be valid 10-digit NPI from CMS registry',
                    'specialty': 'Must match approved medical taxonomy codes',
                    'state': 'Must be valid US state abbreviation'
                }
            },
            'patients': {
                version: '3.2',
                lastUpdated: '2024-07-10',
                columns: {
                    'patient_id': { type: 'VARCHAR', length: 32, nullable: false, constraints: ['PRIMARY_KEY'] },
                    'first_name': { type: 'VARCHAR', length: 50, nullable: false, constraints: ['ENCRYPTED'] },
                    'last_name': { type: 'VARCHAR', length: 50, nullable: false, constraints: ['ENCRYPTED'] },
                    'date_of_birth': { type: 'DATE', nullable: false, constraints: ['HIPAA_PROTECTED'] },
                    'gender': { type: 'VARCHAR', length: 1, nullable: false, constraints: ['ENUM'] },
                    'address': { type: 'VARCHAR', length: 300, nullable: true, constraints: ['ENCRYPTED'] },
                    'city': { type: 'VARCHAR', length: 100, nullable: true, constraints: [] },
                    'state': { type: 'VARCHAR', length: 2, nullable: true, constraints: ['STATE_CODE'] },
                    'zip_code': { type: 'VARCHAR', length: 10, nullable: true, constraints: ['ZIP_VALID'] },
                    'insurance_id': { type: 'VARCHAR', length: 50, nullable: true, constraints: ['ENCRYPTED'] }
                },
                businessRules: {
                    'date_of_birth': 'HIPAA protected - must be anonymized in reporting',
                    'first_name': 'PII - requires encryption at rest',
                    'last_name': 'PII - requires encryption at rest'
                }
            }
        };
    }

    initializeStakeholderMapping() {
        // Map data systems to stakeholders who should be notified of changes
        return {
            // System-to-stakeholder mappings
            systems: {
                'Power BI Claims Dashboard': {
                    owners: ['sarah.chen@healthcare.com', 'mike.johnson@healthcare.com'],
                    type: 'BI_REPORT',
                    teams_channel: 'https://teams.microsoft.com/l/channel/analytics-team',
                    urgency: 'HIGH',
                    dependencies: ['claims', 'providers']
                },
                'Tableau Executive Reports': {
                    owners: ['exec.team@healthcare.com', 'cfo@healthcare.com'],
                    type: 'BI_REPORT', 
                    teams_channel: 'https://teams.microsoft.com/l/channel/executive-reports',
                    urgency: 'CRITICAL',
                    dependencies: ['claims', 'patients']
                },
                'Claims Processing API': {
                    owners: ['dev.team@healthcare.com', 'api.support@healthcare.com'],
                    type: 'APPLICATION',
                    teams_channel: 'https://teams.microsoft.com/l/channel/dev-team',
                    urgency: 'HIGH',
                    dependencies: ['claims', 'providers', 'patients']
                },
                'Azure ML Model Training': {
                    owners: ['ml.team@healthcare.com', 'data.science@healthcare.com'],
                    type: 'ML_PIPELINE',
                    teams_channel: 'https://teams.microsoft.com/l/channel/ml-team',
                    urgency: 'MEDIUM',
                    dependencies: ['claims', 'patients']
                },
                'Compliance Reporting System': {
                    owners: ['compliance@healthcare.com', 'audit@healthcare.com'],
                    type: 'COMPLIANCE',
                    teams_channel: 'https://teams.microsoft.com/l/channel/compliance',
                    urgency: 'CRITICAL',
                    dependencies: ['claims', 'providers', 'patients']
                }
            },
            
            // File-type to affected systems mapping
            fileTypeDependencies: {
                'claims': ['Power BI Claims Dashboard', 'Claims Processing API', 'Azure ML Model Training', 'Compliance Reporting System'],
                'providers': ['Power BI Claims Dashboard', 'Claims Processing API', 'Compliance Reporting System'],
                'patients': ['Tableau Executive Reports', 'Claims Processing API', 'Azure ML Model Training', 'Compliance Reporting System']
            },
            
            // Escalation rules based on change severity
            escalationRules: {
                'CRITICAL': {
                    immediate: ['teams', 'email'],
                    escalateAfter: 15, // minutes
                    escalateTo: ['cto@healthcare.com', 'cio@healthcare.com']
                },
                'HIGH': {
                    immediate: ['teams', 'email'], 
                    escalateAfter: 60, // minutes
                    escalateTo: ['data.governance@healthcare.com']
                },
                'MEDIUM': {
                    immediate: ['teams'],
                    escalateAfter: 240, // minutes 
                    escalateTo: ['data.governance@healthcare.com']
                },
                'LOW': {
                    immediate: ['email'],
                    escalateAfter: 1440, // 24 hours
                    escalateTo: []
                }
            }
        };
    }

    // Multi-channel notification framework
    async notifyAffectedStakeholders(drift) {
        const fileType = this.getFileType(drift.fileName);
        const affectedSystems = this.stakeholderMap.fileTypeDependencies[fileType] || [];
        
        this.log(`📧 Notifying stakeholders about schema drift in ${drift.fileName}...`, 'info');
        
        let totalNotificationsSent = 0;
        
        for (const systemName of affectedSystems) {
            const system = this.stakeholderMap.systems[systemName];
            if (system) {
                const notifications = await this.sendSystemNotifications(systemName, system, drift);
                totalNotificationsSent += notifications;
            }
        }
        
        this.log(`🟢 Sent ${totalNotificationsSent} notifications to affected stakeholders`, 'success');
        this.showToast(`${totalNotificationsSent} stakeholders notified about schema drift`, 'info');
        
        return totalNotificationsSent;
    }

    async sendSystemNotifications(systemName, system, drift) {
        let notificationCount = 0;
        
        // Determine notification channels based on urgency and change severity
        const channels = this.getNotificationChannels(system.urgency, drift.severity);
        
        for (const channel of channels) {
            switch (channel) {
                case 'email':
                    notificationCount += await this.sendEmailNotifications(systemName, system, drift);
                    break;
                case 'teams':
                    notificationCount += await this.sendTeamsNotifications(systemName, system, drift); 
                    break;
                case 'slack':
                    notificationCount += await this.sendSlackNotifications(systemName, system, drift);
                    break;
            }
        }
        
        return notificationCount;
    }

    getNotificationChannels(systemUrgency, changeSeverity) {
        const escalationRule = this.stakeholderMap.escalationRules[changeSeverity];
        return escalationRule ? escalationRule.immediate : ['email'];
    }

    async sendEmailNotifications(systemName, system, drift) {
        this.log(`🟡 Sending email notifications for ${systemName}...`, 'info');
        
        let emailsSent = 0;
        for (const email of system.owners) {
            const emailContent = this.generateEmailTemplate(systemName, system, drift, email);
            
            // Simulate email sending
            await this.delay(200);
            this.log(`  └─ Email sent to ${email}`, 'success');
            
            // Track notification status
            this.trackNotification('email', email, systemName, drift, 'SENT');
            this.logStakeholderActivity('Email Sent', systemName, email, drift);
            emailsSent++;
        }
        
        return emailsSent;
    }

    async sendTeamsNotifications(systemName, system, drift) {
        this.log(`🟪 Sending Teams notification for ${systemName}...`, 'info');
        
        const teamsMessage = this.generateTeamsTemplate(systemName, system, drift);
        
        // Simulate Teams webhook call
        await this.delay(300);
        this.log(`  └─ Teams message posted to ${systemName} channel`, 'success');
        
        // Track notification status  
        this.trackNotification('teams', system.teams_channel, systemName, drift, 'SENT');
        this.logStakeholderActivity('Teams Alert', systemName, `${systemName} Channel`, drift);
        
        return 1;
    }

    async sendSlackNotifications(systemName, system, drift) {
        this.log(`🟧 Sending Slack notification for ${systemName}...`, 'info');
        
        // Simulate Slack API call
        await this.delay(250);
        this.log(`  └─ Slack message sent to ${systemName} workspace`, 'success');
        
        this.trackNotification('slack', 'slack-workspace', systemName, drift, 'SENT');
        
        return 1;
    }

    trackNotification(channel, recipient, system, drift, status) {
        const notificationId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        
        this.notificationStatus.set(notificationId, {
            id: notificationId,
            channel,
            recipient,
            system,
            drift: drift.fileName,
            status,
            timestamp: new Date().toISOString(),
            acknowledged: false
        });
        
        // Add to notification queue for tracking
        this.notificationQueue.push({
            id: notificationId,
            type: 'SCHEMA_DRIFT',
            system,
            recipient,
            timestamp: new Date().toISOString()
        });
    }

    detectSchemaDrift(fileName, currentSchema) {
        const fileType = this.getFileType(fileName);
        const baseline = this.baselineSchemas[fileType];
        
        if (!baseline) {
            return { hasDrift: false, changes: [] };
        }

        const driftChanges = [];
        const currentColumns = new Set(currentSchema);
        const baselineColumns = new Set(Object.keys(baseline.columns));

        // Detect new columns (additions)
        currentColumns.forEach(column => {
            if (!baselineColumns.has(column)) {
                driftChanges.push({
                    type: 'COLUMN_ADDED',
                    column: column,
                    severity: this.assessColumnSeverity(column, fileType),
                    impact: this.assessImpact(column, fileType, 'ADDED'),
                    recommendation: this.getRecommendation(column, fileType, 'ADDED'),
                    businessJustification: this.getBusinessJustification(column, fileType, 'ADDED')
                });
            }
        });

        // Detect removed columns (deletions)
        baselineColumns.forEach(column => {
            if (!currentColumns.has(column)) {
                driftChanges.push({
                    type: 'COLUMN_REMOVED',
                    column: column,
                    severity: 'HIGH', // Removals are always high severity
                    impact: this.assessImpact(column, fileType, 'REMOVED'),
                    recommendation: this.getRecommendation(column, fileType, 'REMOVED'),
                    businessJustification: this.getBusinessJustification(column, fileType, 'REMOVED')
                });
            }
        });

        return {
            hasDrift: driftChanges.length > 0,
            changes: driftChanges,
            fileType: fileType,
            fileName: fileName,
            baselineVersion: baseline.version,
            detectedAt: new Date().toISOString()
        };
    }

    assessColumnSeverity(column, fileType) {
        // Severity assessment
        const criticalKeywords = ['patient_id', 'ssn', 'medical_record', 'diagnosis', 'prescription'];
        const hipaaKeywords = ['name', 'address', 'phone', 'email', 'dob', 'birth'];
        const billingKeywords = ['amount', 'cost', 'charge', 'payment', 'insurance'];

        const columnLower = column.toLowerCase();

        if (criticalKeywords.some(keyword => columnLower.includes(keyword))) {
            return 'CRITICAL';
        }
        if (hipaaKeywords.some(keyword => columnLower.includes(keyword))) {
            return 'HIGH';
        }
        if (billingKeywords.some(keyword => columnLower.includes(keyword))) {
            return 'MEDIUM';
        }
        return 'LOW';
    }

    assessImpact(column, fileType, changeType) {
        const impacts = [];
        
        if (changeType === 'ADDED') {
            impacts.push(`${fileType} processing pipeline may need updates`);
            impacts.push('Downstream Azure SQL tables require schema modifications');
            impacts.push('Data validation rules need review');
            
            if (this.assessColumnSeverity(column, fileType) === 'CRITICAL') {
                impacts.push('HIPAA compliance assessment required');
                impacts.push('Security team approval needed');
            }
        } else if (changeType === 'REMOVED') {
            impacts.push(`Existing reports referencing ${column} will fail`);
            impacts.push('Data transformation pipelines need adjustment');
            impacts.push('Historical data analysis may be affected');
        }

        return impacts;
    }

    getRecommendation(column, fileType, changeType) {
        if (changeType === 'ADDED') {
            const severity = this.assessColumnSeverity(column, fileType);
            if (severity === 'CRITICAL') {
                return 'Require security review and HIPAA assessment before approval';
            } else if (severity === 'HIGH') {
                return 'Apply data protection measures and update privacy documentation';
            } else {
                return 'Review business justification and update data dictionary';
            }
        } else {
            return 'Verify removal is intentional and update all dependent systems';
        }
    }

    getBusinessJustification(column, fileType, changeType) {
        if (changeType === 'ADDED') {
            return `New ${column} field added to ${fileType} data - likely due to regulatory updates or business requirement changes`;
        } else {
            return `${column} field removed from ${fileType} data - may indicate data source changes or privacy compliance updates`;
        }
    }

    // Notification for different channels
    generateEmailTemplate(systemName, system, drift, recipientEmail) {
        const severityEmoji = {
            'CRITICAL': '🔴',
            'HIGH': '🟠',
            'MEDIUM': '🔵',
            'LOW': '🟣'
        }[drift.severity] || '🔶';

        return {
            to: recipientEmail,
            subject: `${severityEmoji} Schema Drift Alert: ${systemName} Impact`,
            html: `
                <h2>Schema Drift Detected</h2>
                <p>Hello,</p>
                <p>A schema change has been detected in <strong>${drift.fileName}</strong> that may impact your system:</p>
                
                <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin: 10px 0;">
                    <h3>🟥 Affected System: ${systemName}</h3>
                    <p><strong>File:</strong> ${drift.fileName}</p>
                    <p><strong>Severity:</strong> ${drift.severity}</p>
                    <p><strong>Changes:</strong> ${drift.changes.length} column changes detected</p>
                </div>

                <h4>🔺Required Actions:</h4>
                <ul>
                    ${drift.changes.map(change => `
                        <li><strong>${change.type}:</strong> ${change.column} (${change.severity})</li>
                        <li style="margin-left: 20px; color: #666;">${change.recommendation}</li>
                    `).join('')}
                </ul>

                <p><strong>Next Steps:</strong></p>
                <ol>
                    <li>Review the schema changes in the lineage automation tool</li>
                    <li>Assess impact on your ${system.type.toLowerCase()}</li>
                    <li>Approve or reject changes as appropriate</li>
                    <li>Update your system if changes are approved</li>
                </ol>

                <p>For immediate questions, please contact the Data Governance team.</p>
                
                <hr>
                <small>This is an automated notification from the Healthcare Data Lineage System</small>
            `,
            text: `Schema Drift Alert: ${systemName}\n\nFile: ${drift.fileName}\nSeverity: ${drift.severity}\nChanges: ${drift.changes.length} detected\n\nPlease review and take appropriate action.`
        };
    }

    generateTeamsTemplate(systemName, system, drift) {
        const severityColor = {
            'CRITICAL': 'attention',
            'HIGH': 'warning',
            'MEDIUM': 'accent', 
            'LOW': 'good'
        }[drift.severity] || 'default';

        return {
            '@type': 'MessageCard',
            '@context': 'http://schema.org/extensions',
            summary: `Schema Drift Alert: ${systemName}`,
            themeColor: severityColor === 'attention' ? 'FF0000' : severityColor === 'warning' ? 'FFA500' : '0078D4',
            sections: [
                {
                    activityTitle: `🔺 Schema Drift Alert`,
                    activitySubtitle: `${systemName} may be impacted`,
                    facts: [
                        { name: 'File:', value: drift.fileName },
                        { name: 'Severity:', value: drift.severity },
                        { name: 'Changes:', value: `${drift.changes.length} column changes` },
                        { name: 'System Type:', value: system.type }
                    ]
                },
                {
                    title: 'Detected Changes:',
                    text: drift.changes.map(change => 
                        `• **${change.type}**: ${change.column} (${change.severity})`
                    ).join('\n')
                }
            ],
            potentialAction: [
                {
                    '@type': 'OpenUri',
                    name: 'Review Changes',
                    targets: [{ os: 'default', uri: 'https://your-lineage-tool.com/schema-drift' }]
                },
                {
                    '@type': 'HttpPOST', 
                    name: 'Acknowledge Alert',
                    target: 'https://your-api.com/acknowledge'
                }
            ]
        };
    }

    async analyzeFileSchemas() {
        this.log('⚫️ Analyzing file schemas for drift detection (using established column mappings)...', 'info');
        this.log(`🟢 Discovered files count: ${this.discoveredFiles.length}`, 'info');
        
        for (const file of this.discoveredFiles) {
            this.log(`⚫️ Analyzing file: ${file.fileName}`, 'info');
            
            // Get current schema for the file
            const currentSchema = this.getSchemaForFileType(file.fileName);
            this.log(`🟦 Current schema: [${currentSchema.join(', ')}]`, 'info');
            
            // Simulate schema drift scenarios (80% chance per file for testing)
            const randomValue = Math.random();
            const shouldSimulateDrift = randomValue < 0.8;
            this.log(`🟣 Drift simulation roll: ${randomValue.toFixed(3)} (threshold: 0.8) → ${shouldSimulateDrift ? 'DRIFT' : 'NO DRIFT'}`, 'info');
            
            if (shouldSimulateDrift) {
                // Create modified schema to simulate drift
                this.log(`⚫️ Simulating schema drift for ${file.fileName}...`, 'info');
                const modifiedSchema = this.simulateSchemaDrift(currentSchema, file.fileName);
                this.log(`🟡 Modified schema: [${modifiedSchema.join(', ')}]`, 'info');
                
                const driftResult = this.detectSchemaDrift(file.fileName, modifiedSchema);
                this.log(`🟢 Drift detection result: hasDrift=${driftResult.hasDrift}, changes=${driftResult.changes.length}`, 'info');
                
                if (driftResult.hasDrift) {
                    this.detectedSchemaDrift.push(driftResult);
                    this.log(`🟠 Schema drift detected in ${file.fileName}`, 'warning');
                } else {
                    this.log(`⛔️ Drift simulation failed - no changes detected`, 'warning');
                }
            } else {
                // No drift - schema matches baseline
                this.log(`🟢 No drift simulation for ${file.fileName} - checking baseline`, 'info');
                const driftResult = this.detectSchemaDrift(file.fileName, currentSchema);
                if (driftResult.hasDrift) {
                    this.detectedSchemaDrift.push(driftResult);
                    this.log(`🟥 Unexpected drift detected in baseline schema for ${file.fileName}`, 'warning');
                }
            }

            await this.delay(200); // Small delay for simulation
        }
        
        if (this.detectedSchemaDrift.length > 0) {
            this.log(`🟢 Schema drift analysis complete: ${this.detectedSchemaDrift.length} drift events detected (post-mapping validation)`, 'warning');
            this.showSchemaDriftSection();
            this.showToast(`Schema drift detected in ${this.detectedSchemaDrift.length} files - Column mappings may need updates`, 'warning');

            // Notify affected stakeholders
            this.log('🟣 Initiating stakeholder notification process...', 'info');
            this.notifyAllAffectedStakeholders();
        } else {
            this.log('🟢 Schema analysis complete: No drift detected', 'success');
            this.hideSchemaDriftSection();
        }
    }

    async notifyAllAffectedStakeholders() {
        let totalNotifications = 0;
        
        for (const drift of this.detectedSchemaDrift) {
            const notificationsSent = await this.notifyAffectedStakeholders(drift);
            totalNotifications += notificationsSent;
            
            // Add delay between drift notifications for realism
            await this.delay(500);
        }

        this.log(`🔵 Stakeholder notification complete: ${totalNotifications} total notifications sent`, 'success');

        // UI Notification Summary
        this.updateNotificationStatusUI();
    }

    simulateSchemaDrift(originalSchema, fileName) {
        const fileType = this.getFileType(fileName);
        this.log(`🔶 File type detected: ${fileType}`, 'info');
        
        const driftScenarios = this.getHealthcareDriftScenarios(fileType);
        this.log(`🟢 Available drift scenarios: ${driftScenarios.length}`, 'info');

        // Pick a random drift scenario
        const scenarioIndex = Math.floor(Math.random() * driftScenarios.length);
        const scenario = driftScenarios[scenarioIndex];
        this.log(`🟣 Selected scenario: ${scenario.type} - ${scenario.column}`, 'info');
        
        const modifiedSchema = [...originalSchema];
        
        if (scenario.type === 'ADD_COLUMN') {
            modifiedSchema.push(scenario.column);
            this.log(`➕ Added column: ${scenario.column}`, 'info');
        } else if (scenario.type === 'REMOVE_COLUMN') {
            const index = modifiedSchema.indexOf(scenario.column);
            if (index > -1) {
                modifiedSchema.splice(index, 1);
                this.log(`➖ Removed column: ${scenario.column}`, 'info');
            } else {
                this.log(`⚠️ Could not remove column ${scenario.column} - not found in schema`, 'warning');
            }
        }
        
        return modifiedSchema;
    }

    getHealthcareDriftScenarios(fileType) {
        const scenarios = {
            'claims': [
                { type: 'ADD_COLUMN', column: 'prior_auth_code', reason: 'New prior authorization tracking requirement' },
                { type: 'ADD_COLUMN', column: 'telehealth_indicator', reason: 'Post-COVID telehealth compliance' },
                { type: 'ADD_COLUMN', column: 'risk_adjustment_code', reason: 'CMS risk adjustment updates' },
                { type: 'REMOVE_COLUMN', column: 'legacy_provider_id', reason: 'Legacy system decommission' }
            ],
            'providers': [
                { type: 'ADD_COLUMN', column: 'dea_number', reason: 'DEA registration tracking for controlled substances' },
                { type: 'ADD_COLUMN', column: 'medicare_provider_id', reason: 'Medicare provider enrollment updates' },
                { type: 'ADD_COLUMN', column: 'quality_score', reason: 'Value-based care quality metrics' }
            ],
            'patients': [
                { type: 'ADD_COLUMN', column: 'emergency_contact_phone', reason: 'Enhanced emergency contact requirements' },
                { type: 'ADD_COLUMN', column: 'preferred_language', reason: 'Cultural competency compliance' },
                { type: 'REMOVE_COLUMN', column: 'ssn_last_four', reason: 'Enhanced privacy protection measures' }
            ]
        };
        
        return scenarios[fileType] || scenarios['claims'];
    }

    showSchemaDriftSection() {
        const schemaDriftSection = document.getElementById('schemaDriftSection');
        if (schemaDriftSection) {
            schemaDriftSection.style.display = 'block';
        }
        
        // Update affected systems count
        const affectedSystemsCount = document.getElementById('affectedSystemsCount');
        if (affectedSystemsCount) {
            affectedSystemsCount.textContent = this.detectedSchemaDrift.length;
        }
        
        // Populate drift actions
        this.populateSchemaDriftActions();
    }

    hideSchemaDriftSection() {
        const schemaDriftSection = document.getElementById('schemaDriftSection');
        if (schemaDriftSection) {
            schemaDriftSection.style.display = 'none';
        }
    }

    populateSchemaDriftActions() {
        const driftActionsContainer = document.getElementById('schemaDriftActions');
        if (!driftActionsContainer) return;
        
        driftActionsContainer.innerHTML = '';
        
        this.detectedSchemaDrift.forEach((drift, index) => {
            drift.changes.forEach((change, changeIndex) => {
                const actionCard = document.createElement('div');
                actionCard.className = 'card bg-base-200 shadow-sm mb-3';
                
                const severityColor = this.getSeverityColor(change.severity);
                const typeIcon = change.type === 'COLUMN_ADDED' ? 'fa-plus-circle' : 'fa-minus-circle';
                const typeColor = change.type === 'COLUMN_ADDED' ? 'text-success' : 'text-error';
                
                actionCard.innerHTML = `
                    <div class="card-body p-4">
                        <div class="flex items-start justify-between mb-2">
                            <div class="flex items-center gap-2">
                                <i class="fas ${typeIcon} ${typeColor}"></i>
                                <h4 class="font-semibold text-sm">${drift.fileName}</h4>
                                <div class="badge ${severityColor} badge-xs">${change.severity}</div>
                            </div>
                            <div class="text-xs text-base-content/60">${new Date(drift.detectedAt).toLocaleTimeString()}</div>
                        </div>
                        
                        <p class="text-sm mb-2">
                            <strong>${change.type === 'COLUMN_ADDED' ? 'New column:' : 'Removed column:'}</strong> 
                            <code class="bg-base-300 px-1 rounded">${change.column}</code>
                        </p>
                        
                        <p class="text-xs text-base-content/70 mb-3">${change.businessJustification}</p>
                        
                        <div class="flex gap-2 flex-wrap">
                            <button class="btn btn-success btn-xs" onclick="window.lineageSimulation.approveDriftChange(${index}, ${changeIndex})">
                                <i class="fas fa-check mr-1"></i>Approve
                            </button>
                            <button class="btn btn-warning btn-xs" onclick="window.lineageSimulation.flagForReview(${index}, ${changeIndex})">
                                <i class="fas fa-flag mr-1"></i>Flag for Review
                            </button>
                            <button class="btn btn-error btn-xs" onclick="window.lineageSimulation.rejectDriftChange(${index}, ${changeIndex})">
                                <i class="fas fa-times mr-1"></i>Reject
                            </button>
                            <button class="btn btn-ghost btn-xs" onclick="window.lineageSimulation.showDriftDetails(${index}, ${changeIndex})">
                                <i class="fas fa-info-circle mr-1"></i>Details
                            </button>
                        </div>
                    </div>
                `;
                
                driftActionsContainer.appendChild(actionCard);
            });
        });
    }

    getSeverityColor(severity) {
        const colors = {
            'CRITICAL': 'badge-error',
            'HIGH': 'badge-warning',
            'MEDIUM': 'badge-info',
            'LOW': 'badge-success'
        };
        return colors[severity] || 'badge-outline';
    }

    approveDriftChange(driftIndex, changeIndex) {
        const drift = this.detectedSchemaDrift[driftIndex];
        const change = drift.changes[changeIndex];
        
        this.log(`🟢 Approved schema change: ${change.column} in ${drift.fileName}`, 'success');
        this.showToast(`Schema change approved: ${change.column}`, 'success');

        // Drift changes
        drift.changes[changeIndex].status = 'APPROVED';
        this.populateSchemaDriftActions();
    }

    flagForReview(driftIndex, changeIndex) {
        const drift = this.detectedSchemaDrift[driftIndex];
        const change = drift.changes[changeIndex];
        
        this.log(`🔺 Flagged for review: ${change.column} in ${drift.fileName}`, 'warning');
        this.showToast(`Schema change flagged for review: ${change.column}`, 'warning');
        
        drift.changes[changeIndex].status = 'UNDER_REVIEW';
    }

    rejectDriftChange(driftIndex, changeIndex) {
        const drift = this.detectedSchemaDrift[driftIndex];
        const change = drift.changes[changeIndex];
        
        this.log(`⛔️ Rejected schema change: ${change.column} in ${drift.fileName}`, 'error');
        this.showToast(`Schema change rejected: ${change.column}`, 'error');
        
        drift.changes[changeIndex].status = 'REJECTED';
        this.populateSchemaDriftActions();
    }

    showDriftDetails(driftIndex, changeIndex) {
        const drift = this.detectedSchemaDrift[driftIndex];
        const change = drift.changes[changeIndex];
        
        this.log(`🟧 Schema Drift Details for ${change.column}:`, 'info');
        this.log(`  └─ File: ${drift.fileName}`, 'info');
        this.log(`  └─ Type: ${change.type}`, 'info');
        this.log(`  └─ Severity: ${change.severity}`, 'info');
        this.log(`  └─ Recommendation: ${change.recommendation}`, 'info');
        
        change.impact.forEach(impact => {
            this.log(`  └─ Impact: ${impact}`, 'info');
        });
        
        this.showToast(`Schema drift details logged for ${change.column}`, 'info');

        // Show stakeholder notification status for specific change
        this.showNotificationStatus(driftIndex, changeIndex);
    }

    showNotificationStatus(driftIndex, changeIndex) {
        const drift = this.detectedSchemaDrift[driftIndex];
        const change = drift.changes[changeIndex];
        
        this.log(`🔵 Notification Status for ${change.column}:`, 'info');
        
        // Filter notifications for this specific drift
        const relatedNotifications = Array.from(this.notificationStatus.values()).filter(
            notification => notification.drift === drift.fileName
        );
        
        if (relatedNotifications.length > 0) {
            this.log(`  └─ Total notifications sent: ${relatedNotifications.length}`, 'success');
            
            // Group by system
            const notificationsBySystem = {};
            relatedNotifications.forEach(notification => {
                if (!notificationsBySystem[notification.system]) {
                    notificationsBySystem[notification.system] = [];
                }
                notificationsBySystem[notification.system].push(notification);
            });
            
            Object.entries(notificationsBySystem).forEach(([system, notifications]) => {
                this.log(`  └─ ${system}: ${notifications.length} notifications`, 'info');
                notifications.forEach(notification => {
                    const statusIcon = notification.acknowledged ? '🟢' : notification.status === 'SENT' ? '🔵' : '⛔️';
                    this.log(`    └─ ${statusIcon} ${notification.channel}: ${notification.recipient}`, 'info');
                });
            });
        } else {
            this.log(`  └─ No notifications sent yet`, 'warning');
        }
    }

    // Activity feed with stakeholder tracking
    logStakeholderActivity(action, system, recipient, drift) {
        const timestamp = new Date().toLocaleTimeString();
        const activityMessage = `[${timestamp}] 👥 ${action}: ${system} → ${recipient} (${drift.fileName})`;
        
        this.log(activityMessage, 'info');
        
        // Stakeholder activity log
        if (!this.stakeholderActivityLog) {
            this.stakeholderActivityLog = [];
        }
        
        this.stakeholderActivityLog.push({
            timestamp: new Date().toISOString(),
            action,
            system,
            recipient,
            drift: drift.fileName,
            severity: drift.severity
        });
    }

    updateNotificationStatusUI() {
        const notificationStatusElement = document.getElementById('stakeholderNotificationStatus');
        const notificationCountElement = document.getElementById('notificationCount');
        const emailCountElement = document.getElementById('emailNotificationCount');
        const teamsCountElement = document.getElementById('teamsNotificationCount');
        const affectedSystemsElement = document.getElementById('affectedSystemsNotified');
        
        if (!notificationStatusElement) return;
        
        const allNotifications = Array.from(this.notificationStatus.values());
        const totalNotifications = allNotifications.length;
        
        if (totalNotifications > 0) {
            notificationStatusElement.style.display = 'block';
            
            // Count notifications by channel
            const emailNotifications = allNotifications.filter(n => n.channel === 'email').length;
            const teamsNotifications = allNotifications.filter(n => n.channel === 'teams').length;
            
            // Count unique affected systems
            const uniqueSystems = new Set(allNotifications.map(n => n.system)).size;

            // Notification elements
            if (notificationCountElement) {
                notificationCountElement.textContent = `${totalNotifications} sent`;
            }
            if (emailCountElement) {
                emailCountElement.textContent = emailNotifications;
            }
            if (teamsCountElement) {
                teamsCountElement.textContent = teamsNotifications;
            }
            if (affectedSystemsElement) {
                affectedSystemsElement.textContent = uniqueSystems;
            }
        } else {
            notificationStatusElement.style.display = 'none';
        }
    }

    // Audit trail and compliance reporting
    generateAuditTrail() {
        const auditReport = {
            timestamp: new Date().toISOString(),
            sessionId: `session-${Date.now()}`,
            summary: {
                totalNotifications: this.notificationQueue.length,
                stakeholdersNotified: new Set(this.notificationQueue.map(n => n.recipient)).size,
                systemsAffected: new Set(this.notificationQueue.map(n => n.system)).size,
                driftEventsDetected: this.detectedSchemaDrift.length
            },
            notifications: this.notificationQueue.map(notification => ({
                ...notification,
                deliveryStatus: this.notificationStatus.get(notification.id)?.status || 'UNKNOWN'
            })),
            stakeholderActivity: this.stakeholderActivityLog || [],
            driftEvents: this.detectedSchemaDrift.map(drift => ({
                fileName: drift.fileName,
                severity: drift.severity,
                changesCount: drift.changes.length,
                detectedAt: drift.detectedAt,
                changes: drift.changes.map(change => ({
                    type: change.type,
                    column: change.column,
                    severity: change.severity,
                    businessJustification: change.businessJustification
                }))
            })),
            complianceInfo: {
                healthcareCompliance: 'HIPAA',
                dataGovernanceFramework: 'Enterprise Data Management',
                auditRetentionPeriod: '7 years',
                regulatoryRequirements: ['HIPAA', 'CMS', 'FDA 21 CFR Part 11']
            }
        };
        
        return auditReport;
    }

    getNotificationCount(fileName) {
        // Count notifications sent for a specific file
        return Array.from(this.notificationStatus.values()).filter(
            notification => notification.drift === fileName
        ).length;
    }

    exportAuditTrail() {
        const auditReport = this.generateAuditTrail();
        
        const blob = new Blob([JSON.stringify(auditReport, null, 2)], {
            type: 'application/json'
        });
        
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `schema-drift-audit-${new Date().toISOString().split('T')[0]}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        
        this.log('📋 Audit trail exported successfully', 'success');
        this.showToast('Audit trail downloaded for compliance records', 'success');
    }

    // Test: Force schema drift
    async forceSchemaDrift() {
        if (this.discoveredFiles.length === 0) {
            this.showToast('No files discovered yet - run a directory scan first', 'warning');
            return;
        }

        this.log('⚡ FORCING SCHEMA DRIFT for testing...', 'warning');
        this.detectedSchemaDrift = []; // Clear any existing drift
        
        // Force drift on the first file
        const firstFile = this.discoveredFiles[0];
        this.log(`🔶 Targeting file: ${firstFile.fileName}`, 'info');
        
        const currentSchema = this.getSchemaForFileType(firstFile.fileName);
        this.log(`🔘 Original schema: [${currentSchema.join(', ')}]`, 'info');
        
        const modifiedSchema = this.simulateSchemaDrift(currentSchema, firstFile.fileName);
        this.log(`🟠 Modified schema: [${modifiedSchema.join(', ')}]`, 'info');
        
        const driftResult = this.detectSchemaDrift(firstFile.fileName, modifiedSchema);
        
        if (driftResult.hasDrift) {
            this.detectedSchemaDrift.push(driftResult);
            this.log(`🟢 Forced drift successful: ${driftResult.changes.length} changes detected`, 'success');
            
            // Show Drift section and notify stakeholders
            this.showSchemaDriftSection();
            this.showToast(`Schema drift forced: ${driftResult.changes.length} changes detected`, 'warning');
            
            // Notify stakeholders
            this.log('🔵 Initiating stakeholder notification process...', 'info');
            await this.notifyAllAffectedStakeholders();
        } else {
            this.log('⛔️ Force drift failed - no changes detected', 'error');
            this.showToast('Force drift failed - unable to generate schema changes', 'error');
        }
    }

    getTransformationColor(transformation) {
        const colors = {
            'RENAME_VALIDATE': 'badge-info',
            'ANONYMIZATION': 'badge-warning',
            'LOOKUP_ENRICHMENT': 'badge-success',
            'DEDUPLICATION': 'badge-primary',
            'EXTERNAL_VALIDATION': 'badge-error',
            'STANDARDIZATION': 'badge-accent',
            'HASH_GENERATION': 'badge-secondary',
            'FIELD_ENCRYPTION': 'badge-warning',
            'ADDRESS_CLEANSING': 'badge-info'
        };
        return colors[transformation] || 'badge-outline';
    }

    getImpactColor(impact) {
        const colors = {
            'LOW': 'badge-success',
            'MEDIUM': 'badge-warning',
            'HIGH': 'badge-error',
            'CRITICAL': 'badge-error'
        };
        return colors[impact] || 'badge-outline';
    }

    getFileType(fileName) {
        if (fileName.includes('claims')) return 'claims';
        if (fileName.includes('providers')) return 'providers';
        if (fileName.includes('patients')) return 'patients';
        if (fileName.includes('procedures')) return 'procedures';
        if (fileName.includes('medications')) return 'medications';
        if (fileName.includes('lab_results')) return 'lab_results';
        return 'claims'; // default
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
        // API Status Text
        document.getElementById('apiStatus').textContent = status;
        document.getElementById('apiStatusDesc').textContent = description;

        // API Status Badge
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
            info: '🟣',
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

        // Clear activity log for new automation cycle
        document.getElementById('activityLog').innerHTML = '';
        
        // Update UI
        document.getElementById('startScan').disabled = true;
        document.getElementById('scannerReady').classList.add('hidden');
        document.getElementById('scannerLoading').classList.remove('hidden');

        this.log('🔷 Starting new automation cycle...');
        this.log('Starting directory scan on ADLS Gen2 container: claims-data');
        this.log('Connecting to Azure Storage Account...');

        await this.delay(1500);

        this.log('Connection established, scanning for new files...');
        
        await this.delay(2000);

        // Pseudo-dynamic file discovery
        this.discoveredFiles = this.generateRandomFiles();
        
        this.log(`Scan complete! Found ${this.discoveredFiles.length} new files for processing`, 'success');

        // Schema drift detection as part of file discovery
        this.log('🔘 Analyzing discovered files for schema drift...', 'info');
        this.detectedSchemaDrift = [];
        await this.analyzeFileSchemas();
        
        if (this.detectedSchemaDrift.length > 0) {
            this.log(`🟢 Schema drift analysis complete: ${this.detectedSchemaDrift.length} drift events detected during discovery`, 'warning');
            this.showSchemaDriftSection();
            this.showToast(`Schema drift detected in ${this.detectedSchemaDrift.length} files during discovery`, 'warning');
            
            // Notify stakeholders about drift found during discovery
            this.log('🔵 Initiating stakeholder notification process...', 'info');
            await this.notifyAllAffectedStakeholders();
        } else {
            this.log('🟢 Schema analysis complete: No drift detected in discovered files', 'success');
        }
        
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

        // Update column lineage mapping if enabled
        const isColumnLineageEnabled = document.getElementById('columnLineageToggle').checked;
        if (isColumnLineageEnabled) {
            this.populateColumnLineageTable();
            this.log('🟧 Column mappings updated with discovered files', 'info');
        }

        // Update JSON Generator status to show it's ready
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

        // Purview Registration status to show it's ready
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
            
            // Check if this file has schema drift detected
            const fileDrift = this.detectedSchemaDrift.find(drift => drift.fileName === file.fileName);
            
            // Create entities based on complexity level
            for (let i = 0; i < fileComplexity; i++) {
                const entitySuffix = i === 0 ? '' : `_${i}`;
                
                if (i === 0) {
                    // Source Dataset (always first entity) with schema drift metadata
                    const tableEntity = {
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
                    };
                    
                    // Add schema drift metadata if detected
                    if (fileDrift) {
                        tableEntity.attributes.schemaDriftDetected = true;
                        tableEntity.attributes.schemaDriftSeverity = fileDrift.severity || 'UNKNOWN';
                        tableEntity.attributes.schemaDriftChanges = fileDrift.changes.length;
                        tableEntity.attributes.schemaDriftDetectedAt = fileDrift.detectedAt;
                        tableEntity.attributes.baselineSchemaVersion = fileDrift.baselineVersion;
                        tableEntity.attributes.driftChangeTypes = fileDrift.changes.map(c => c.type);
                        tableEntity.attributes.affectedColumns = fileDrift.changes.map(c => c.column);
                        tableEntity.attributes.stakeholdersNotified = this.getNotificationCount(file.fileName);
                    } else {
                        tableEntity.attributes.schemaDriftDetected = false;
                        tableEntity.attributes.schemaDriftSeverity = null;
                        tableEntity.attributes.stakeholdersNotified = 0;
                    }
                    
                    entities.push(tableEntity);
                } else if (i === 1) {
                    // Process (second entity) with schema drift impact
                    const processEntity = {
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
                    };
                    
                    // Add schema drift impact to process
                    if (fileDrift) {
                        processEntity.attributes.affectedBySchemaDrift = true;
                        processEntity.attributes.driftImpactLevel = fileDrift.severity;
                        processEntity.attributes.pipelineUpdateRequired = fileDrift.changes.length > 0;
                        processEntity.attributes.lastSchemaValidation = fileDrift.detectedAt;
                    } else {
                        processEntity.attributes.affectedBySchemaDrift = false;
                        processEntity.attributes.pipelineUpdateRequired = false;
                    }
                    
                    entities.push(processEntity);
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
        
        // Auto Mode Toggle
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
        this.detectedSchemaDrift = [];
        
        // Reset notification system
        this.notificationQueue = [];
        this.notificationStatus = new Map();
        this.stakeholderActivityLog = [];

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

        // Hide schema drift section
        this.hideSchemaDriftSection();
        
        // Reset notification status UI
        const notificationStatusElement = document.getElementById('stakeholderNotificationStatus');
        if (notificationStatusElement) {
            notificationStatusElement.style.display = 'none';
        }
        // Reset notification counters
        ['notificationCount', 'emailNotificationCount', 'teamsNotificationCount', 'affectedSystemsNotified'].forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.textContent = id === 'notificationCount' ? '0 sent' : '0';
            }
        });

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

// Global functions for navbar menu
function shareDemo() {
    const subject = encodeURIComponent('JSON-Driven Lineage Automation Framework Demo');
    const body = encodeURIComponent(`Hi,

I wanted to share this interactive demonstration of an automated data lineage framework for Microsoft Purview:

<i class="fas fa-play-circle"></i> Demo: ${window.location.href}
<i class="fab fa-github"></i> GitHub: https://github.com/qoryhanisagal/lineageautomation

Key Features:
• Automated schema drift detection
• Dynamic JSON payload generation for Purview REST API
• Column-level lineage tracking
• Real-time pipeline monitoring simulation

This framework addresses enterprise challenges around mandatory data cataloging and schema evolution in Microsoft Fabric environments.

Best regards`);
    
    const mailtoLink = `mailto:?subject=${subject}&body=${body}`;
    window.open(mailtoLink, '_blank');
}

function exportDemoReport() {
    const timestamp = new Date().toISOString();
    const autoMode = document.getElementById('autoModeToggle')?.checked ? 'Auto Mode' : 'Manual Mode';
    const columnLineage = document.getElementById('columnLineageToggle')?.checked ? 'Enabled' : 'Disabled';
    const complexity = document.getElementById('complexityLevel')?.value || 'auto';
    const jsonContent = document.getElementById('jsonContent')?.textContent || 'Not generated yet - run the demo to see sample payload';
    
    const report = `# Lineage Automation Framework - Demo Report
Generated: ${timestamp}

## Demo Session Summary
- Settings Used: ${autoMode}
- Column Lineage: ${columnLineage}
- Complexity Level: ${complexity}

## Azure Prerequisites Required
### Service Principal Setup
\`\`\`bash
# Create service principal for automation
az ad sp create-for-rbac --name "lineage-automation-sp" \\
  --role "Purview Data Curator"

# Assign ADLS Gen2 permissions  
az role assignment create \\
  --assignee <service-principal-id> \\
  --role "Storage Blob Data Reader" \\
  --scope /subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<storage-account>
\`\`\`

### Required Azure Resources
- Microsoft Purview account with REST API access
- Azure Data Lake Storage Gen2 account
- Service Principal with appropriate permissions
- Azure Key Vault for credential management (recommended)

## Configuration Template
\`\`\`javascript
const CONFIG = {
    purviewAccount: 'your-purview-account',
    tenantId: 'your-tenant-id', 
    clientId: 'your-client-id',
    clientSecret: 'your-client-secret',
    adlsContainer: 'your-container-name',
    sqlServer: 'your-sql-server.database.windows.net',
    sqlDatabase: 'your-database'
};
\`\`\`

## Generated JSON Payload Sample
${jsonContent}

## Implementation Notes
- Framework handles A→B→C→D→E schema evolution automatically
- Mandatory cataloging ensures no data is excluded from lineage tracking
- Column-level lineage provides detailed transformation mapping
- Integrates with Fabric medallion architecture (Bronze/Silver/Gold)

## Next Steps
1. Configure Azure prerequisites above
2. Deploy framework code from GitHub repository
3. Set up Event Grid triggers for real-time file detection
4. Configure pipeline mappings for your data sources

## Support
GitHub Repository: https://github.com/qoryhanisagal/lineageautomation
Documentation: See repository README and /demo/docs/ folder

---
Generated by JSON-Driven Lineage Automation Framework Demo`;

    // Create and download the report
    const blob = new Blob([report], { type: 'text/markdown' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `lineage-automation-demo-report-${new Date().toISOString().split('T')[0]}.md`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    
    // Show success notification
    if (window.lineageSimulation && window.lineageSimulation.showToast) {
        window.lineageSimulation.showToast('Demo report exported successfully!', 'success');
    }
}

// Initialize simulation when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.lineageSimulation = new LineageSimulation();
});