// ===================================================================
// JSON-Driven Lineage Automation Framework for Microsoft Purview
// Author: Koiree (Sequoyah Dozier)
// Purpose: Auto-generate and register lineage relationships in Purview
// ===================================================================

import axios from 'axios';
import path from 'path';

// Configuration - Replace with actual values
const CONFIG = {
    purviewAccount: 'your-purview-account',
    tenantId: 'your-tenant-id',
    clientId: 'your-client-id',
    clientSecret: 'your-client-secret',
    adlsContainer: 'claims-data',
    sqlServer: 'your-sql-server.database.windows.net',
    sqlDatabase: 'your-database'
};

/**
 * Core class for managing lineage automation
 */
class LineageAutomationFramework {
    constructor(config) {
        this.config = config;
        this.accessToken = null;
    }

    /**
     * Authenticate with Microsoft Purview using service principal
     */
    async authenticate() {
        try {
            const tokenUrl = `https://login.microsoftonline.com/${this.config.tenantId}/oauth2/v2.0/token`;
            
            const tokenRequest = {
                grant_type: 'client_credentials',
                client_id: this.config.clientId,
                client_secret: this.config.clientSecret,
                scope: 'https://purview.azure.net/.default'
            };

            const response = await axios.post(tokenUrl, new URLSearchParams(tokenRequest));
            this.accessToken = response.data.access_token;
            
            console.log('Successfully authenticated with Microsoft Purview');
            return true;
            
        } catch (error) {
            console.error('Authentication failed:', error.message);
            return false;
        }
    }

    /**
     * Scan directory for new files (simulated for demo)
     * In production, this would connect to ADLS Gen2 SDK
     */
    scanDirectory(containerPath) {
        console.log(`Scanning directory: ${containerPath}`);
        
        // Simulated file discovery - replace with actual ADLS Gen2 SDK calls
        const discoveredFiles = [
            {
                fileName: 'claims_2024_07_25.csv',
                filePath: `adls://${this.config.adlsContainer}/incoming/claims_2024_07_25.csv`,
                fileSize: 45632,
                lastModified: new Date().toISOString(),
                schema: ['claim_id', 'patient_id', 'provider_id', 'amount', 'date_service']
            }
        ];
        
        console.log(`Found ${discoveredFiles.length} new files for processing`);
        return discoveredFiles;
    }

    /**
     * Map file to transformation pipeline based on naming conventions
     */
    mapToPipeline(fileName) {
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
            }
        };
        
        // Simple mapping logic - can be enhanced with regex or lookup tables
        for (const [prefix, config] of Object.entries(pipelineMapping)) {
            if (fileName.startsWith(prefix)) {
                return config;
            }
        }
        
        return null;
    }

    /**
     * Generate JSON payload for Purview lineage registration
     * This is the core innovation - dynamic JSON generation
     */
    generateLineageJSON(fileInfo, pipelineConfig) {
        const timestamp = new Date().toISOString();
        
        const lineagePayload = {
            entities: [
                {
                    typeName: "DataSet",
                    attributes: {
                        qualifiedName: fileInfo.filePath,
                        name: fileInfo.fileName,
                        description: `Source file for ${pipelineConfig.transformationType}`,
                        owner: "External_Data_Provider",
                        createTime: timestamp,
                        fileSize: fileInfo.fileSize,
                        format: "CSV",
                        schema: fileInfo.schema
                    },
                    guid: this.generateGuid(),
                    status: "ACTIVE"
                },
                {
                    typeName: "Process",
                    attributes: {
                        qualifiedName: `adf://pipelines/${pipelineConfig.pipelineName}`,
                        name: pipelineConfig.pipelineName,
                        description: `Automated ${pipelineConfig.transformationType} process`,
                        processType: "ETL_PIPELINE",
                        inputs: [fileInfo.filePath],
                        outputs: [`sql://${this.config.sqlServer}/${this.config.sqlDatabase}/dbo/${pipelineConfig.destinationTable}`],
                        createTime: timestamp
                    },
                    guid: this.generateGuid(),
                    status: "ACTIVE"
                },
                {
                    typeName: "Table",
                    attributes: {
                        qualifiedName: `sql://${this.config.sqlServer}/${this.config.sqlDatabase}/dbo/${pipelineConfig.destinationTable}`,
                        name: pipelineConfig.destinationTable,
                        description: `Processed data from ${fileInfo.fileName}`,
                        schema: "dbo",
                        tableType: "MANAGED_TABLE",
                        createTime: timestamp
                    },
                    guid: this.generateGuid(),
                    status: "ACTIVE"
                }
            ],
            referredEntities: {},
            guidAssignments: {}
        };
        
        console.log('Generated lineage JSON payload');
        console.log(JSON.stringify(lineagePayload, null, 2));
        
        return lineagePayload;
    }

    /**
     * Register lineage in Microsoft Purview via REST API
     */
    async registerLineage(lineagePayload) {
        if (!this.accessToken) {
            console.error('Not authenticated. Call authenticate() first.');
            return false;
        }
        
        try {
            const purviewUrl = `https://${this.config.purviewAccount}.purview.azure.com/datamap/api/atlas/v2/entity/bulk`;
            
            const headers = {
                'Authorization': `Bearer ${this.accessToken}`,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            };

            console.log('Posting lineage to Purview...');
            const response = await axios.post(purviewUrl, lineagePayload, { headers });
            
            if (response.status === 200 || response.status === 201) {
                console.log('Lineage successfully registered in Purview');
                console.log('Entity GUIDs:', response.data.guidAssignments);
                return true;
            }
            
        } catch (error) {
            console.error('Failed to register lineage:', error.response?.data || error.message);
            return false;
        }
    }

    /**
     * Main orchestration method - the complete automation flow
     */
    async processNewFiles() {
        console.log('Starting automated lineage registration process...\n');
        
        // Step 1: Authenticate
        const authenticated = await this.authenticate();
        if (!authenticated) return;
        
        // Step 2: Scan for new files
        const newFiles = this.scanDirectory(this.config.adlsContainer);
        
        // Step 3: Process each file
        for (const fileInfo of newFiles) {
            console.log(`\nProcessing file: ${fileInfo.fileName}`);
            
            // Map to pipeline
            const pipelineConfig = this.mapToPipeline(fileInfo.fileName);
            if (!pipelineConfig) {
                console.log('No pipeline mapping found, skipping...');
                continue;
            }

            console.log(`Mapped to pipeline: ${pipelineConfig.pipelineName}`);

            // Generate JSON
            const lineagePayload = this.generateLineageJSON(fileInfo, pipelineConfig);

            // Register in Purview
            await this.registerLineage(lineagePayload);
            
            console.log('File processing complete\n');
        }
        
        console.log('Automated lineage registration process finished!');
    }

    /**
     * Utility method to generate GUIDs for entities
     */
    generateGuid() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }

    /**
     * Demo method to show sample JSON output without API calls
     */
    demonstrateJSONGeneration() {
        console.log('=== DEMO: JSON Generation for Kevin ===\n');
        
        const sampleFile = {
            fileName: 'claims_2024_07_25.csv',
            filePath: `adls://${this.config.adlsContainer}/incoming/claims_2024_07_25.csv`,
            fileSize: 45632,
            lastModified: new Date().toISOString(),
            schema: ['claim_id', 'patient_id', 'provider_id', 'amount', 'date_service']
        };
        
        const samplePipeline = {
            pipelineName: 'transform_claims_pipeline',
            destinationTable: 'processed_claims',
            transformationType: 'standardization_and_validation'
        };
        
        const demoJSON = this.generateLineageJSON(sampleFile, samplePipeline);
        
        console.log('This JSON would be posted to Purview REST API:');
        console.log(`POST https://${this.config.purviewAccount}.purview.azure.com/datamap/api/atlas/v2/entity/bulk`);
        console.log('\nSample Response (201 Created):');
        console.log({
            "guidAssignments": {
                [demoJSON.entities[0].guid]: demoJSON.entities[0].guid,
                [demoJSON.entities[1].guid]: demoJSON.entities[1].guid,
                [demoJSON.entities[2].guid]: demoJSON.entities[2].guid
            },
            "mutatedEntities": {
                "CREATE": demoJSON.entities.map(e => ({ guid: e.guid, typeName: e.typeName }))
            }
        });
    }
}

// ===================================================================
// DEMO EXECUTION - Perfect for Kevin's video walkthrough
// ===================================================================

async function main() {
    console.log('JSON-Driven Lineage Automation Framework');
    console.log('Follow-up demo for Kevin\n');

    const framework = new LineageAutomationFramework(CONFIG);

    // Demo the JSON generation capability
    framework.demonstrateJSONGeneration();

    console.log('\nKey Benefits for Scale:');
    console.log('  - Repeatable registration as schemas evolve');
    console.log('  - Governance coverage for schema drift');
    console.log('  - Automated impact analysis via Purview UI');
    console.log('  - Column-level traceability with business glossary');

    console.log('\nNext Steps:');
    console.log('  • Configure with your Purview account details');
    console.log('  • Connect to ADLS Gen2 SDK for real directory scanning');
    console.log('  • Set up Power Automate triggers for new file detection');
    console.log('  • Implement schema mapping rules for your data flows');
}

// Run the demo
if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    main().catch(console.error);
}

export default LineageAutomationFramework;