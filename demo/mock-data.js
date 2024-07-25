// ===================================================================
// Mock Data for Lineage Automation Framework Demo
// Realistic sample data for Kevin's demonstration
// ===================================================================

window.mockData = {
    sampleFiles: [
        {
            fileName: 'claims_2024_07_25.csv',
            filePath: 'adls://claimsdata/incoming/claims_2024_07_25.csv',
            fileSize: 2847392,
            lastModified: '2024-07-25T08:15:30Z',
            schema: [
                'claim_id',
                'patient_id', 
                'provider_id',
                'service_date',
                'diagnosis_code',
                'procedure_code',
                'claim_amount',
                'insurance_type',
                'status'
            ]
        },
        {
            fileName: 'providers_2024_07_25.csv', 
            filePath: 'adls://claimsdata/incoming/providers_2024_07_25.csv',
            fileSize: 156782,
            lastModified: '2024-07-25T08:22:15Z',
            schema: [
                'provider_id',
                'provider_name',
                'npi_number',
                'specialty',
                'address',
                'city',
                'state',
                'zip_code',
                'phone'
            ]
        },
        {
            fileName: 'patients_2024_07_25.csv',
            filePath: 'adls://claimsdata/incoming/patients_2024_07_25.csv', 
            fileSize: 1923847,
            lastModified: '2024-07-25T08:18:45Z',
            schema: [
                'patient_id',
                'first_name',
                'last_name', 
                'date_of_birth',
                'gender',
                'address',
                'city',
                'state',
                'zip_code',
                'insurance_id'
            ]
        }
    ],

    pipelineConfigurations: {
        'claims_': {
            pipelineName: 'transform_claims_pipeline',
            destinationTable: 'processed_claims',
            transformationType: 'standardization_and_validation',
            description: 'Validates claim data, standardizes formats, and applies business rules',
            estimatedRuntime: '12-15 minutes',
            dataFlowSteps: [
                'Data validation and cleansing',
                'Diagnosis code standardization', 
                'Amount calculations and verification',
                'Duplicate detection and removal',
                'Final quality checks'
            ]
        },
        'providers_': {
            pipelineName: 'transform_providers_pipeline',
            destinationTable: 'processed_providers',
            transformationType: 'deduplication_and_enrichment',
            description: 'Deduplicates provider records and enriches with external data',
            estimatedRuntime: '5-8 minutes',
            dataFlowSteps: [
                'NPI validation against external registry',
                'Duplicate provider detection',
                'Address standardization',
                'Specialty code mapping',
                'Contact information validation'
            ]
        },
        'patients_': {
            pipelineName: 'transform_patients_pipeline',
            destinationTable: 'processed_patients',
            transformationType: 'privacy_and_anonymization',
            description: 'Applies privacy controls and data anonymization where required',
            estimatedRuntime: '8-10 minutes',
            dataFlowSteps: [
                'PII identification and classification',
                'Address standardization',
                'Data anonymization for research datasets',
                'Privacy compliance checks',
                'Insurance verification'
            ]
        }
    },

    purviewApiEndpoints: {
        authentication: 'https://login.microsoftonline.com/{tenant-id}/oauth2/v2.0/token',
        entityBulkCreate: 'https://{purview-account}.purview.azure.com/datamap/api/atlas/v2/entity/bulk',
        lineageQuery: 'https://{purview-account}.purview.azure.com/datamap/api/atlas/v2/lineage/{guid}',
        searchAssets: 'https://{purview-account}.purview.azure.com/datamap/api/search/query'
    },

    sampleJsonPayload: {
        entities: [
            {
                typeName: "DataSet",
                attributes: {
                    qualifiedName: "adls://claimsdata/incoming/claims_2024_07_25.csv",
                    name: "claims_2024_07_25.csv",
                    description: "Source file for standardization_and_validation",
                    owner: "External_Data_Provider",
                    createTime: "2024-07-25T08:15:30Z",
                    fileSize: 2847392,
                    format: "CSV"
                },
                guid: "12345678-1234-4567-8901-123456789012",
                status: "ACTIVE"
            }
        ],
        referredEntities: {},
        guidAssignments: {}
    },

    expectedApiResponse: {
        guidAssignments: {
            "12345678-1234-4567-8901-123456789012": "12345678-1234-4567-8901-123456789012"
        },
        mutatedEntities: {
            "CREATE": [
                {
                    guid: "12345678-1234-4567-8901-123456789012",
                    typeName: "DataSet"
                }
            ]
        },
        partialUpdatedEntities: [],
        status: "SUCCESS"
    }
};