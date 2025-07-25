// ===================================================================
// Unit Tests for JSON-Driven Lineage Automation Framework
// ===================================================================

import { describe, test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import LineageAutomationFramework from '../lineage-automation.js';

describe('LineageAutomationFramework', () => {
  let framework;
  const mockConfig = {
    purviewAccount: 'test-purview',
    tenantId: 'test-tenant',
    clientId: 'test-client',
    clientSecret: 'test-secret',
    adlsContainer: 'test-container',
    sqlServer: 'test-server.database.windows.net',
    sqlDatabase: 'test-database'
  };

  beforeEach(() => {
    framework = new LineageAutomationFramework(mockConfig);
  });

  describe('mapToPipeline', () => {
    test('should map claims files correctly', () => {
      const result = framework.mapToPipeline('claims_2024_07_25.csv');

      assert.strictEqual(result.pipelineName, 'transform_claims_pipeline');
      assert.strictEqual(result.destinationTable, 'processed_claims');
      assert.strictEqual(result.transformationType, 'standardization_and_validation');
    });

    test('should map providers files correctly', () => {
      const result = framework.mapToPipeline('providers_2024_07_25.csv');
      
      // Add assertions for providers mapping
      assert(result !== null);
      // Add specific assertions based on your expected provider pipeline mapping
    });

    test('should return null for unknown file patterns', () => {
      const result = framework.mapToPipeline('unknown_file.csv');
      assert.strictEqual(result, null);
    });
  });

  describe('generateLineageJSON', () => {
    test('should generate valid JSON payload', () => {
      const fileInfo = {
        fileName: 'claims_2024_07_25.csv',
        filePath: 'adls://test-container/claims_2024_07_25.csv',
        fileSize: 12345,
        schema: ['claim_id', 'patient_id', 'amount']
      };

      const pipelineConfig = {
        pipelineName: 'transform_claims_pipeline',
        destinationTable: 'processed_claims',
        transformationType: 'standardization_and_validation'
      };

      const result = framework.generateLineageJSON(fileInfo, pipelineConfig);

      assert.strictEqual(result.entities.length, 3);
      assert.strictEqual(result.entities[0].typeName, 'DataSet');
      assert.strictEqual(result.entities[1].typeName, 'Process');
      assert.strictEqual(result.entities[2].typeName, 'Table');
    });
  });

  describe('generateGuid', () => {
    test('should generate valid GUID format', () => {
      const guid = framework.generateGuid();
      const guidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

      assert.match(guid, guidRegex);
    });

    test('should generate unique GUIDs', () => {
      const guid1 = framework.generateGuid();
      const guid2 = framework.generateGuid();
      
      assert.notStrictEqual(guid1, guid2);
    });
  });
});