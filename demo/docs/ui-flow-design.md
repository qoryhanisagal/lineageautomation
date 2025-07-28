# UI Flow Design: Data Processing Workflow

> Design rationale for the sequential processing flow in the interactive demonstration interface

## Overview

The interactive demonstration follows a carefully designed sequential workflow that mirrors enterprise data governance best practices. This document explains the design decisions behind the UI flow order and processing logic.

## Sequential Processing Workflow

### 1. File Discovery Phase
**Purpose:** Identify and catalog incoming data files with their basic structural metadata

**Process:**
- Scan data lake directories for new files
- Extract file metadata (size, timestamp, format)
- Perform initial schema inspection
- Register file entities in the governance catalog

### 2. Column Lineage Mapping Phase  
**Purpose:** Establish transformation relationships between source and destination schemas

**Process:**
- Analyze source file schema structure
- Map source columns to destination table columns
- Document transformation rules (rename, type conversion, business logic)
- Create column-level lineage relationships

**Key Dependencies:**
- Requires completed file discovery metadata
- Establishes baseline for downstream drift detection
- Defines expected transformation patterns

### 3. Schema Drift Detection Phase
**Purpose:** Validate current data against established mapping expectations

**Process:**
- Compare incoming schema against baseline patterns
- Identify schema evolution (A→B→C→D→E progression)
- Assess impact on existing column mappings
- Generate drift analysis reports

## Design Rationale

### Why Sequential Processing?

**Technical Dependencies:**
- Schema drift detection requires established column mappings to determine what constitutes "drift"
- Column lineage mapping needs complete file metadata before establishing relationships
- Each phase builds upon the previous phase's outputs

**Business Process Alignment:**
- Mirrors enterprise data governance workflows where mapping rules are established before compliance validation
- Follows data quality assessment patterns used in healthcare data processing
- Enables clear impact analysis when schema changes are detected

**User Experience Benefits:**
- Progressive disclosure of complexity (simple → detailed → analytical)
- Clear cause-and-effect relationship between processing steps
- Logical troubleshooting flow when issues arise

### Healthcare Data Pipeline Context

In healthcare data environments, this sequential approach addresses specific challenges:

**Regulatory Compliance:**
- Column lineage mapping establishes audit trails required for healthcare data governance
- Schema drift detection ensures data quality standards are maintained
- Sequential processing provides clear documentation for compliance audits

**Risk Management:**
- Early identification of schema changes before they impact downstream systems
- Clear visibility into which specific column mappings are affected by drift
- Structured approach to validating data transformation accuracy

## Alternative Flow Considerations

### Parallel Processing Option
While parallel processing could be implemented for performance gains, the sequential approach was chosen because:

- **Clarity:** Easier to understand and troubleshoot processing steps
- **Debugging:** Issues can be isolated to specific processing phases
- **Documentation:** Natural workflow for generating compliance documentation
- **Training:** Matches how data engineers typically approach lineage problems

### Real-Time Detection Option
Schema drift could be detected immediately during file discovery, but delayed detection provides:

- **Context:** Drift analysis includes impact on established column mappings
- **Accuracy:** More comprehensive analysis with full transformation context
- **Actionability:** Specific remediation guidance based on mapping relationships

## Implementation Notes

The UI flow design directly supports the framework's core value proposition of **automated pipeline lineage tracking with intelligent schema drift handling**. By following enterprise data governance patterns, the demonstration effectively showcases how the framework would integrate into existing data operations workflows.

This sequential design also facilitates the framework's ability to handle the enterprise challenge of mandatory data cataloging regardless of schema changes, providing a clear path from detection through resolution.