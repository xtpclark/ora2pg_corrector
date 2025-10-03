import { state, editors, dom } from './state.js';
import { apiFetch } from './api.js';
import { showToast, toggleButtonLoading, renderClients, switchTab, renderSettingsForms, renderReportTable, renderFileBrowser, renderObjectTypeTree, renderObjectList, renderSessionHistory, populateTypeDropdown, renderActiveConfig } from './ui.js';

/**
 * Sends an audit log event to the backend for the current client.
 * @async
 * @param {number} clientId - The ID of the client.
 * @param {string} action - A short code for the action being logged (e.g., 'save_settings').
 * @param {string} details - A description of the event.
 */
async function log_audit(clientId, action, details) {
    if (!clientId) return;
    try {
        await apiFetch(`/api/client/${clientId}/log_audit`, {
            method: 'POST',
            body: JSON.stringify({ action, details })
        });
    } catch (error) {
        console.error("Failed to log audit event:", error);
    }
}

/**
 * Updates the workspace file status display.
 * @param {string|null} filename - The name of the loaded file, or null to clear.
 */
function updateWorkspaceStatus(filename = null) {
    const statusEl = document.getElementById('workspace-file-status');
    if (statusEl) {
        statusEl.textContent = filename ? `File: ${filename}` : '';
    }
}

/**
 * Clears both editors in the workspace.
 */
function handleClearWorkspace() {
    if (editors.original?.setValue) {
        editors.original.setValue('-- Paste or load your source SQL here...');
    }
    if (editors.corrected?.setValue) {
        editors.corrected.setValue('-- PostgreSQL-converted SQL will appear here...');
    }
    state.currentFileId = null;
    updateWorkspaceStatus();
    
    // Clear validation result
    const resultDiv = document.getElementById('validation-result');
    if (resultDiv) resultDiv.classList.add('hidden');
    
    showToast('Workspace cleared');
}

/**
 * Copies content from source editor to target editor.
 */
function handleCopyToTarget() {
    const sourceContent = editors.original?.getValue() || '';
    if (!sourceContent || sourceContent.trim() === '' || sourceContent.startsWith('--')) {
        showToast('No SQL to copy', true);
        return;
    }
    if (editors.corrected?.setValue) {
        editors.corrected.setValue(sourceContent);
        showToast('Copied to target editor');
    }
}

/**
 * Updates the source dialect label when selector changes.
 */
function handleDialectChange() {
    const selector = document.getElementById('source-dialect-selector');
    if (!selector) return;
    
    const dialectNames = {
        'oracle': 'Oracle',
        'mysql': 'MySQL',
        'sqlserver': 'SQL Server',
        'postgres': 'PostgreSQL',
        'generic': 'Generic SQL'
    };
    
    // Update any dialect label if it exists
    const label = document.getElementById('source-dialect-label');
    if (label) {
        label.textContent = dialectNames[selector.value] || selector.value;
    }
}

/**
 * Fetches and renders the audit log history for the currently selected client.
 * @async
 */
async function fetchAndRenderAuditLogs() {
    if (!state.currentClientId) return;
    try {
        const logs = await apiFetch(`/api/client/${state.currentClientId}/audit_logs`);
        const tbody = document.getElementById('audit-log-body');
        tbody.innerHTML = '';
        if (logs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="3" class="text-center py-4 text-gray-500 dark:text-gray-400">No audit history for this client.</td></tr>';
            return;
        }
        logs.forEach(log => {
            const row = `
                <tr>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600 dark:text-gray-400">${new Date(log.timestamp).toLocaleString()}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-gray-300">${log.action}</td>
                    <td class="px-6 py-4 whitespace-normal text-sm text-gray-600 dark:text-gray-400">${log.details}</td>
                </tr>`;
            tbody.innerHTML += row;
        });
    } catch (error) {
        showToast("Failed to fetch audit logs.", true);
    }
}

/**
 * Selects a migration session, updates the UI, and fetches the files for that session.
 * @async
 * @param {number | null} sessionId - The ID of the session to select, or null to deselect.
 */
async function selectSession(sessionId) {
    if (!sessionId) {
        state.currentSessionId = null;
        const fileBrowserContainer = document.getElementById('file-browser-container');
        if (fileBrowserContainer) fileBrowserContainer.classList.add('hidden');
        renderSessionHistory();
        return;
    }
    
    state.currentSessionId = sessionId;
    renderSessionHistory(); 
    
    try {
        const files = await apiFetch(`/api/session/${sessionId}/files`);
        renderFileBrowser(files);
    } catch (error) {
        showToast("Failed to load files for this session.", true);
    }
}

/**
 * Selects a client, making it active. Fetches and renders all associated data.
 * @async
 * @export
 * @param {number} clientId - The ID of the client to select.
 */
export async function selectClient(clientId) {
    state.currentClientId = clientId;
    if (!clientId) {
        dom.mainContentEl.classList.add('hidden');
        dom.welcomeMessageEl.classList.remove('hidden');
        const activeConfigDisplay = document.getElementById('active-config-display');
        if (activeConfigDisplay) activeConfigDisplay.innerHTML = '';
        state.currentClientId = null;
        renderClients();
        return;
    }
    
    const selectedClient = state.clients.find(c => c.client_id === clientId);
    dom.clientNameHeaderEl.textContent = selectedClient.client_name;
    
    dom.mainContentEl.classList.remove('hidden');
    dom.welcomeMessageEl.classList.add('hidden');
    switchTab('migration');
    
    // Reset UI sections and state for the new client (with null-safety)
    const reportContainer = document.getElementById('report-container');
    const fileBrowserContainer = document.getElementById('file-browser-container');
    const objectSelectorContainer = document.getElementById('object-selector-container');
    const sessionHistoryContainer = document.getElementById('session-history-container');
    const exportReportBtn = document.getElementById('export-report-btn');
    
    if (reportContainer) reportContainer.classList.add('hidden');
    if (fileBrowserContainer) fileBrowserContainer.classList.add('hidden');
    if (objectSelectorContainer) objectSelectorContainer.classList.add('hidden');
    if (sessionHistoryContainer) sessionHistoryContainer.classList.add('hidden');
    if (exportReportBtn) exportReportBtn.disabled = true;

    state.currentReportData = null;
    state.objectList = [];
    state.sessions = [];
    state.currentSessionId = null;
    state.selectedObjects = {};

    try {
        const [config, sessions] = await Promise.all([
            apiFetch(`/api/client/${state.currentClientId}/config`),
            apiFetch(`/api/client/${state.currentClientId}/sessions`)
        ]);
        
        renderActiveConfig(config);
        renderSettingsForms(config);
        populateTypeDropdown(config);
        
        state.sessions = sessions;
        renderSessionHistory();

        // Safe editor value setting with CodeMirror
        if (editors.original?.setValue) {
            editors.original.setValue('-- Paste or load your source SQL here...');
        }
        if (editors.corrected?.setValue) {
            editors.corrected.setValue('-- PostgreSQL-converted SQL will appear here...');
        }
    } catch (error) {
        showToast("Failed to load client configuration or sessions.", true);
    }
}

/**
 * Handles the file input change event to load a local SQL file into the editor.
 * @export
 * @param {Event} event - The file input change event.
 */
export function handleFileSelect(event) {
    const file = event.target.files[0];
    if (!file) return;
    
    const reader = new FileReader();
    reader.onload = function(e) {
        const content = e.target.result;
        
        // Safe editor operations with null checks
        if (editors.original?.setValue) {
            editors.original.setValue(content);
        } else {
            console.warn('Original editor not initialized');
        }
        
        if (editors.corrected?.setValue) {
            editors.corrected.setValue('-- PostgreSQL-converted SQL will appear here...');
        }
        
        updateWorkspaceStatus(file.name);
        showToast(`Loaded ${file.name}`);
        log_audit(state.currentClientId, 'load_sql_file', `Loaded SQL from file: ${file.name}`);
        event.target.value = '';
    };
    reader.onerror = () => showToast('Failed to read the file.', true);
    reader.readAsText(file);
}

/**
 * Fetches the complete list of objects from the Oracle schema and renders the object selector tree.
 * @async
 */
async function handleGetObjectList() {
    if (!state.currentClientId) return;
    
    const button = document.getElementById('run-ora2pg-btn');
    toggleButtonLoading(button, true, '<span>Discover Objects</span>');
    
    try {
        showToast('Fetching all objects from Oracle schema...');
        const objectList = await apiFetch(`/api/client/${state.currentClientId}/get_object_list`);
        
        state.objectList = objectList;
        renderObjectTypeTree();
        showToast('Object list loaded. Please make your selection.');
        log_audit(state.currentClientId, 'get_object_list_success', `Fetched ${objectList.length} objects.`);
    } catch (error) {
        showToast(error.message, true);
        log_audit(state.currentClientId, 'get_object_list_failed', `Failed to fetch object list: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}

/**
 * Handles the grouped Ora2Pg export process using the centrally tracked selected objects.
 * @async
 */
async function handleRunGroupedOra2PgExport() {
    if (!state.currentClientId) return;

    const exportableObjects = Object.entries(state.selectedObjects).filter(([_, names]) => names.length > 0);
    if (exportableObjects.length === 0) {
        showToast('No objects selected for export.', true);
        return;
    }

    const button = document.getElementById('export-selected-btn');
    toggleButtonLoading(button, true, 'Export Selected');

    try {
        for (const [type, names] of exportableObjects) {
            showToast(`Exporting ${names.length} object(s) of type ${type}...`);
            await apiFetch(`/api/client/${state.currentClientId}/run_ora2pg`, {
                method: 'POST',
                body: JSON.stringify({ 
                    selected_objects: names,
                    type: type 
                })
            });
        }
        
        const newSessions = await apiFetch(`/api/client/${state.currentClientId}/sessions`);
        state.sessions = newSessions;
        renderSessionHistory();
        
        showToast(`All exports complete! Check the new session(s).`);
        const objectSelectorContainer = document.getElementById('object-selector-container');
        if (objectSelectorContainer) objectSelectorContainer.classList.add('hidden');
        state.selectedObjects = {};

    } catch (error) {
        showToast(error.message, true);
        log_audit(state.currentClientId, 'run_ora2pg_failed', `Grouped export failed: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}

/**
 * Handles a click on a file in the file browser. Fetches its content and loads it into the editor.
 * @async
 * @param {number} fileId - The ID of the file to load.
 */
async function handleFileClick(fileId) {
    if (!fileId) return;
    state.currentFileId = fileId;

    showToast(`Loading file...`);
    try {
        const data = await apiFetch('/api/get_exported_file', {
            method: 'POST',
            body: JSON.stringify({ file_id: fileId })
        });

        // Safe editor operations
        if (editors.original?.setValue) {
            editors.original.setValue(data.content || '');
        }
        if (editors.corrected?.setValue) {
            editors.corrected.setValue('-- PostgreSQL-converted SQL will appear here...');
        }
        
        updateWorkspaceStatus(data.filename || 'Exported file');
        switchTab('workspace');

    } catch (error) {
        showToast(`Failed to load file: ${error.message}`, true);
    }
}

/**
 * Fetches and triggers the download of the original Oracle DDL for a single object.
 * Uses the global "Pretty DDL" checkbox setting.
 * @async
 * @param {string} objectName - The name of the database object.
 * @param {string} objectType - The type of the database object (e.g., 'TABLE').
 */
async function handleDownloadSingleDDL(objectName, objectType) {
    if (!state.currentClientId || !objectName) return;

    const pretty = document.getElementById('ddl-format-pretty')?.checked || false;

    showToast(`Fetching ${pretty ? 'pretty' : 'raw'} DDL for ${objectName}...`);
    try {
        const data = await apiFetch(`/api/client/${state.currentClientId}/get_oracle_ddl`, {
            method: 'POST',
            body: JSON.stringify({ object_name: objectName, object_type: objectType, pretty: pretty })
        });
        
        const blob = new Blob([data.ddl], { type: 'application/sql' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `${objectName}_oracle${pretty ? '_pretty' : ''}.sql`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        log_audit(state.currentClientId, 'download_oracle_ddl', `Downloaded ${pretty ? 'pretty' : 'raw'} DDL for ${objectName}.`);
    } catch (error) {
        showToast(`Failed to download DDL: ${error.message}`, true);
        log_audit(state.currentClientId, 'download_oracle_ddl_failed', `Failed to download DDL for ${objectName}: ${error.message}`);
    }
}

/**
 * Fetches and triggers the download of a single SQL file containing the DDL for multiple objects.
 * Uses the centrally tracked selected objects and the global "Pretty DDL" checkbox setting.
 * @async
 */
async function handleDownloadBulkDDL() {
    if (!state.currentClientId) return;

    const allSelections = Object.entries(state.selectedObjects).flatMap(([type, names]) => 
        names.map(name => ({ name, type }))
    );

    if (allSelections.length === 0) {
        showToast('Please select at least one object to download.', true);
        return;
    }

    const pretty = document.getElementById('ddl-format-pretty')?.checked || false;
    const button = document.getElementById('download-original-ddl-btn');
    toggleButtonLoading(button, true, 'Download Original DDL');
    showToast(`Fetching ${pretty ? 'pretty' : 'raw'} DDL for ${allSelections.length} objects...`);

    try {
        const data = await apiFetch(`/api/client/${state.currentClientId}/get_bulk_oracle_ddl`, {
            method: 'POST',
            body: JSON.stringify({ objects: allSelections, pretty: pretty })
        });

        const blob = new Blob([data.ddl], { type: 'application/sql' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `oracle_ddl_export${pretty ? '_pretty' : ''}.sql`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        log_audit(state.currentClientId, 'download_bulk_oracle_ddl', `Downloaded bulk ${pretty ? 'pretty' : 'raw'} DDL for ${allSelections.length} objects.`);
    } catch (error) {
        showToast(`Failed to download bulk DDL: ${error.message}`, true);
        log_audit(state.currentClientId, 'download_bulk_oracle_ddl_failed', `Failed to download bulk DDL: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}

/**
 * Triggers the generation of an Ora2Pg assessment report on the backend.
 * @async
 */
async function handleGenerateReport() {
    if (!state.currentClientId) {
        showToast('Please select a client first.', true);
        return;
    }
    const button = document.getElementById('run-report-btn');
    toggleButtonLoading(button, true, '<span>Generate Assessment Report</span>');
    try {
        const response = await apiFetch(`/api/client/${state.currentClientId}/generate_report`, {
            method: 'POST'
        });
        
        state.currentReportData = response;
        renderReportTable(response);
        await log_audit(state.currentClientId, 'generate_report', 'Report generated successfully.');
        showToast('Report generated successfully!');
    } catch (error) {
        await log_audit(state.currentClientId, 'generate_report_failed', `Report generation failed: ${error.message}`);
        showToast(`Failed to generate report: ${error.message}`, true);
    } finally {
        toggleButtonLoading(button, false);
    }
}

/**
 * Exports the currently stored report data as an AsciiDoc file.
 * @export
 */
export function handleExportReport() {
    if (!state.currentReportData) {
        showToast('No report data to export. Please generate a report first.', true);
        return;
    }
    const asciiDocString = convertReportToAsciiDoc(state.currentReportData);
    const blob = new Blob([asciiDocString], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `ora2pg_report_${state.currentClientId}.adoc`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
}

/**
 * Converts the JSON report data from Ora2Pg into a formatted AsciiDoc string.
 * @param {object} data - The JSON report data.
 * @returns {string} The formatted AsciiDoc string.
 */
function convertReportToAsciiDoc(data) {
    let adoc = `= Ora2Pg Migration Assessment Report\n`;
    adoc += `Schema: ${data.Schema} | Version: ${data.Version} | Size: ${data.Size}\n`;
    adoc += `Migration Level: ${data['migration level']} | Estimated Cost: ${data['human days cost']}\n\n`;
    
    adoc += `[cols="2,1,1,1,5a", options="header"]\n`;
    adoc += `|===\n`;
    adoc += `| Object | Number | Invalid | Cost | Comments\n\n`;
    
    data.objects.forEach(item => {
        if (item.object.startsWith('Total')) return;
        adoc += `| ${item.object || ''} | ${item.number || '0'} | ${item.invalid || '0'} | ${item['cost value'] || '0.00'} | ${item.comment || ''}\n`;
    });
    
    const total = data.objects.find(item => item.object.startsWith('Total'));
    if (total) {
        adoc += `\n| *${total.object}* | *${total.number}* | *${total.invalid}* | *${total['cost value']}* | ${total.comment}\n`;
    }

    adoc += `|===\n`;
    return adoc;
}

/**
 * Sends the SQL from the source editor to the AI for conversion to PostgreSQL.
 * Now works with any source dialect and doesn't require a loaded file.
 * @async
 * @export
 */
export async function handleCorrectWithAI() {
    if (!state.currentClientId) {
        showToast('Please select a client first.', true);
        return;
    }

    const originalSql = editors.original?.getValue ? editors.original.getValue() : '';
    
    if (!originalSql || originalSql.trim() === '' || originalSql.startsWith('--')) {
        showToast('No SQL to convert.', true);
        return;
    }

    // Get source dialect
    const dialectSelector = document.getElementById('source-dialect-selector');
    const sourceDialect = dialectSelector ? dialectSelector.value : 'oracle';

    const button = document.getElementById('correct-ai-btn');
    toggleButtonLoading(button, true, 'Convert');
    
    try {
        showToast('Converting SQL with AI...');
        const correctionData = await apiFetch(`/api/correct_sql`, {
            method: 'POST',
            body: JSON.stringify({ 
                sql: originalSql, 
                client_id: state.currentClientId,
                source_dialect: sourceDialect
            })
        });
        
        if (editors.corrected?.setValue) {
            editors.corrected.setValue(correctionData.corrected_sql || '');
        }
        
        // Only update file status if we're working with a loaded file
        if (state.currentFileId) {
            await apiFetch(`/api/file/${state.currentFileId}/status`, {
                method: 'POST',
                body: JSON.stringify({ status: 'corrected' })
            });
            
            if (state.currentSessionId) {
                await selectSession(state.currentSessionId);
            }
        }
        
        showToast(`Conversion complete. Tokens: ${correctionData.metrics.tokens_used}`);
        log_audit(state.currentClientId, 'convert_sql', `Converted ${sourceDialect} to PostgreSQL`);

    } catch (error) {
        showToast(error.message, true);
        log_audit(state.currentClientId, 'convert_sql_failed', `Conversion failed: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}

/**
 * Sends the SQL from the target editor to the backend for validation against a PostgreSQL database.
 * Now works without requiring a loaded file.
 * @async
 * @export
 */
export async function handleValidateSql() {
    if (!state.currentClientId) {
        showToast('Please configure a client first.', true);
        return;
    }

    const correctedSql = editors.corrected?.getValue ? editors.corrected.getValue() : '';
    
    if (!correctedSql || correctedSql.trim() === '' || correctedSql.startsWith('--')) {
        showToast('No SQL to validate.', true);
        return;
    }

    const button = document.getElementById('validate-btn');
    toggleButtonLoading(button, true, 'Validate');
    
    try {
        showToast('Validating SQL...');
        const isCleanSlate = document.getElementById('clean-slate-checkbox')?.checked || false;
        const isAutoCreateDdl = document.getElementById('auto-create-ddl-checkbox')?.checked || false;
        
        const validationData = await apiFetch(`/api/validate`, {
            method: 'POST',
            body: JSON.stringify({ 
                sql: correctedSql, 
                client_id: state.currentClientId,
                clean_slate: isCleanSlate,
                auto_create_ddl: isAutoCreateDdl
            })
        });

        const newStatus = validationData.status === 'success' ? 'validated' : 'failed';
        
        // Only update file status if working with a loaded file
        if (state.currentFileId) {
            await apiFetch(`/api/file/${state.currentFileId}/status`, {
                method: 'POST',
                body: JSON.stringify({ status: newStatus })
            });
            
            if (state.currentSessionId) {
                await selectSession(state.currentSessionId);
            }
        }

        // Update editor if SQL was modified during validation
        if (validationData.corrected_sql && editors.corrected?.setValue) {
            editors.corrected.setValue(validationData.corrected_sql);
        }

        // Show inline validation result
        const resultDiv = document.getElementById('validation-result');
        const titleDiv = document.getElementById('validation-result-title');
        const messageDiv = document.getElementById('validation-result-message');
        
        if (resultDiv && titleDiv && messageDiv) {
            resultDiv.classList.remove('hidden');
            if (newStatus === 'validated') {
                titleDiv.innerHTML = '<i class="fas fa-check-circle text-green-500 mr-2"></i>Validation Successful';
                resultDiv.className = 'mt-2 p-2 rounded text-sm bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800';
            } else {
                titleDiv.innerHTML = '<i class="fas fa-exclamation-circle text-red-500 mr-2"></i>Validation Failed';
                resultDiv.className = 'mt-2 p-2 rounded text-sm bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800';
            }
            messageDiv.textContent = validationData.message;
        }
        
        showToast(validationData.message, newStatus !== 'validated');
        log_audit(state.currentClientId, 'validate_sql', `Validation: ${newStatus}`);

    } catch (error) {
        showToast(error.message, true);
        log_audit(state.currentClientId, 'validate_sql_failed', `Validation error: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}

/**
 * Tests the connection to the validation PostgreSQL database.
 * @async
 * @export
 */
export async function handleTestPgConnection() {
    if (!state.currentClientId) return;
    const pgDsn = document.getElementById('validation_pg_dsn').value;
    if (!pgDsn) {
        showToast('PostgreSQL DSN is required.', true);
        return;
    }
    const button = document.getElementById('test-pg-conn-btn');
    toggleButtonLoading(button, true, 'Test Connection');
    try {
        const data = await apiFetch('/api/test_pg_connection', {
            method: 'POST',
            body: JSON.stringify({ pg_dsn: pgDsn })
        });
        if (data) {
            showToast(data.message, data.status !== 'success');
        }
    } catch(error) {
        showToast(error.message, true);
    } finally {
        toggleButtonLoading(button, false);
    }
}

/**
 * Tests the connection to the source Oracle database using Ora2Pg.
 * @async
 * @export
 */
export async function handleTestOra2PgConnection() {
    if (!state.currentClientId) return;
    const button = document.getElementById('test-ora-conn-btn');
    toggleButtonLoading(button, true, 'Test Oracle Connection');
    
    const formData = new FormData(dom.settingsForm);
    const config = {};
    for (let [key, value] of formData.entries()) {
        const input = dom.settingsForm.querySelector(`[name="${key}"]`);
        if (input.type === 'checkbox') {
            config[key] = input.checked;
        } else {
            config[key] = value;
        }
    }

    try {
        const data = await apiFetch(`/api/client/${state.currentClientId}/test_ora2pg_connection`, {
            method: 'POST',
            body: JSON.stringify(config)
        });
        if (data) {
            showToast(data.message, data.status !== 'success');
        }
    } catch (error) {
        showToast(error.message, true);
    } finally {
        toggleButtonLoading(button, false);
    }
}

/**
 * Handles the submission of the main settings form, saving all configuration.
 * @async
 * @export
 * @param {Event} e - The form submission event.
 */
export async function handleSaveSettings(e) {
    e.preventDefault();
    if (!state.currentClientId) return;
    const formData = new FormData(dom.settingsForm);
    const config = {};
    for (let [key, value] of formData.entries()) {
        const input = dom.settingsForm.querySelector(`[name="${key}"]`);
        if (input.type === 'checkbox') {
            config[key] = input.checked;
        } else {
            config[key] = value;
        }
    }
    try {
        await apiFetch(`/api/client/${state.currentClientId}/config`, {
            method: 'POST',
            body: JSON.stringify(config)
        });
        showToast('Settings saved successfully.');
    } catch (error) {
        showToast(error.message, true);
    }
}

/**
 * Prompts the user for a new client name and creates the client.
 * @async
 * @export
 */
export async function handleAddClient() {
    const clientName = prompt('Enter the new client name:');
    if (clientName && clientName.trim()) {
        try {
            const newClient = await apiFetch('/api/clients', {
                method: 'POST',
                body: JSON.stringify({ client_name: clientName.trim() })
            });
            state.clients.push(newClient);
            state.clients.sort((a, b) => a.client_name.localeCompare(b.client_name));
            selectClient(newClient.client_id);
        } catch(error) {
            showToast(error.message, true);
        }
    }
}

/**
 * Initializes the application by fetching initial data like clients, providers, and config options.
 * @async
 * @export
 */
export async function initializeApp() {
    console.log("Initializing Ora2Pg AI Corrector...");
    try {
        const results = await Promise.all([
            apiFetch('/api/clients'),
            apiFetch('/api/ai_providers'),
            apiFetch('/api/ora2pg_config_options'),
            apiFetch('/api/app_settings')
        ]);
        state.clients = results[0];
        state.aiProviders = results[1];
        state.ora2pgOptions = results[2];
        state.appSettings = results[3];
        renderClients();
    } catch (error) {
        dom.welcomeMessageEl.innerHTML = `<div class="text-center text-red-400 dark:text-red-500">
            <i class="fas fa-exclamation-triangle text-5xl mb-4"></i>
            <h2 class="text-2xl">Failed to initialize application</h2>
            <p class="text-red-600 dark:text-red-400 mt-2">Could not connect to the backend. Please ensure the server is running and accessible.</p>
        </div>`;
        showToast(error.message, true);
    }
}

/**
 * Initializes all the main event listeners for the application.
 * @export
 */
export function initEventListeners() {
    // Listener for the client selector dropdown
    document.getElementById('client-selector').addEventListener('change', e => {
        const selectedValue = e.target.value;
        if (selectedValue === '--new--') {
            handleAddClient();
            if (!state.currentClientId) {
                 e.target.value = '';
            }
        } else if (selectedValue) {
            selectClient(parseInt(selectedValue));
        }
    });

    // Listener for source dialect selector
    const dialectSelector = document.getElementById('source-dialect-selector');
    if (dialectSelector) {
        dialectSelector.addEventListener('change', handleDialectChange);
    }

    // Listener for the main tab bar
    dom.tabsEl.addEventListener('click', e => {
        if (e.target && e.target.matches('.tab-button')) {
            const tabName = e.target.dataset.tab;
            switchTab(tabName);
            if (tabName === 'audit') {
                fetchAndRenderAuditLogs();
            }
        }
    });
    
    // Listener for the "Edit Settings" link in the sidebar
    document.querySelector('body').addEventListener('click', e => {
         if (e.target && e.target.matches('.sidebar .tab-button')) {
            const tabName = e.target.dataset.tab;
            switchTab(tabName);
         }
    });

    // Listener for AI provider dropdown to auto-fill model and endpoint
    dom.settingsForm.addEventListener('change', e => {
        if (e.target.id === 'ai_provider') {
            const selectedProviderName = e.target.value;
            const provider = state.aiProviders.find(p => p.name === selectedProviderName);
            if (provider) {
                const modelInput = document.getElementById('ai_model');
                const endpointInput = document.getElementById('ai_endpoint');
                if (modelInput) modelInput.value = provider.default_model || '';
                if (endpointInput) endpointInput.value = provider.api_endpoint || '';
            }
        }
    });

    // Listener for copy button in the workspace
    const copyBtn = document.getElementById('copy-btn');
    if (copyBtn) {
        copyBtn.addEventListener('click', () => {
            const correctedSql = editors.corrected?.getValue ? editors.corrected.getValue() : '';
            if (!correctedSql || correctedSql.startsWith('--')) {
                showToast('No SQL to copy', true);
                return;
            }
            navigator.clipboard.writeText(correctedSql).then(() => {
                showToast('Copied to clipboard!');
            }, () => {
                showToast('Failed to copy text.', true);
            });
        });
    }
    
    // Main event delegation for various buttons and links
    document.addEventListener('click', e => {
        const target = e.target.closest('button, a.file-item, a.session-item, a.object-type-link');
        if (!target) return;
        
        if (target.classList.contains('object-type-link')) {
            e.preventDefault();
            const objectType = target.dataset.objectType;
            renderObjectList(objectType);
            
            document.querySelectorAll('.object-type-link').forEach(link => {
                link.classList.remove('bg-indigo-600', 'dark:bg-indigo-500', 'text-white');
                link.classList.add('text-gray-700', 'dark:text-gray-300');
            });
            target.classList.remove('text-gray-700', 'dark:text-gray-300');
            target.classList.add('bg-indigo-600', 'dark:bg-indigo-500', 'text-white');
            return;
        }

        if (target.classList.contains('download-ddl-btn')) {
            e.preventDefault();
            const objectName = target.dataset.objectName;
            const objectType = target.dataset.objectType;
            handleDownloadSingleDDL(objectName, objectType);
            return;
        }

        if (target.classList.contains('file-item')) {
            e.preventDefault();
            const fileId = target.dataset.fileId;
            handleFileClick(parseInt(fileId));
            return;
        }
        
        if (target.classList.contains('session-item')) {
            e.preventDefault();
            const sessionId = target.dataset.sessionId;
            selectSession(parseInt(sessionId));
            return;
        }

        // Switch for all buttons with IDs
        switch (target.id) {
            case 'run-report-btn': handleGenerateReport(); break;
            case 'export-report-btn': handleExportReport(); break;
            case 'run-ora2pg-btn': handleGetObjectList(); break; 
            case 'export-selected-btn': handleRunGroupedOra2PgExport(); break;
            case 'download-original-ddl-btn': handleDownloadBulkDDL(); break;
            case 'select-all-objects':
                document.querySelectorAll('#object-list input[type="checkbox"]:not([disabled])').forEach(cb => {
                    if (!cb.checked) cb.click();
                });
                break;
            case 'select-none-objects':
                 document.querySelectorAll('#object-list input[type="checkbox"]:checked').forEach(cb => {
                    if (cb.checked) cb.click();
                 });
                break;
            case 'copy-to-target-btn': handleCopyToTarget(); break;
            case 'clear-workspace-btn': handleClearWorkspace(); break;
            case 'test-pg-conn-btn': handleTestPgConnection(); break;
            case 'test-ora-conn-btn': handleTestOra2PgConnection(); break;
            case 'load-file-proxy-btn': dom.filePicker.click(); break;
            case 'correct-ai-btn': handleCorrectWithAI(); break;
            case 'validate-btn': handleValidateSql(); break;
        }
    });
    
    document.getElementById('object-list').addEventListener('change', e => {
        if (e.target.matches('input[type="checkbox"]')) {
            const objectName = e.target.value;
            const objectType = e.target.dataset.objectType;

            if (!state.selectedObjects[objectType]) {
                state.selectedObjects[objectType] = [];
            }

            if (e.target.checked) {
                if (!state.selectedObjects[objectType].includes(objectName)) {
                    state.selectedObjects[objectType].push(objectName);
                }
            } else {
                state.selectedObjects[objectType] = state.selectedObjects[objectType].filter(name => name !== objectName);
            }
        }
    });
    
    document.addEventListener('input', e => {
        if (e.target.id === 'object-filter-input') {
            const filterValue = e.target.value.toLowerCase();
            const items = document.querySelectorAll('#object-list > div'); 
            items.forEach(item => {
                const label = item.querySelector('label');
                if (label) {
                    const objectName = label.textContent.toLowerCase();
                    if (objectName.includes(filterValue)) {
                        item.style.display = 'flex';
                    } else {
                        item.style.display = 'none';
                    }
                }
            });
        }
    });
    
    dom.filePicker.addEventListener('change', handleFileSelect);
    dom.settingsForm.addEventListener('submit', handleSaveSettings);
}
