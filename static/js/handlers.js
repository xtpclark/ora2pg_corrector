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
    
    if (!sourceContent || sourceContent.trim() === '') {
        showToast('Source editor is empty', true);
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
    console.log('selectClient called with:', clientId);
    state.currentClientId = clientId;
    toggleClientActions();
    
    if (!clientId) {
        dom.mainContentEl.classList.add('hidden');
        dom.welcomeMessageEl.classList.remove('hidden');
        const activeConfigDisplay = document.getElementById('active-config-display');
        if (activeConfigDisplay) activeConfigDisplay.innerHTML = '';
        state.currentClientId = null;
        renderClients(); // ONLY render here when deselecting
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

        // Load migration tools data
        loadDDLCacheStats();
        updateToolsStatus();

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
    
    if (!originalSql || originalSql.trim() === '') {
        showToast('Source editor is empty', true);
        return;
    }

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
    
    if (!correctedSql || correctedSql.trim() === '') {
        showToast('Target editor is empty', true);
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
        
        // Reload fresh config from server
        const freshConfig = await apiFetch(`/api/client/${state.currentClientId}/config`);
        
        // Update sidebar with fresh config
        renderActiveConfig(freshConfig);
        
        showToast('Settings saved successfully');
        log_audit(state.currentClientId, 'save_settings', 'Configuration updated');
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
            
            // Add to state and sort
            state.clients.push(newClient);
            state.clients.sort((a, b) => a.client_name.localeCompare(b.client_name));
            
            // Set current client BEFORE rendering
            state.currentClientId = newClient.client_id;
            
            // Now render the dropdown with the selection
            renderClients();
            
            // Then complete the client selection process
            await selectClient(newClient.client_id);
            
            showToast(`Client "${clientName.trim()}" created successfully`);
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
 * Shows the client action buttons when a client is selected
 */
function toggleClientActions() {
    const actionsDiv = document.getElementById('client-actions');
    if (actionsDiv) {
        if (state.currentClientId) {
            actionsDiv.classList.remove('hidden');
        } else {
            actionsDiv.classList.add('hidden');
        }
    }
}

/**
 * Shows a confirmation modal
 */
function showConfirmModal(title, message, onConfirm) {
    const modal = document.getElementById('confirm-modal');
    const titleEl = document.getElementById('confirm-modal-title');
    const messageEl = document.getElementById('confirm-modal-message');
    const confirmBtn = document.getElementById('confirm-modal-confirm');
    const cancelBtn = document.getElementById('confirm-modal-cancel');
    
    titleEl.textContent = title;
    messageEl.textContent = message;
    
    modal.classList.remove('hidden');
    modal.classList.add('flex');
    
    const handleConfirm = () => {
        modal.classList.add('hidden');
        modal.classList.remove('flex');
        confirmBtn.removeEventListener('click', handleConfirm);
        cancelBtn.removeEventListener('click', handleCancel);
        onConfirm();
    };
    
    const handleCancel = () => {
        modal.classList.add('hidden');
        modal.classList.remove('flex');
        confirmBtn.removeEventListener('click', handleConfirm);
        cancelBtn.removeEventListener('click', handleCancel);
    };
    
    confirmBtn.addEventListener('click', handleConfirm);
    cancelBtn.addEventListener('click', handleCancel);
}

/**
 * Handles editing (renaming) the current client
 */
async function handleEditClient() {
    if (!state.currentClientId) return;
    
    const currentClient = state.clients.find(c => c.client_id === state.currentClientId);
    if (!currentClient) return;
    
    const newName = prompt('Enter new client name:', currentClient.client_name);
    if (!newName || newName.trim() === '' || newName === currentClient.client_name) return;
    
    try {
        await apiFetch(`/api/client/${state.currentClientId}`, {
            method: 'PUT',
            body: JSON.stringify({ client_name: newName.trim() })
        });
        
        // Update client in state
        currentClient.client_name = newName.trim();
        state.clients.sort((a, b) => a.client_name.localeCompare(b.client_name));
        
        // Refresh dropdown AND header
        renderClients();
        dom.clientNameHeaderEl.textContent = newName.trim();
        
        showToast('Client renamed successfully');
        log_audit(state.currentClientId, 'rename_client', `Renamed to: ${newName.trim()}`);
    } catch (error) {
        showToast(error.message, true);
    }
}
/**
 * Handles deleting the current client
 */
async function handleDeleteClient() {
    if (!state.currentClientId) return;
    
    const currentClient = state.clients.find(c => c.client_id === state.currentClientId);
    if (!currentClient) return;
    
    showConfirmModal(
        'Delete Client',
        `Are you sure you want to delete "${currentClient.client_name}"? This will delete all associated configurations, sessions, and files. This action cannot be undone.`,
        async () => {
            try {
                await apiFetch(`/api/client/${state.currentClientId}`, {
                    method: 'DELETE'
                });
                
                state.clients = state.clients.filter(c => c.client_id !== state.currentClientId);
                state.currentClientId = null;
                renderClients();
                selectClient(null);
                showToast('Client deleted successfully');
            } catch (error) {
                showToast(error.message, true);
            }
        }
    );
}


// =============================================================================
// One-Click Migration Functions
// =============================================================================

let migrationPollInterval = null;

/**
 * Starts a one-click DDL migration for the current client.
 * @async
 */
async function handleStartMigration() {
    if (!state.currentClientId) {
        showToast('Please select a client first.', true);
        return;
    }

    const button = document.getElementById('start-migration-btn');
    const progressDiv = document.getElementById('migration-progress');
    const resultsDiv = document.getElementById('migration-results');
    const optionsDiv = document.getElementById('migration-options');

    // Get options
    const autoCreateDdl = document.getElementById('migration-auto-create-ddl')?.checked ?? true;
    const cleanSlate = document.getElementById('migration-clean-slate')?.checked ?? false;

    // Confirm if clean slate is enabled
    if (cleanSlate) {
        if (!confirm('Clean slate mode will DROP all existing tables in the validation database before migration. Continue?')) {
            return;
        }
    }

    // Update UI to show progress
    button.disabled = true;
    button.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Running...';
    optionsDiv?.classList.add('hidden');
    resultsDiv?.classList.add('hidden');
    progressDiv?.classList.remove('hidden');

    // Reset progress display
    updateMigrationProgress({
        phase: 'Starting',
        processed_objects: 0,
        total_objects: 0,
        status: 'running'
    });

    try {
        // Start the migration
        await apiFetch(`/api/client/${state.currentClientId}/start_migration`, {
            method: 'POST',
            body: JSON.stringify({
                auto_create_ddl: autoCreateDdl,
                clean_slate: cleanSlate
            })
        });

        showToast('Migration started...');

        // Start polling for status
        migrationPollInterval = setInterval(() => pollMigrationStatus(), 2000);

    } catch (error) {
        showToast(error.message, true);
        resetMigrationUI();
    }
}

/**
 * Polls the migration status endpoint and updates the UI.
 * @async
 */
async function pollMigrationStatus() {
    if (!state.currentClientId) {
        clearInterval(migrationPollInterval);
        return;
    }

    try {
        const status = await apiFetch(`/api/client/${state.currentClientId}/migration_status`);

        updateMigrationProgress(status);

        // Check if migration is complete
        // Keep polling while: running, pending, or in any active phase
        const activeStates = ['running', 'pending', 'discovering', 'exporting', 'converting', 'validating'];
        const isActive = activeStates.includes(status.status) || activeStates.includes(status.phase);

        if (!isActive) {
            clearInterval(migrationPollInterval);
            migrationPollInterval = null;
            showMigrationResults(status);
        }

    } catch (error) {
        console.error('Failed to poll migration status:', error);
    }
}

/**
 * Updates the migration progress UI elements.
 * @param {object} status - The migration status object
 */
function updateMigrationProgress(status) {
    const phaseEl = document.getElementById('migration-phase');
    const countsEl = document.getElementById('migration-counts');
    const progressBar = document.getElementById('migration-progress-bar');
    const statusText = document.getElementById('migration-status-text');

    const phaseNames = {
        'discovery': 'Discovering objects...',
        'export': 'Exporting DDL...',
        'converting': 'Converting with AI...',
        'validating': 'Validating SQL...',
        'completed': 'Complete!',
        'failed': 'Failed',
        'partial': 'Partially Complete'
    };

    if (phaseEl) {
        phaseEl.textContent = phaseNames[status.phase] || status.phase || 'Starting...';
    }

    if (countsEl) {
        countsEl.textContent = `${status.processed_objects || 0} / ${status.total_objects || 0} objects`;
    }

    if (progressBar && status.total_objects > 0) {
        const percent = Math.round((status.processed_objects / status.total_objects) * 100);
        progressBar.style.width = `${percent}%`;
    }

    if (statusText) {
        if (status.successful > 0 || status.failed > 0) {
            statusText.textContent = `${status.successful || 0} successful, ${status.failed || 0} failed`;
        } else {
            statusText.textContent = '';
        }
    }
}

/**
 * Shows the final migration results.
 * @param {object} status - The final migration status
 */
async function showMigrationResults(status) {
    const progressDiv = document.getElementById('migration-progress');
    const resultsDiv = document.getElementById('migration-results');
    const resultsContent = document.getElementById('migration-results-content');

    progressDiv?.classList.add('hidden');
    resultsDiv?.classList.remove('hidden');

    const isSuccess = status.status === 'completed';
    const isPartial = status.status === 'partial';

    let html = `
        <div class="flex items-center mb-3">
            <i class="fas ${isSuccess ? 'fa-check-circle text-green-300' : isPartial ? 'fa-exclamation-circle text-yellow-300' : 'fa-times-circle text-red-300'} text-2xl mr-3"></i>
            <div>
                <div class="text-white font-semibold">${isSuccess ? 'Migration Complete!' : isPartial ? 'Migration Partially Complete' : 'Migration Failed'}</div>
                <div class="text-indigo-100 text-sm">${status.successful || 0} files processed, ${status.failed || 0} failed</div>
            </div>
        </div>
    `;

    // Fetch object-level summary
    try {
        const objectSummary = await apiFetch(`/api/client/${state.currentClientId}/objects/summary`);
        if (objectSummary && objectSummary.totals && objectSummary.totals.total > 0) {
            html += `
                <div class="mt-3 bg-white/10 rounded-lg p-3">
                    <div class="text-white text-sm font-medium mb-2">Objects Summary:</div>
                    <div class="grid grid-cols-2 gap-2 text-xs">
            `;

            // Show by type
            for (const [objType, counts] of Object.entries(objectSummary.by_type)) {
                const successRate = counts.total > 0 ? Math.round((counts.validated / counts.total) * 100) : 0;
                const statusColor = successRate === 100 ? 'text-green-300' : successRate > 0 ? 'text-yellow-300' : 'text-red-300';
                html += `
                    <div class="flex justify-between items-center bg-white/5 rounded px-2 py-1">
                        <span class="text-indigo-100">${objType}</span>
                        <span class="${statusColor}">${counts.validated}/${counts.total}</span>
                    </div>
                `;
            }

            html += `
                    </div>
                    <div class="mt-2 text-center text-indigo-200 text-xs">
                        Total: ${objectSummary.totals.validated}/${objectSummary.totals.total} objects validated
                    </div>
                </div>
            `;
        }
    } catch (e) {
        console.error('Failed to fetch object summary:', e);
    }

    if (status.errors && status.errors.length > 0) {
        html += `
            <div class="mt-3 max-h-32 overflow-y-auto">
                <div class="text-white text-sm font-medium mb-1">Errors:</div>
                <ul class="text-red-200 text-xs space-y-1">
                    ${status.errors.slice(0, 10).map(e => `<li>- ${e}</li>`).join('')}
                    ${status.errors.length > 10 ? `<li class="text-indigo-200">...and ${status.errors.length - 10} more</li>` : ''}
                </ul>
            </div>
        `;
    }

    html += `
        <div class="flex gap-2 mt-4">
            <button id="migration-done-btn" class="bg-white/20 hover:bg-white/30 text-white py-2 px-4 rounded transition-colors text-sm">
                <i class="fas fa-redo mr-1"></i>Run Another
            </button>
            <button id="view-objects-btn" class="bg-white/20 hover:bg-white/30 text-white py-2 px-4 rounded transition-colors text-sm">
                <i class="fas fa-list mr-1"></i>View Objects
            </button>
        </div>
    `;

    if (resultsContent) {
        resultsContent.innerHTML = html;
    }

    resetMigrationUI();

    // Refresh sessions list to show new export
    refreshSessions();
}

/**
 * Resets the migration UI to its initial state.
 */
function resetMigrationUI() {
    const button = document.getElementById('start-migration-btn');
    if (button) {
        button.disabled = false;
        button.innerHTML = '<i class="fas fa-rocket mr-2"></i>Start Migration';
    }
}

/**
 * Refreshes the sessions list after migration.
 * @async
 */
async function refreshSessions() {
    if (!state.currentClientId) return;

    try {
        const sessions = await apiFetch(`/api/client/${state.currentClientId}/sessions`);
        state.sessions = sessions;

        // Import and call renderSessionHistory if available
        const sessionHistoryContainer = document.getElementById('session-history-container');
        if (sessionHistoryContainer && sessions.length > 0) {
            sessionHistoryContainer.classList.remove('hidden');
        }

        // Update tools status after migration completes
        updateToolsStatus();
        loadDDLCacheStats();
    } catch (error) {
        console.error('Failed to refresh sessions:', error);
    }
}

// =============================================================================
// Migration Tools Handlers (DDL Cache, Rollback, Reports)
// =============================================================================

/**
 * Loads DDL cache stats for the current client.
 */
async function loadDDLCacheStats() {
    if (!state.currentClientId) return;
    try {
        const stats = await apiFetch(`/api/client/${state.currentClientId}/ddl_cache/stats`);
        document.getElementById('ddl-cache-count').textContent = `${stats.total_entries} entries`;
        document.getElementById('ddl-cache-hits').textContent = `${stats.total_hits} hits`;
        state.ddlCacheStats = stats;
    } catch (error) {
        console.error('Failed to load DDL cache stats:', error);
    }
}

/**
 * Shows the DDL cache modal with cache entries.
 */
async function handleViewDDLCache() {
    if (!state.currentClientId) {
        alert('Please select a client first');
        return;
    }

    try {
        const stats = await apiFetch(`/api/client/${state.currentClientId}/ddl_cache/stats`);
        const content = document.getElementById('ddl-cache-content');

        if (stats.entries.length === 0) {
            content.innerHTML = '<p class="text-gray-500 dark:text-gray-400 text-center py-8">No cached DDL entries</p>';
        } else {
            content.innerHTML = `
                <div class="space-y-3">
                    <div class="flex justify-between text-sm text-gray-600 dark:text-gray-400 mb-2">
                        <span>Total: ${stats.total_entries} entries</span>
                        <span>Total hits: ${stats.total_hits}</span>
                    </div>
                    ${stats.entries.map(entry => `
                        <div class="bg-gray-50 dark:bg-gray-800 p-3 rounded-lg border border-gray-200 dark:border-gray-700">
                            <div class="flex justify-between items-start">
                                <div>
                                    <span class="font-medium text-gray-900 dark:text-white">${entry.object_name}</span>
                                    <span class="text-xs text-gray-500 ml-2">${entry.object_type}</span>
                                </div>
                                <span class="text-xs bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 px-2 py-1 rounded">
                                    ${entry.hit_count} hits
                                </span>
                            </div>
                            <div class="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                ${entry.ai_provider} / ${entry.ai_model}
                            </div>
                            <div class="text-xs text-gray-400 dark:text-gray-500 mt-1">
                                Created: ${entry.created_at}
                            </div>
                        </div>
                    `).join('')}
                </div>
            `;
        }

        document.getElementById('ddl-cache-modal').classList.remove('hidden');
    } catch (error) {
        console.error('Failed to load DDL cache:', error);
        alert('Failed to load DDL cache: ' + error.message);
    }
}

/**
 * Clears the DDL cache for the current client.
 */
async function handleClearDDLCache() {
    if (!state.currentClientId) {
        alert('Please select a client first');
        return;
    }

    if (!confirm('Are you sure you want to clear the DDL cache? This cannot be undone.')) {
        return;
    }

    try {
        await apiFetch(`/api/client/${state.currentClientId}/ddl_cache`, { method: 'DELETE' });
        loadDDLCacheStats();
        alert('DDL cache cleared successfully');
    } catch (error) {
        console.error('Failed to clear DDL cache:', error);
        alert('Failed to clear DDL cache: ' + error.message);
    }
}

/**
 * Gets the current session ID for tools (from state or finds the latest TABLE session).
 */
function getCurrentSessionId() {
    if (state.currentSessionId) return state.currentSessionId;
    // Find the latest TABLE session
    if (state.sessions && state.sessions.length > 0) {
        const tableSession = state.sessions.find(s => s.export_type === 'TABLE');
        return tableSession ? tableSession.session_id : state.sessions[0].session_id;
    }
    return null;
}

/**
 * Shows the rollback preview modal.
 */
async function handlePreviewRollback() {
    const sessionId = getCurrentSessionId();
    if (!sessionId) {
        alert('No session selected. Please run a migration first.');
        return;
    }

    try {
        const preview = await apiFetch(`/api/session/${sessionId}/rollback/preview`);

        if (preview.message) {
            alert(preview.message);
            return;
        }

        document.getElementById('rollback-warning-text').textContent = preview.warning;

        const content = document.getElementById('rollback-content');
        content.innerHTML = `
            <div class="space-y-2">
                ${preview.objects_to_drop.map(obj => `
                    <div class="flex items-center justify-between py-2 border-b border-gray-200 dark:border-gray-700">
                        <div>
                            <span class="text-sm font-medium text-gray-900 dark:text-white">${obj.name}</span>
                            <span class="text-xs text-gray-500 ml-2">${obj.type}</span>
                            ${obj.table ? `<span class="text-xs text-gray-400 ml-1">ON ${obj.table}</span>` : ''}
                        </div>
                        <code class="text-xs bg-gray-100 dark:bg-gray-800 px-2 py-1 rounded">${obj.drop_statement}</code>
                    </div>
                `).join('')}
            </div>
        `;

        // Store the session ID for download button
        state.currentRollbackSessionId = sessionId;
        document.getElementById('rollback-modal').classList.remove('hidden');

    } catch (error) {
        console.error('Failed to preview rollback:', error);
        alert('Failed to preview rollback: ' + error.message);
    }
}

/**
 * Downloads the rollback script.
 */
async function handleDownloadRollback() {
    const sessionId = state.currentRollbackSessionId || getCurrentSessionId();
    if (!sessionId) {
        alert('No session selected.');
        return;
    }

    try {
        const response = await fetch(`/api/session/${sessionId}/rollback/download`);
        if (!response.ok) throw new Error('Failed to download');

        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `rollback_session_${sessionId}.sql`;
        a.click();
        window.URL.revokeObjectURL(url);
    } catch (error) {
        console.error('Failed to download rollback:', error);
        alert('Failed to download rollback script: ' + error.message);
    }
}

/**
 * Copies the rollback script to clipboard.
 */
async function handleCopyRollback() {
    const sessionId = state.currentRollbackSessionId || getCurrentSessionId();
    if (!sessionId) return;

    try {
        const data = await apiFetch(`/api/session/${sessionId}/rollback`);
        await navigator.clipboard.writeText(data.content);
        alert('Rollback script copied to clipboard');
    } catch (error) {
        console.error('Failed to copy rollback:', error);
        alert('Failed to copy rollback script');
    }
}

/**
 * Executes the rollback script on PostgreSQL with confirmation.
 */
async function handleExecuteRollback() {
    const sessionId = state.currentRollbackSessionId || getCurrentSessionId();
    if (!sessionId) {
        alert('No session selected.');
        return;
    }

    // Get object count for confirmation message
    const warningText = document.getElementById('rollback-warning-text')?.textContent || '';

    if (!confirm(`⚠️ WARNING: This will execute the rollback script on PostgreSQL.\n\n${warningText}\n\nThis action cannot be undone. Continue?`)) {
        return;
    }

    // Double confirmation for safety
    if (!confirm('Are you SURE you want to drop these objects from the database?')) {
        return;
    }

    const executeBtn = document.getElementById('execute-rollback-btn');
    const originalText = executeBtn.innerHTML;
    executeBtn.innerHTML = '<i class="fas fa-spinner fa-spin mr-1"></i>Executing...';
    executeBtn.disabled = true;

    try {
        const result = await apiFetch(`/api/session/${sessionId}/rollback/execute`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ confirm: true })
        });

        if (result.success) {
            alert(`✅ ${result.message}`);
            // Close modal and refresh
            document.getElementById('rollback-modal').classList.add('hidden');
            refreshSessions();
        } else {
            alert(`❌ Rollback failed: ${result.error}\n\n${result.hint || ''}`);
        }
    } catch (error) {
        console.error('Failed to execute rollback:', error);
        alert(`❌ Failed to execute rollback: ${error.message}`);
    } finally {
        executeBtn.innerHTML = originalText;
        executeBtn.disabled = false;
    }
}

/**
 * Shows the migration report modal.
 */
async function handleViewMigrationReport() {
    const sessionId = getCurrentSessionId();
    if (!sessionId) {
        alert('No session selected. Please run a migration first.');
        return;
    }

    try {
        const report = await apiFetch(`/api/session/${sessionId}/report`);
        document.getElementById('report-text').textContent = report.content;
        state.currentReportSessionId = sessionId;
        document.getElementById('report-modal').classList.remove('hidden');
    } catch (error) {
        console.error('Failed to load report:', error);
        alert('Failed to load migration report: ' + error.message);
    }
}

/**
 * Downloads the migration report.
 */
async function handleDownloadMigrationReport() {
    const sessionId = state.currentReportSessionId || getCurrentSessionId();
    if (!sessionId) {
        alert('No session selected.');
        return;
    }

    try {
        const response = await fetch(`/api/session/${sessionId}/report/download`);
        if (!response.ok) throw new Error('Failed to download');

        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `migration_report_session_${sessionId}.adoc`;
        a.click();
        window.URL.revokeObjectURL(url);
    } catch (error) {
        console.error('Failed to download report:', error);
        alert('Failed to download migration report: ' + error.message);
    }
}

/**
 * Copies the migration report to clipboard.
 */
async function handleCopyMigrationReport() {
    const reportText = document.getElementById('report-text').textContent;
    if (reportText) {
        await navigator.clipboard.writeText(reportText);
        alert('Report copied to clipboard');
    }
}

/**
 * Shows the objects modal with detailed list of migrated objects.
 */
async function handleViewObjects() {
    if (!state.currentClientId) {
        alert('Please select a client first');
        return;
    }

    try {
        const summary = await apiFetch(`/api/client/${state.currentClientId}/objects/summary`);

        // Build modal content
        let html = `
            <div class="mb-4">
                <div class="flex justify-between items-center mb-2">
                    <span class="text-lg font-semibold text-gray-900 dark:text-white">
                        ${summary.totals.validated}/${summary.totals.total} Objects Validated
                    </span>
                    <span class="text-sm ${summary.totals.failed > 0 ? 'text-red-500' : 'text-green-500'}">
                        ${summary.totals.failed > 0 ? summary.totals.failed + ' failed' : 'All passed'}
                    </span>
                </div>
            </div>
        `;

        // Show by type with expandable details
        for (const [objType, counts] of Object.entries(summary.by_type)) {
            const successRate = counts.total > 0 ? Math.round((counts.validated / counts.total) * 100) : 0;
            const barColor = successRate === 100 ? 'bg-green-500' : successRate > 0 ? 'bg-yellow-500' : 'bg-red-500';

            html += `
                <div class="mb-3 bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
                    <div class="flex justify-between items-center mb-1">
                        <span class="font-medium text-gray-900 dark:text-white">${objType}</span>
                        <span class="text-sm text-gray-600 dark:text-gray-400">
                            ${counts.validated}/${counts.total} (${successRate}%)
                        </span>
                    </div>
                    <div class="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
                        <div class="${barColor} rounded-full h-2 transition-all" style="width: ${successRate}%"></div>
                    </div>
                </div>
            `;
        }

        // Show in modal
        const content = document.getElementById('ddl-cache-content');
        const modal = document.getElementById('ddl-cache-modal');
        const title = modal?.querySelector('h3');
        if (title) title.textContent = 'Migration Objects';
        if (content) content.innerHTML = html;
        modal?.classList.remove('hidden');

    } catch (error) {
        console.error('Failed to load objects:', error);
        alert('Failed to load objects: ' + error.message);
    }
}

/**
 * Updates the tools status text based on current session.
 */
function updateToolsStatus() {
    const sessionId = getCurrentSessionId();
    if (sessionId) {
        const session = state.sessions?.find(s => s.session_id === sessionId);
        const sessionName = session ? `Session ${sessionId} (${session.export_type})` : `Session ${sessionId}`;
        document.getElementById('rollback-status').textContent = sessionName;
        document.getElementById('report-status').textContent = sessionName;
    } else {
        document.getElementById('rollback-status').textContent = 'Select a session to view rollback';
        document.getElementById('report-status').textContent = 'Select a session to generate report';
    }
}


/**
 * Initializes all the main event listeners for the application.
 * @export
 */
export function initEventListeners() {
    // Listener for the client selector dropdown
    const clientSelector = document.getElementById('client-selector');
    if (clientSelector) {
        clientSelector.addEventListener('change', e => {
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
    }

    // Client management buttons (with null checks)
    const addClientBtn = document.getElementById('add-client-btn');
    if (addClientBtn) {
        addClientBtn.addEventListener('click', handleAddClient);
    }
    
    const editClientBtn = document.getElementById('edit-client-btn');
    if (editClientBtn) {
        editClientBtn.addEventListener('click', handleEditClient);
    }
    
    const deleteClientBtn = document.getElementById('delete-client-btn');
    if (deleteClientBtn) {
        deleteClientBtn.addEventListener('click', handleDeleteClient);
    }

    // Source dialect selector
    const dialectSelector = document.getElementById('source-dialect-selector');
    if (dialectSelector) {
        dialectSelector.addEventListener('change', handleDialectChange);
    }

    // Listener for the main tab bar
    if (dom.tabsEl) {
        dom.tabsEl.addEventListener('click', e => {
            if (e.target && e.target.matches('.tab-button')) {
                const tabName = e.target.dataset.tab;
                switchTab(tabName);
                if (tabName === 'audit') {
                    fetchAndRenderAuditLogs();
                }
            }
        });
    }
    
    // Listener for the "Edit Settings" link in the sidebar
    document.querySelector('body').addEventListener('click', e => {
         if (e.target && e.target.matches('.sidebar .tab-button')) {
            const tabName = e.target.dataset.tab;
            switchTab(tabName);
         }
    });

    // Listener for AI provider dropdown to auto-fill model and endpoint
    if (dom.settingsForm) {
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
    }

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
            case 'start-migration-btn': handleStartMigration(); break;
            case 'migration-done-btn':
                document.getElementById('migration-results')?.classList.add('hidden');
                break;
            case 'view-objects-btn': handleViewObjects(); break;
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
            case 'load-file-proxy-btn':
                if (dom.filePicker) dom.filePicker.click();
                break;
            case 'correct-ai-btn': handleCorrectWithAI(); break;
            case 'validate-btn': handleValidateSql(); break;
            // Migration Tools handlers
            case 'view-ddl-cache-btn': handleViewDDLCache(); break;
            case 'clear-ddl-cache-btn': handleClearDDLCache(); break;
            case 'close-ddl-cache-modal': document.getElementById('ddl-cache-modal').classList.add('hidden'); break;
            case 'preview-rollback-btn': handlePreviewRollback(); break;
            case 'download-rollback-btn': handleDownloadRollback(); break;
            case 'close-rollback-modal': document.getElementById('rollback-modal').classList.add('hidden'); break;
            case 'copy-rollback-btn': handleCopyRollback(); break;
            case 'download-rollback-modal-btn': handleDownloadRollback(); break;
            case 'execute-rollback-btn': handleExecuteRollback(); break;
            case 'view-report-btn': handleViewMigrationReport(); break;
            case 'download-report-btn': handleDownloadMigrationReport(); break;
            case 'close-report-modal': document.getElementById('report-modal').classList.add('hidden'); break;
            case 'copy-report-btn': handleCopyMigrationReport(); break;
            case 'download-report-modal-btn': handleDownloadMigrationReport(); break;
        }
    });
    
    const objectList = document.getElementById('object-list');
    if (objectList) {
        objectList.addEventListener('change', e => {
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
    }
    
    const objectFilterInput = document.getElementById('object-filter-input');
    if (objectFilterInput) {
        objectFilterInput.addEventListener('input', e => {
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
        });
    }
    
    if (dom.filePicker) {
        dom.filePicker.addEventListener('change', handleFileSelect);
    }
    
    if (dom.settingsForm) {
        dom.settingsForm.addEventListener('submit', handleSaveSettings);
    }
}
