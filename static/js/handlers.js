import { state, editors, dom } from './state.js';
import { apiFetch, getMigrationHistory, getSessionDetails, getAllMigrationHistory } from './api.js';
import { showToast, showConfirmModal, showInputModal, toggleButtonLoading, renderClients, switchTab, renderSettingsForms, renderReportTable, renderFileBrowser, renderObjectTypeTree, renderObjectList, renderSessionHistory, populateTypeDropdown, renderActiveConfig, renderMigrationHistory, showSessionDetailsModal, renderGlobalMigrationHistory } from './ui.js';

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

// Running migrations polling interval
let runningMigrationsInterval = null;

/**
 * Fetches and renders the list of running migrations across all clients.
 * @async
 */
async function fetchAndRenderRunningMigrations() {
    const listEl = document.getElementById('running-migrations-list');
    const badgeEl = document.getElementById('running-count-badge');

    if (!listEl) return; // Element not in DOM (probably on a different page)

    try {
        const data = await apiFetch('/api/running_migrations');
        const migrations = data.migrations || [];
        const count = data.running_count || 0;

        // Update badge
        if (badgeEl) {
            if (count > 0) {
                badgeEl.textContent = count;
                badgeEl.classList.remove('hidden');
            } else {
                badgeEl.classList.add('hidden');
            }
        }

        // Render migrations
        if (migrations.length === 0) {
            listEl.innerHTML = `
                <p class="text-gray-500 dark:text-gray-400 text-sm text-center py-4">
                    <i class="fas fa-check-circle mr-2 text-green-500"></i>No migrations currently running
                </p>
            `;
            return;
        }

        listEl.innerHTML = migrations.map(m => {
            const percent = m.total_count > 0 ? Math.round((m.processed_count / m.total_count) * 100) : 0;
            const statusIcon = getStatusIcon(m.workflow_status);
            const statusColor = getStatusColor(m.workflow_status);

            return `
                <div class="border border-gray-200 dark:border-gray-700 rounded-lg p-4 mb-3 last:mb-0">
                    <div class="flex items-center justify-between mb-2">
                        <div class="flex items-center">
                            <i class="${statusIcon} ${statusColor} mr-2"></i>
                            <span class="font-medium text-gray-900 dark:text-white">${escapeHtml(m.client_name)}</span>
                            <span class="ml-2 text-xs bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 px-2 py-0.5 rounded">
                                ${m.export_type || 'DDL'}
                            </span>
                        </div>
                        <span class="text-sm text-gray-500 dark:text-gray-400">
                            Session #${m.session_id}
                        </span>
                    </div>

                    <div class="mb-2">
                        <div class="flex justify-between text-xs text-gray-500 dark:text-gray-400 mb-1">
                            <span>${m.current_phase || m.workflow_status}</span>
                            <span>${m.processed_count} / ${m.total_count} (${percent}%)</span>
                        </div>
                        <div class="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
                            <div class="bg-purple-500 h-2 rounded-full transition-all duration-300" style="width: ${percent}%"></div>
                        </div>
                    </div>

                    <div class="text-xs text-gray-500 dark:text-gray-400">
                        <i class="fas fa-file-code mr-1"></i>${escapeHtml(m.current_file || 'Initializing...')}
                    </div>

                    <div class="mt-2 flex justify-between items-center">
                        <span class="text-xs text-gray-400 dark:text-gray-500">
                            Started: ${formatDateTime(m.started_at)}
                        </span>
                        <button onclick="window.selectClientById(${m.client_id})"
                                class="text-xs text-purple-500 hover:text-purple-600 dark:text-purple-400 dark:hover:text-purple-300">
                            <i class="fas fa-external-link-alt mr-1"></i>View Client
                        </button>
                    </div>
                </div>
            `;
        }).join('');

    } catch (error) {
        console.error('Failed to fetch running migrations:', error);
        listEl.innerHTML = `
            <p class="text-red-500 dark:text-red-400 text-sm text-center py-4">
                <i class="fas fa-exclamation-triangle mr-2"></i>Failed to fetch running migrations
            </p>
        `;
    }
}

/**
 * Returns appropriate icon for migration status
 */
function getStatusIcon(status) {
    switch (status) {
        case 'exporting': return 'fas fa-download';
        case 'validating': return 'fas fa-check-double';
        case 'converting': return 'fas fa-exchange-alt';
        case 'discovering': return 'fas fa-search';
        default: return 'fas fa-spinner fa-spin';
    }
}

/**
 * Returns appropriate color class for migration status
 */
function getStatusColor(status) {
    switch (status) {
        case 'exporting': return 'text-blue-500';
        case 'validating': return 'text-green-500';
        case 'converting': return 'text-yellow-500';
        case 'discovering': return 'text-purple-500';
        default: return 'text-gray-500';
    }
}

/**
 * Escapes HTML characters to prevent XSS
 */
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Formats a datetime string for display
 */
function formatDateTime(dateStr) {
    if (!dateStr) return 'Unknown';
    try {
        const date = new Date(dateStr);
        return date.toLocaleString();
    } catch {
        return dateStr;
    }
}

/**
 * Starts polling for running migrations (every 3 seconds)
 */
function startRunningMigrationsPolling() {
    // Initial fetch
    fetchAndRenderRunningMigrations();

    // Clear any existing interval
    if (runningMigrationsInterval) {
        clearInterval(runningMigrationsInterval);
    }

    // Poll every 3 seconds
    runningMigrationsInterval = setInterval(fetchAndRenderRunningMigrations, 3000);
}

/**
 * Stops polling for running migrations
 */
function stopRunningMigrationsPolling() {
    if (runningMigrationsInterval) {
        clearInterval(runningMigrationsInterval);
        runningMigrationsInterval = null;
    }
}

/**
 * Selects a client by ID and switches to the migration tab.
 * Used by the running migrations dashboard "View Client" button.
 * @param {number} clientId - The ID of the client to select.
 */
function selectClientById(clientId) {
    // Update the dropdown
    const selector = document.getElementById('client-selector');
    if (selector) {
        selector.value = clientId;
        // Trigger the change event to load the client
        selector.dispatchEvent(new Event('change'));
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
 * Fetches and renders the migration history for the current client.
 * @async
 */
async function loadMigrationHistory() {
    if (!state.currentClientId) return;

    const migrationHistorySection = document.getElementById('migration-history-section');

    try {
        const response = await getMigrationHistory(state.currentClientId, 5);
        const migrations = response?.migrations || [];

        if (migrations.length > 0) {
            migrationHistorySection?.classList.remove('hidden');
            renderMigrationHistory(migrations, handleMigrationHistoryClick);
        } else {
            migrationHistorySection?.classList.add('hidden');
        }
    } catch (error) {
        console.error('Failed to load migration history:', error);
        migrationHistorySection?.classList.add('hidden');
    }
}

/**
 * Handles click on a migration history row to show session details.
 * @async
 * @param {number} sessionId - The session ID to show details for.
 */
async function handleMigrationHistoryClick(sessionId) {
    if (!sessionId) return;

    try {
        showToast('Loading session details...');
        const details = await getSessionDetails(sessionId);
        showSessionDetailsModal(details.session, details.files);
    } catch (error) {
        showToast('Failed to load session details: ' + error.message, true);
    }
}

/**
 * Fetches and renders the global migration history (all clients) for the welcome page.
 * @async
 */
async function loadGlobalMigrationHistory() {
    try {
        const response = await getAllMigrationHistory(20);
        const migrations = response?.migrations || [];
        renderGlobalMigrationHistory(migrations, handleMigrationHistoryClick);
    } catch (error) {
        console.error('Failed to load global migration history:', error);
        const container = document.getElementById('global-migration-history-container');
        if (container) {
            container.innerHTML = `
                <div class="text-gray-500 dark:text-gray-400 text-center py-8">
                    <i class="fas fa-exclamation-circle text-4xl mb-3 opacity-50"></i>
                    <p>Failed to load migration history.</p>
                </div>`;
        }
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
    const migrationHistorySection = document.getElementById('migration-history-section');
    const exportReportBtn = document.getElementById('export-report-btn');

    if (reportContainer) reportContainer.classList.add('hidden');
    if (fileBrowserContainer) fileBrowserContainer.classList.add('hidden');
    if (objectSelectorContainer) objectSelectorContainer.classList.add('hidden');
    if (sessionHistoryContainer) sessionHistoryContainer.classList.add('hidden');
    if (migrationHistorySection) migrationHistorySection.classList.add('hidden');
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

        // Load migration history (token/cost tracking)
        loadMigrationHistory();

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
        // Check if database is missing and offer to create it
        if (error.database_missing && error.database_name) {
            const createDb = await showConfirmModal({
                title: 'Database Not Found',
                message: `Database "${error.database_name}" does not exist.\n\nWould you like to create it?`,
                confirmText: 'Create Database',
                confirmClass: 'bg-green-600 hover:bg-green-700'
            });
            if (createDb) {
                await handleCreatePgDatabase(pgDsn, error.database_name);
            }
        } else {
            showToast(error.message, true);
        }
    } finally {
        toggleButtonLoading(button, false);
    }
}

/**
 * Creates a PostgreSQL database.
 * @async
 * @param {string} pgDsn - The PostgreSQL DSN
 * @param {string} dbName - The database name to create
 */
async function handleCreatePgDatabase(pgDsn, dbName) {
    try {
        showToast(`Creating database "${dbName}"...`);
        const result = await apiFetch('/api/create_pg_database', {
            method: 'POST',
            body: JSON.stringify({ pg_dsn: pgDsn })
        });
        showToast(result.message);

        // Re-test connection after creating
        const testResult = await apiFetch('/api/test_pg_connection', {
            method: 'POST',
            body: JSON.stringify({ pg_dsn: pgDsn })
        });
        if (testResult.status === 'success') {
            showToast(testResult.message);
        }
    } catch (error) {
        showToast(`Failed to create database: ${error.message}`, true);
    }
}

/**
 * Fetches available AI models from the provider's API and populates the dropdown.
 * @async
 * @export
 */
export async function handleRefreshAIModels() {
    if (!state.currentClientId) {
        showToast('Please select a client first.', true);
        return;
    }

    const button = document.getElementById('refresh-models-btn');
    const modelSelect = document.getElementById('ai_model');
    const currentValue = modelSelect?.value;

    if (button) {
        button.disabled = true;
        button.classList.add('opacity-50');
    }

    try {
        showToast('Fetching available models...');
        const response = await apiFetch(`/api/client/${state.currentClientId}/ai_models`);

        if (response.models && response.models.length > 0) {
            // Clear existing options and add new ones
            modelSelect.innerHTML = '<option value="">Select a model...</option>';

            response.models.forEach(model => {
                const option = document.createElement('option');
                option.value = model.id;
                option.textContent = model.display_name || model.id;
                if (model.id === currentValue) {
                    option.selected = true;
                }
                modelSelect.appendChild(option);
            });

            showToast(`Found ${response.models.length} available models`);
        } else {
            showToast('No models found from the API', true);
        }
    } catch (error) {
        showToast(error.message || 'Failed to fetch AI models', true);
    } finally {
        if (button) {
            button.disabled = false;
            button.classList.remove('opacity-50');
        }
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

    // Handle unchecked checkboxes (not included in FormData)
    dom.settingsForm.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
        if (!config.hasOwnProperty(checkbox.name)) {
            config[checkbox.name] = checkbox.checked;
        }
    });
    
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
    const clientName = await showInputModal({
        title: 'New Client',
        message: 'Enter a name for the new migration client.',
        placeholder: 'e.g., HR_Production_Migration'
    });

    if (clientName) {
        try {
            const newClient = await apiFetch('/api/clients', {
                method: 'POST',
                body: JSON.stringify({ client_name: clientName })
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

            showToast(`Client "${clientName}" created successfully`);
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

        // Start polling for running migrations (shown on welcome screen)
        startRunningMigrationsPolling();

        // Load global migration history (shown on welcome screen)
        loadGlobalMigrationHistory();

        // Expose selectClientById for the running migrations dashboard
        window.selectClientById = selectClientById;
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
 * Handles editing (renaming) the current client
 */
async function handleEditClient() {
    if (!state.currentClientId) return;

    const currentClient = state.clients.find(c => c.client_id === state.currentClientId);
    if (!currentClient) return;

    const newName = await showInputModal({
        title: 'Rename Client',
        message: 'Enter a new name for this client.',
        placeholder: 'Client name',
        defaultValue: currentClient.client_name
    });

    if (!newName || newName === currentClient.client_name) return;

    try {
        await apiFetch(`/api/client/${state.currentClientId}`, {
            method: 'PUT',
            body: JSON.stringify({ client_name: newName })
        });

        // Update client in state
        currentClient.client_name = newName;
        state.clients.sort((a, b) => a.client_name.localeCompare(b.client_name));

        // Refresh dropdown AND header
        renderClients();
        dom.clientNameHeaderEl.textContent = newName;

        showToast('Client renamed successfully');
        log_audit(state.currentClientId, 'rename_client', `Renamed to: ${newName}`);
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

    const confirmed = await showConfirmModal({
        title: 'Delete Client',
        message: `Are you sure you want to delete "${currentClient.client_name}"?\n\nThis will delete all associated configurations, sessions, and files. This action cannot be undone.`,
        confirmText: 'Delete'
    });

    if (!confirmed) return;

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
    const objectPreviewDiv = document.getElementById('object-preview-container');

    // Get options
    const sessionName = document.getElementById('migration-session-name')?.value?.trim() || '';
    const autoCreateDdl = document.getElementById('migration-auto-create-ddl')?.checked ?? true;
    const cleanSlate = document.getElementById('migration-clean-slate')?.checked ?? false;

    // Confirm if clean slate is enabled
    if (cleanSlate) {
        const confirmed = await showConfirmModal({
            title: 'Clean Slate Mode',
            message: 'This will DROP all existing tables in the validation database before migration.\n\nAre you sure you want to continue?',
            confirmText: 'Continue',
            confirmClass: 'bg-orange-600 hover:bg-orange-700'
        });
        if (!confirmed) return;
    }

    // Update UI to show progress
    button.disabled = true;
    button.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Running...';
    objectPreviewDiv?.classList.add('hidden');
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
        // Build request body
        const requestBody = {
            auto_create_ddl: autoCreateDdl,
            clean_slate: cleanSlate
        };
        if (sessionName) {
            requestBody.session_name = sessionName;
        }

        // Start the migration
        await apiFetch(`/api/client/${state.currentClientId}/start_migration`, {
            method: 'POST',
            body: JSON.stringify(requestBody)
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

    // Fetch object-level summary for this session only (not cumulative)
    if (status.session_id) {
        try {
            const objectSummary = await apiFetch(`/api/session/${status.session_id}/objects/summary`);
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
 * Starts a data export (COPY or INSERT) for the current client.
 * @async
 */
/**
 * Loads the list of tables from Oracle for the data export table selector.
 * @async
 */
async function handleLoadDataTables() {
    if (!state.currentClientId) {
        showToast('Please select a client first.', true);
        return;
    }

    const button = document.getElementById('load-data-tables-btn');
    const listDiv = document.getElementById('data-tables-list');
    const selectAllBtn = document.getElementById('select-all-data-tables');
    const selectNoneBtn = document.getElementById('select-none-data-tables');

    const originalHtml = button.innerHTML;
    button.innerHTML = '<i class="fas fa-spinner fa-spin mr-1"></i>Loading...';
    button.disabled = true;

    try {
        const objectList = await apiFetch(`/api/client/${state.currentClientId}/get_object_list`);

        // Filter to only TABLE type objects
        const tables = objectList.filter(obj => obj.object_type === 'TABLE');

        if (tables.length === 0) {
            listDiv.innerHTML = '<span class="text-gray-400">No tables found in schema</span>';
            return;
        }

        // Build checkbox list
        let html = '<div class="grid grid-cols-2 gap-1">';
        for (const table of tables) {
            html += `
                <label class="flex items-center cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 rounded px-1">
                    <input type="checkbox" value="${table.object_name}"
                           class="data-table-checkbox h-3 w-3 rounded text-green-600 mr-2" checked>
                    <span class="truncate">${table.object_name}</span>
                </label>`;
        }
        html += '</div>';

        listDiv.innerHTML = html;

        // Show select all/none buttons
        selectAllBtn?.classList.remove('hidden');
        selectNoneBtn?.classList.remove('hidden');

    } catch (error) {
        listDiv.innerHTML = `<span class="text-red-500">Error: ${error.message}</span>`;
    } finally {
        button.innerHTML = originalHtml;
        button.disabled = false;
    }
}

/**
 * Selects or deselects all data table checkboxes.
 * @param {boolean} select - True to select all, false to deselect all
 */
function handleSelectAllDataTables(select) {
    const checkboxes = document.querySelectorAll('.data-table-checkbox');
    checkboxes.forEach(cb => cb.checked = select);
}

/**
 * Gets the list of selected tables from the checkbox selector.
 * @returns {string[]|null} Array of selected table names, or null if none selected (meaning all)
 */
function getSelectedDataTables() {
    const checkboxes = document.querySelectorAll('.data-table-checkbox');
    if (checkboxes.length === 0) {
        // No checkboxes loaded - return null for "all tables"
        return null;
    }

    const selected = Array.from(checkboxes)
        .filter(cb => cb.checked)
        .map(cb => cb.value);

    // If all are selected, return null for "all tables"
    if (selected.length === checkboxes.length) {
        return null;
    }

    return selected.length > 0 ? selected : null;
}

/**
 * Starts a data export (COPY or INSERT) for the current client.
 * @async
 */
async function handleStartDataExport() {
    if (!state.currentClientId) {
        showToast('Please select a client first.', true);
        return;
    }

    const button = document.getElementById('start-data-export-btn');
    const formatSelect = document.getElementById('data-export-format');
    const whereInput = document.getElementById('data-export-where');
    const autoLoadCheckbox = document.getElementById('data-auto-load');
    const exactCountsCheckbox = document.getElementById('data-exact-counts');
    const resultsDiv = document.getElementById('data-export-results');

    // Get values from the form
    const exportType = formatSelect?.value || 'COPY';
    const whereClause = whereInput?.value?.trim() || '';
    const autoLoad = autoLoadCheckbox?.checked ?? true;
    const useExactCounts = exactCountsCheckbox?.checked ?? false;

    // Get selected tables from checkboxes (null means all tables)
    const tables = getSelectedDataTables();

    // Update button to show loading state
    const originalHtml = button.innerHTML;
    button.disabled = true;
    button.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Exporting...';

    // Hide previous results
    if (resultsDiv) resultsDiv.classList.add('hidden');

    try {
        // Build request body
        const requestBody = {
            type: exportType,
            session_name: `Data Export (${exportType}) - ${new Date().toLocaleString()}`
        };
        if (tables && tables.length > 0) {
            requestBody.tables = tables;
        }
        if (whereClause) {
            requestBody.where_clause = whereClause;
        }

        // Call the run_ora2pg endpoint
        const result = await apiFetch(`/api/client/${state.currentClientId}/run_ora2pg`, {
            method: 'POST',
            body: JSON.stringify(requestBody)
        });

        const fileCount = result.files?.length || 0;
        const sessionId = result.session_id;

        // Extract table names from exported files for counting
        // File names are like "TABLE_NAME_output_copy.sql" or "output_copy.sql"
        const exportedTables = result.files
            ?.map(f => f.replace(/_output_(copy|insert)\.sql$/i, ''))
            .filter(t => t && !t.startsWith('output_'))
            .map(t => t.toUpperCase()) || [];

        const tablesToCount = tables || exportedTables;

        // Initialize results display
        if (resultsDiv) {
            resultsDiv.innerHTML = `
                <div class="text-xs font-medium text-green-600 dark:text-green-400 mb-2">
                    <i class="fas fa-check-circle mr-1"></i>Export complete - ${fileCount} file(s)
                </div>`;
            resultsDiv.classList.remove('hidden');
        }

        // Auto-load into PostgreSQL if enabled
        let loadResult = null;
        if (autoLoad && sessionId) {
            button.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Loading into PG...';

            if (resultsDiv) {
                resultsDiv.innerHTML += `
                    <div class="text-xs text-gray-500 dark:text-gray-400 mt-2">
                        <i class="fas fa-spinner fa-spin mr-1"></i>Loading data into PostgreSQL...
                    </div>`;
            }

            try {
                loadResult = await apiFetch(`/api/session/${sessionId}/load_data`, {
                    method: 'POST'
                });
            } catch (loadError) {
                // Load failed - show warning but continue
                if (resultsDiv) {
                    resultsDiv.innerHTML += `
                        <div class="text-xs text-yellow-500 mt-2">
                            <i class="fas fa-exclamation-triangle mr-1"></i>Load failed: ${loadError.message}
                        </div>`;
                }
            }
        }

        // Get counts and display final results
        if (tablesToCount.length > 0 && resultsDiv) {
            try {
                const countResult = await apiFetch(`/api/client/${state.currentClientId}/table_counts`, {
                    method: 'POST',
                    body: JSON.stringify({ tables: tablesToCount, exact: useExactCounts })
                });

                // Build results table
                let resultsHtml = `
                    <div class="text-xs font-medium text-green-600 dark:text-green-400 mb-2">
                        <i class="fas fa-check-circle mr-1"></i>Export complete - ${fileCount} file(s)
                    </div>`;

                if (loadResult) {
                    resultsHtml += `
                        <div class="text-xs font-medium text-blue-600 dark:text-blue-400 mb-2">
                            <i class="fas fa-database mr-1"></i>Loaded ${loadResult.loaded_files} file(s) into PostgreSQL
                        </div>`;
                }

                resultsHtml += `
                    <div class="text-xs text-gray-700 dark:text-gray-300">
                        <table class="w-full">
                            <thead>
                                <tr class="border-b border-gray-200 dark:border-gray-700">
                                    <th class="text-left py-1">Table</th>
                                    <th class="text-right py-1">Rows in PG</th>
                                </tr>
                            </thead>
                            <tbody>`;

                for (const [table, data] of Object.entries(countResult.counts)) {
                    const countStr = data.count.toLocaleString();
                    const exactBadge = data.exact ? '' : '<span class="text-gray-400 ml-1">~</span>';
                    const errorClass = data.error ? 'text-red-500' : '';
                    resultsHtml += `
                        <tr class="border-b border-gray-100 dark:border-gray-800">
                            <td class="py-1 ${errorClass}">${table}</td>
                            <td class="text-right py-1 ${errorClass}">${countStr}${exactBadge}</td>
                        </tr>`;
                }

                resultsHtml += `
                            </tbody>
                            <tfoot>
                                <tr class="font-medium">
                                    <td class="py-1">Total</td>
                                    <td class="text-right py-1">${countResult.total.toLocaleString()}</td>
                                </tr>
                            </tfoot>
                        </table>
                        <p class="text-gray-400 mt-1 text-xs">~ = approximate count (use ANALYZE for exact)</p>
                    </div>`;

                resultsDiv.innerHTML = resultsHtml;

            } catch (countError) {
                // Count failed but export succeeded
                let statusHtml = `
                    <div class="text-xs font-medium text-green-600 dark:text-green-400">
                        <i class="fas fa-check-circle mr-1"></i>Export complete - ${fileCount} file(s)
                    </div>`;
                if (loadResult) {
                    statusHtml += `
                        <div class="text-xs font-medium text-blue-600 dark:text-blue-400 mt-1">
                            <i class="fas fa-database mr-1"></i>Loaded ${loadResult.total_rows.toLocaleString()} rows
                        </div>`;
                }
                statusHtml += `
                    <div class="text-xs text-yellow-500 mt-1">
                        <i class="fas fa-exclamation-triangle mr-1"></i>Could not get row counts: ${countError.message}
                    </div>`;
                resultsDiv.innerHTML = statusHtml;
            }
        } else {
            const msg = loadResult
                ? `Data export and load completed! ${loadResult.total_rows.toLocaleString()} rows loaded.`
                : `Data export completed! ${fileCount} file(s) generated.`;
            showToast(msg);
        }

        // Refresh sessions to show the new export
        refreshSessions();

        // Clear the WHERE input after successful export
        if (whereInput) whereInput.value = '';

    } catch (error) {
        const errorMsg = error.message || 'Data export failed';
        const errorDetails = error.details || '';
        showToast(errorMsg, true);
        if (resultsDiv) {
            let errorHtml = `
                <div class="text-xs text-red-500">
                    <i class="fas fa-times-circle mr-1"></i>${errorMsg}
                </div>`;
            if (errorDetails) {
                // Show Oracle/detailed error in a code block
                errorHtml += `
                    <div class="mt-2 p-2 bg-red-50 dark:bg-red-900/20 rounded text-xs font-mono text-red-700 dark:text-red-300 overflow-x-auto whitespace-pre-wrap">
                        ${errorDetails.replace(/</g, '&lt;').replace(/>/g, '&gt;')}
                    </div>`;
            }
            resultsDiv.innerHTML = errorHtml;
            resultsDiv.classList.remove('hidden');
        }
    } finally {
        // Reset button state
        button.disabled = false;
        button.innerHTML = originalHtml;
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

        // Re-render the session history to show the new session
        renderSessionHistory();

        // Refresh migration history (token/cost tracking)
        loadMigrationHistory();

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
        showToast('Please select a client first', true);
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
        showToast('Failed to load DDL cache: ' + error.message, true);
    }
}

/**
 * Clears the DDL cache for the current client.
 */
async function handleClearDDLCache() {
    if (!state.currentClientId) {
        showToast('Please select a client first', true);
        return;
    }

    const confirmed = await showConfirmModal({
        title: 'Clear DDL Cache',
        message: 'Are you sure you want to clear the DDL cache?\n\nThis cannot be undone.',
        confirmText: 'Clear Cache'
    });
    if (!confirmed) return;

    try {
        await apiFetch(`/api/client/${state.currentClientId}/ddl_cache`, { method: 'DELETE' });
        loadDDLCacheStats();
        showToast('DDL cache cleared successfully');
    } catch (error) {
        console.error('Failed to clear DDL cache:', error);
        showToast('Failed to clear DDL cache: ' + error.message, true);
    }
}

/**
 * Gets the current session ID for tools (from state or finds the latest DDL/TABLE session).
 */
function getCurrentSessionId() {
    if (state.currentSessionId) return state.currentSessionId;
    // Find the latest DDL or TABLE session (sessions are already sorted by created_at DESC)
    if (state.sessions && state.sessions.length > 0) {
        // Prefer DDL or TABLE sessions, but fallback to most recent
        const ddlSession = state.sessions.find(s => s.export_type === 'DDL' || s.export_type === 'TABLE');
        return ddlSession ? ddlSession.session_id : state.sessions[0].session_id;
    }
    return null;
}

/**
 * Shows the rollback preview modal.
 */
async function handlePreviewRollback() {
    const sessionId = getCurrentSessionId();
    if (!sessionId) {
        showToast('No session selected. Please run a migration first.', true);
        return;
    }

    try {
        const preview = await apiFetch(`/api/session/${sessionId}/rollback/preview`);

        if (preview.message) {
            showToast(preview.message, true);
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
        showToast('Failed to preview rollback: ' + error.message, true);
    }
}

/**
 * Downloads the rollback script.
 */
async function handleDownloadRollback() {
    const sessionId = state.currentRollbackSessionId || getCurrentSessionId();
    if (!sessionId) {
        showToast('No session selected.', true);
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
        showToast('Failed to download rollback script: ' + error.message, true);
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
        showToast('Rollback script copied to clipboard');
    } catch (error) {
        console.error('Failed to copy rollback:', error);
        showToast('Failed to copy rollback script', true);
    }
}

/**
 * Executes the rollback script on PostgreSQL with confirmation.
 */
async function handleExecuteRollback() {
    const sessionId = state.currentRollbackSessionId || getCurrentSessionId();
    if (!sessionId) {
        showToast('No session selected.', true);
        return;
    }

    // Get object count for confirmation message
    const warningText = document.getElementById('rollback-warning-text')?.textContent || '';

    // First confirmation
    const confirmed1 = await showConfirmModal({
        title: '⚠️ Execute Rollback',
        message: `This will execute the rollback script on PostgreSQL.\n\n${warningText}\n\nThis action cannot be undone.`,
        confirmText: 'Continue',
        confirmClass: 'bg-red-600 hover:bg-red-700'
    });
    if (!confirmed1) return;

    // Double confirmation for safety
    const confirmed2 = await showConfirmModal({
        title: '⚠️ Final Confirmation',
        message: 'Are you ABSOLUTELY SURE you want to drop these objects from the database?\n\nThis cannot be undone!',
        confirmText: 'Yes, Drop Objects',
        confirmClass: 'bg-red-600 hover:bg-red-700'
    });
    if (!confirmed2) return;

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
            showToast(result.message);
            // Close modal and refresh
            document.getElementById('rollback-modal').classList.add('hidden');
            refreshSessions();
        } else {
            showToast(`Rollback failed: ${result.error}`, true);
        }
    } catch (error) {
        console.error('Failed to execute rollback:', error);
        showToast(`Failed to execute rollback: ${error.message}`, true);
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
        showToast('No session selected. Please run a migration first.', true);
        return;
    }

    try {
        const report = await apiFetch(`/api/session/${sessionId}/report`);
        document.getElementById('report-text').textContent = report.content;
        state.currentReportSessionId = sessionId;
        document.getElementById('report-modal').classList.remove('hidden');
    } catch (error) {
        console.error('Failed to load report:', error);
        showToast('Failed to load migration report: ' + error.message, true);
    }
}

/**
 * Downloads the migration report.
 */
async function handleDownloadMigrationReport() {
    const sessionId = state.currentReportSessionId || getCurrentSessionId();
    if (!sessionId) {
        showToast('No session selected.', true);
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
        showToast('Failed to download migration report: ' + error.message, true);
    }
}

/**
 * Copies the migration report to clipboard.
 */
async function handleCopyMigrationReport() {
    const reportText = document.getElementById('report-text').textContent;
    if (reportText) {
        await navigator.clipboard.writeText(reportText);
        showToast('Report copied to clipboard');
    }
}

/**
 * Shows the objects modal with detailed list of migrated objects for the current session.
 */
async function handleViewObjects() {
    if (!state.currentClientId) {
        showToast('Please select a client first', true);
        return;
    }

    const sessionId = getCurrentSessionId();
    if (!sessionId) {
        showToast('No session selected. Please run a migration first.', true);
        return;
    }

    try {
        const summary = await apiFetch(`/api/session/${sessionId}/objects/summary`);

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
        showToast('Failed to load objects: ' + error.message, true);
    }
}

/**
 * Updates the tools status text based on current session.
 */
function updateToolsStatus() {
    const sessionId = getCurrentSessionId();
    if (sessionId) {
        const session = state.sessions?.find(s => s.session_id === sessionId);
        // Use friendly session name, fallback to "Session X" if not found
        const displayName = session?.session_name || `Session ${sessionId}`;
        document.getElementById('rollback-status').textContent = displayName;
        document.getElementById('report-status').textContent = displayName;
    } else {
        document.getElementById('rollback-status').textContent = 'Select a session to view rollback';
        document.getElementById('report-status').textContent = 'Select a session to generate report';
    }
}


/**
 * Toggle the object preview section visibility.
 */
function handleToggleObjectPreview() {
    const container = document.getElementById('object-preview-container');
    const chevron = document.getElementById('preview-chevron');
    if (container) {
        const isHidden = container.classList.contains('hidden');
        container.classList.toggle('hidden');
        chevron?.classList.toggle('rotate-90', isHidden);
    }
}

/**
 * Refresh the object preview list by fetching objects from Oracle.
 * @async
 */
async function handleRefreshObjectPreview() {
    if (!state.currentClientId) {
        showToast('Please select a client first.', true);
        return;
    }

    const listContainer = document.getElementById('object-preview-list');
    const refreshBtn = document.getElementById('refresh-object-preview');

    if (listContainer) {
        listContainer.innerHTML = '<p class="text-center py-4"><i class="fas fa-spinner fa-spin mr-2"></i>Discovering objects...</p>';
    }
    if (refreshBtn) {
        refreshBtn.disabled = true;
    }

    try {
        // Use the same endpoint as the object discovery feature
        const objectList = await apiFetch(`/api/client/${state.currentClientId}/get_object_list`);

        if (!objectList || objectList.length === 0) {
            listContainer.innerHTML = '<p class="text-center py-4">No objects found. Check your Oracle connection settings.</p>';
            return;
        }

        // Group objects by type
        const objectsByType = {};
        for (const obj of objectList) {
            const type = obj.type || 'UNKNOWN';
            if (!objectsByType[type]) {
                objectsByType[type] = [];
            }
            objectsByType[type].push(obj);
        }

        // Build summary of objects by type
        let html = '<div class="grid grid-cols-2 sm:grid-cols-3 gap-2">';
        const objectTypes = Object.keys(objectsByType).sort();

        for (const type of objectTypes) {
            const count = objectsByType[type].length;
            html += `
                <div class="bg-white/10 rounded px-3 py-2">
                    <span class="font-medium">${type}</span>
                    <span class="text-white/70 ml-2">${count}</span>
                </div>
            `;
        }
        html += '</div>';
        html += `<p class="mt-3 text-xs text-white/60">${objectList.length} objects total will be migrated</p>`;

        if (listContainer) {
            listContainer.innerHTML = html;
        }

    } catch (error) {
        console.error('Failed to fetch objects:', error);
        if (listContainer) {
            listContainer.innerHTML = `<p class="text-center py-4 text-red-300">Error: ${error.message}</p>`;
        }
    } finally {
        if (refreshBtn) {
            refreshBtn.disabled = false;
        }
    }
}

/**
 * Toggle the advanced tools section visibility.
 */
function handleToggleAdvancedTools() {
    const content = document.getElementById('advanced-tools-content');
    const chevron = document.getElementById('tools-chevron');
    if (content) {
        const isHidden = content.classList.contains('hidden');
        content.classList.toggle('hidden');
        chevron?.classList.toggle('rotate-180', isHidden);
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

        // Handle file item clicks (use closest() to handle clicks on child elements)
        const fileItem = target.closest('.file-item');
        if (fileItem) {
            e.preventDefault();
            const fileId = fileItem.dataset.fileId;
            handleFileClick(parseInt(fileId));
            return;
        }

        // Handle session item clicks (use closest() to handle clicks on child elements)
        const sessionItem = target.closest('.session-item');
        if (sessionItem) {
            e.preventDefault();
            const sessionId = sessionItem.dataset.sessionId;
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
            case 'refresh-models-btn': handleRefreshAIModels(); break;
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
            // Task-Based UI handlers
            case 'toggle-object-preview': handleToggleObjectPreview(); break;
            case 'refresh-object-preview': handleRefreshObjectPreview(); break;
            case 'toggle-advanced-tools': handleToggleAdvancedTools(); break;
            case 'open-workspace-btn': switchTab('workspace'); break;
            case 'close-assessment-btn': document.getElementById('report-container')?.classList.add('hidden'); break;
            // Data Migration handlers
            case 'load-data-tables-btn': handleLoadDataTables(); break;
            case 'select-all-data-tables': handleSelectAllDataTables(true); break;
            case 'select-none-data-tables': handleSelectAllDataTables(false); break;
            case 'start-data-export-btn': handleStartDataExport(); break;
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
