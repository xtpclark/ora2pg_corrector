import { state, editors, dom } from './state.js';
import { apiFetch } from './api.js';
import { showToast, toggleButtonLoading, renderClients, switchTab, renderSettingsForms, renderReportTable, renderFileBrowser, renderObjectSelector, renderSessionHistory, populateTypeDropdown } from './ui.js';

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

async function fetchAndRenderAuditLogs() {
    if (!state.currentClientId) return;
    try {
        const logs = await apiFetch(`/api/client/${state.currentClientId}/audit_logs`);
        const tbody = document.getElementById('audit-log-body');
        tbody.innerHTML = '';
        if (logs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="3" class="text-center py-4 text-gray-500">No audit history for this client.</td></tr>';
            return;
        }
        logs.forEach(log => {
            const row = `
                <tr>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-400">${new Date(log.timestamp).toLocaleString()}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-300">${log.action}</td>
                    <td class="px-6 py-4 whitespace-normal text-sm text-gray-400">${log.details}</td>
                </tr>`;
            tbody.innerHTML += row;
        });
    } catch (error) {
        showToast("Failed to fetch audit logs.", true);
    }
}

async function selectSession(sessionId) {
    if (!sessionId) {
        state.currentSessionId = null;
        document.getElementById('file-browser-container').classList.add('hidden');
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


export async function selectClient(clientId) {
    state.currentClientId = clientId;
    if (!clientId) {
        dom.mainContentEl.classList.add('hidden');
        dom.welcomeMessageEl.classList.remove('hidden');
        state.currentClientId = null;
        renderClients();
        return;
    }
    
    const selectedClient = state.clients.find(c => c.client_id === clientId);
    dom.clientNameHeaderEl.textContent = selectedClient.client_name;
    
    renderClients();
    dom.mainContentEl.classList.remove('hidden');
    dom.welcomeMessageEl.classList.add('hidden');
    switchTab('migration');

    document.getElementById('report-container').classList.add('hidden');
    document.getElementById('file-browser-container').classList.add('hidden');
    document.getElementById('object-selector-container').classList.add('hidden');
    document.getElementById('session-history-container').classList.add('hidden');
    const exportReportBtn = document.getElementById('export-report-btn');
    if (exportReportBtn) exportReportBtn.disabled = true;

    state.currentReportData = null;
    state.objectList = [];
    state.sessions = [];
    state.currentSessionId = null;

    try {
        const [config, sessions] = await Promise.all([
            apiFetch(`/api/client/${state.currentClientId}/config`),
            apiFetch(`/api/client/${state.currentClientId}/sessions`)
        ]);
        
        renderSettingsForms(config);
        // --- THIS IS THE FIX ---
        populateTypeDropdown(config);
        
        state.sessions = sessions;
        renderSessionHistory();

        editors.original.setValue('-- Oracle SQL will appear here...');
        editors.corrected.setValue('-- AI-corrected PostgreSQL will appear here...');
    } catch (error) {
        showToast("Failed to load client configuration or sessions.", true);
    }
}

export function handleFileSelect(event) {
    const file = event.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = function(e) {
        const content = e.target.result;
        editors.original.setValue(content);
        editors.corrected.setValue('-- AI-corrected PostgreSQL will appear here...');
        showToast(`Loaded ${file.name} successfully.`);
        log_audit(state.currentClientId, 'load_sql_file', `Loaded SQL from file: ${file.name}`);
        event.target.value = '';
    };
    reader.onerror = () => showToast('Failed to read the file.', true);
    reader.readAsText(file);
}

async function handleRunOra2PgExport(selectedObjects) {
    if (!state.currentClientId) return;

    const button = document.getElementById('export-selected-btn');
    toggleButtonLoading(button, true, 'Export Selected');

    try {
        showToast('Ora2Pg export in progress...');
        // --- UPDATED: Read the export type from the new dropdown ---
        const exportType = document.getElementById('migration-export-type').value;
        const data = await apiFetch(`/api/client/${state.currentClientId}/run_ora2pg`, {
            method: 'POST',
            body: JSON.stringify({ 
                selected_objects: selectedObjects,
                type: exportType 
            })
        });
        
        const newSessions = await apiFetch(`/api/client/${state.currentClientId}/sessions`);
        state.sessions = newSessions;
        renderSessionHistory();
        
        if (data.session_id) {
            await selectSession(data.session_id);
        }
        
        showToast(`Ora2Pg export complete! ${data.files ? data.files.length : 0} files generated.`);
        document.getElementById('object-selector-container').classList.add('hidden');

    } catch (error) {
        showToast(error.message, true);
        log_audit(state.currentClientId, 'run_ora2pg_failed', `Ora2Pg export failed: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}


async function handleGetObjectList() {
    if (!state.currentClientId) return;
    
    const filePerTableCheckbox = document.getElementById('file_per_table');
    if (!filePerTableCheckbox || !filePerTableCheckbox.checked) {
        await handleRunSingleFileExport();
        return;
    }
    
    const button = document.getElementById('run-ora2pg-btn');
    toggleButtonLoading(button, true, '<span>Run Ora2Pg Export</span>');
    
    document.getElementById('file-browser-container').classList.add('hidden');
    document.getElementById('report-container').classList.add('hidden');

    try {
        showToast('Fetching object list from Oracle schema...');
        const objectList = await apiFetch(`/api/client/${state.currentClientId}/get_object_list`, {
            method: 'POST',
            body: JSON.stringify({ object_types: ['TABLE'] }) // Defaulting to TABLE for now
        });
        state.objectList = objectList;
        renderObjectSelector();
        showToast('Object list loaded. Please make your selection.');
        log_audit(state.currentClientId, 'get_object_list_success', `Fetched ${objectList.length} objects.`);

    } catch (error) {
        showToast(error.message, true);
        log_audit(state.currentClientId, 'get_object_list_failed', `Failed to fetch object list: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}

async function handleRunSingleFileExport() {
    if (!state.currentClientId) return;
    const button = document.getElementById('run-ora2pg-btn');
    toggleButtonLoading(button, true, '<span>Run Ora2Pg Export</span>');
    try {
        showToast('Ora2Pg single-file export in progress...');
        // --- UPDATED: Read the export type from the new dropdown ---
        const exportType = document.getElementById('migration-export-type').value;
        const data = await apiFetch(`/api/client/${state.currentClientId}/run_ora2pg`, { 
            method: 'POST',
            body: JSON.stringify({ type: exportType })
        });
        
        const newSessions = await apiFetch(`/api/client/${state.currentClientId}/sessions`);
        state.sessions = newSessions;
        renderSessionHistory();

        if (data.session_id) {
            await selectSession(data.session_id);
        }

        if (data.sql_output) {
            editors.original.setValue(data.sql_output);
            showToast('Ora2Pg export complete! SQL loaded into Workspace.');
            switchTab('workspace');
        } else {
            showToast('Ora2Pg export complete!');
        }
    } catch (error) {
        showToast(error.message, true);
        log_audit(state.currentClientId, 'run_ora2pg_failed', `Single-file export failed: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}


async function handleFileClick(fileId) {
    if (!fileId) return;
    state.currentFileId = fileId; 

    showToast(`Loading file...`);
    try {
        const data = await apiFetch('/api/get_exported_file', {
            method: 'POST',
            body: JSON.stringify({ file_id: fileId })
        });

        editors.original.setValue(data.content || '');
        editors.corrected.setValue('-- AI-corrected PostgreSQL will appear here...');
        switchTab('workspace');

    } catch (error) {
        showToast(`Failed to load file: ${error.message}`, true);
    }
}

async function handleDownloadSingleDDL(objectName, objectType) {
    if (!state.currentClientId || !objectName) return;

    showToast(`Fetching DDL for ${objectName}...`);
    try {
        const data = await apiFetch(`/api/client/${state.currentClientId}/get_oracle_ddl`, {
            method: 'POST',
            body: JSON.stringify({ object_name: objectName, object_type: objectType })
        });
        
        const blob = new Blob([data.ddl], { type: 'application/sql' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `${objectName}_oracle.sql`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        log_audit(state.currentClientId, 'download_oracle_ddl', `Downloaded original DDL for ${objectName}.`);
    } catch (error) {
        showToast(`Failed to download DDL: ${error.message}`, true);
        log_audit(state.currentClientId, 'download_oracle_ddl_failed', `Failed to download DDL for ${objectName}: ${error.message}`);
    }
}

async function handleDownloadBulkDDL(selectedObjects) {
    if (!state.currentClientId || !selectedObjects || selectedObjects.length === 0) return;

    const button = document.getElementById('download-original-ddl-btn');
    toggleButtonLoading(button, true, 'Download Original DDL');
    showToast(`Fetching DDL for ${selectedObjects.length} objects...`);

    try {
        const data = await apiFetch(`/api/client/${state.currentClientId}/get_bulk_oracle_ddl`, {
            method: 'POST',
            body: JSON.stringify({ objects: selectedObjects })
        });

        const blob = new Blob([data.ddl], { type: 'application/sql' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `oracle_ddl_export.sql`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        log_audit(state.currentClientId, 'download_bulk_oracle_ddl', `Downloaded bulk DDL for ${selectedObjects.length} objects.`);
    } catch (error) {
        showToast(`Failed to download bulk DDL: ${error.message}`, true);
        log_audit(state.currentClientId, 'download_bulk_oracle_ddl_failed', `Failed to download bulk DDL: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}


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

export async function handleCorrectWithAI() {
    if (!state.currentClientId || !state.currentFileId) {
        showToast('Please select a file from a session first.', true);
        return;
    }

    const originalSql = editors.original.getValue();
    if (!originalSql || originalSql.trim() === '' || originalSql.startsWith('-- Oracle SQL')) {
        showToast('No original SQL to correct.', true);
        return;
    }

    const button = document.getElementById('correct-ai-btn');
    toggleButtonLoading(button, true, 'Correct with AI');
    
    try {
        showToast('AI correction in progress...');
        const correctionData = await apiFetch(`/api/correct_sql`, {
            method: 'POST',
            body: JSON.stringify({ sql: originalSql, client_id: state.currentClientId })
        });
        
        editors.corrected.setValue(correctionData.corrected_sql || '');
        
        await apiFetch(`/api/file/${state.currentFileId}/status`, {
            method: 'POST',
            body: JSON.stringify({ status: 'corrected' })
        });
        
        if (state.currentSessionId) {
            await selectSession(state.currentSessionId);
        }
        
        showToast(`AI correction complete. Tokens used: ${correctionData.metrics.tokens_used}`);
        log_audit(state.currentClientId, 'correct_sql_success', `File ID ${state.currentFileId} corrected.`);

    } catch (error) {
        showToast(error.message, true);
        log_audit(state.currentClientId, 'correct_sql_failed', `Failed to correct File ID ${state.currentFileId}: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}

export async function handleValidateSql() {
    if (!state.currentClientId || !state.currentFileId) {
        showToast('Please select a corrected file to validate.', true);
        return;
    }

    const correctedSql = editors.corrected.getValue();
    if (!correctedSql || correctedSql.trim() === '' || correctedSql.startsWith('-- AI-corrected')) {
        showToast('No corrected SQL to validate.', true);
        return;
    }

    const button = document.getElementById('validate-btn');
    toggleButtonLoading(button, true, 'Validate');
    
    try {
        showToast('Validation in progress...');
        const isCleanSlate = document.getElementById('clean-slate-checkbox').checked;
        const isAutoCreateDdl = document.getElementById('auto-create-ddl-checkbox').checked;
        
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
        
        await apiFetch(`/api/file/${state.currentFileId}/status`, {
            method: 'POST',
            body: JSON.stringify({ status: newStatus })
        });

        if (validationData.corrected_sql) {
            editors.corrected.setValue(validationData.corrected_sql);
        }

        if (state.currentSessionId) {
            await selectSession(state.currentSessionId);
        }
        
        showToast(validationData.message, newStatus !== 'validated');
        log_audit(state.currentClientId, 'validate_sql', `Validation for file ID ${state.currentFileId}: ${newStatus}.`);

    } catch (error) {
        showToast(error.message, true);
        log_audit(state.currentClientId, 'validate_sql_failed', `Validation failed for file ID ${state.currentFileId}: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}

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

export async function initializeApp() {
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
        dom.welcomeMessageEl.innerHTML = `<div class="text-center text-red-400">
            <i class="fas fa-exclamation-triangle text-5xl mb-4"></i>
            <h2 class="text-2xl">Failed to initialize application</h2>
            <p class="text-red-500 mt-2">Could not connect to the backend. Please ensure the server is running and accessible.</p>
        </div>`;
        showToast(error.message, true);
    }
}

export function initEventListeners() {
    dom.clientListEl.addEventListener('click', e => {
        if (e.target && e.target.matches('.sidebar-item')) {
            e.preventDefault();
            selectClient(parseInt(e.target.dataset.clientId));
        }
    });

    dom.tabsEl.addEventListener('click', e => {
        if (e.target && e.target.matches('.tab-button')) {
            const tabName = e.target.dataset.tab;
            switchTab(tabName);
            if (tabName === 'audit') {
                fetchAndRenderAuditLogs();
            }
        }
    });

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

    document.getElementById('copy-btn').addEventListener('click', () => {
        const correctedSql = editors.corrected.getValue();
        navigator.clipboard.writeText(correctedSql).then(() => {
            showToast('Copied to clipboard!');
        }, () => {
            showToast('Failed to copy text.', true);
        });
    });
    
    document.addEventListener('click', e => {
        const target = e.target.closest('button, a.file-item, a.session-item');
        if (!target) return;
        
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

        switch (target.id) {
            case 'run-report-btn': handleGenerateReport(); break;
            case 'export-report-btn': handleExportReport(); break;
            case 'run-ora2pg-btn': handleGetObjectList(); break; 
            case 'export-selected-btn': {
                const selected = Array.from(document.querySelectorAll('#object-list input:checked')).map(el => el.value);
                if (selected.length > 0) {
                    handleRunOra2PgExport(selected);
                } else {
                    showToast('Please select at least one object to export.', true);
                }
                break;
            }
            case 'download-original-ddl-btn': {
                const selected = Array.from(document.querySelectorAll('#object-list input:checked')).map(el => {
                    const parent = el.closest('.flex.items-center.justify-between');
                    const button = parent.querySelector('.download-ddl-btn');
                    return { name: el.value, type: button.dataset.objectType };
                });
                if (selected.length > 0) {
                    handleDownloadBulkDDL(selected); 
                } else {
                    showToast('Please select at least one object to download.', true);
                }
                break;
            }
            case 'select-all-objects':
                document.querySelectorAll('#object-list input[type="checkbox"]').forEach(cb => cb.checked = true);
                break;
            case 'select-none-objects':
                 document.querySelectorAll('#object-list input[type="checkbox"]').forEach(cb => cb.checked = false);
                break;
            case 'test-pg-conn-btn': handleTestPgConnection(); break;
            case 'test-ora-conn-btn': handleTestOra2PgConnection(); break;
            case 'add-client-btn': handleAddClient(); break;
            case 'load-file-proxy-btn': dom.filePicker.click(); break;
            case 'correct-ai-btn': handleCorrectWithAI(); break;
            case 'validate-btn': handleValidateSql(); break;
        }
    });
    
    document.addEventListener('input', e => {
        if (e.target.id === 'object-filter-input') {
            const filterValue = e.target.value.toLowerCase();
            const labels = document.querySelectorAll('#object-list label');
            labels.forEach(label => {
                const objectName = label.textContent.toLowerCase();
                const parentDiv = label.closest('.flex.items-center.justify-between');
                if (objectName.includes(filterValue)) {
                    parentDiv.style.display = 'flex';
                } else {
                    parentDiv.style.display = 'none';
                }
            });
        }
    });
    
    dom.filePicker.addEventListener('change', handleFileSelect);
    dom.settingsForm.addEventListener('submit', handleSaveSettings);
}

