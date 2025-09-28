import { state, editors, dom } from './state.js';
import { apiFetch } from './api.js';
import { showToast, toggleButtonLoading, renderClients, switchTab, renderSettingsForms, renderReportTable, renderFileBrowser } from './ui.js';

async function log_audit(clientId, action, details) {
    if (!clientId) return;
    try {
        await apiFetch(`/api/client/${clientId}/log_audit`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
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
    document.getElementById('export-report-btn').disabled = true;
    state.currentReportData = null;

    try {
        const config = await apiFetch(`/api/client/${state.currentClientId}/config`);
        renderSettingsForms(config);
        editors.original.setValue('-- Oracle SQL will appear here...');
        editors.corrected.setValue('-- AI-corrected PostgreSQL will appear here...');
    } catch (error) {
        showToast("Failed to load client configuration.", true);
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

export async function handleRunOra2Pg() {
    if (!state.currentClientId) return;
    const button = document.getElementById('run-ora2pg-btn');
    toggleButtonLoading(button, true, 'Run Ora2Pg Export');
    
    document.getElementById('file-browser-container').classList.add('hidden');
    
    try {
        showToast('Ora2Pg export in progress...');
        const data = await apiFetch(`/api/client/${state.currentClientId}/run_ora2pg`, { method: 'POST' });
        
        if (data.files) {
            renderFileBrowser(data.files);
            showToast(`Ora2Pg export complete! ${data.files.length} files generated.`);
            log_audit(state.currentClientId, 'run_ora2pg_success', `Multi-file export successful.`);
        } else if (data.sql_output) {
            editors.original.setValue(data.sql_output);
            showToast('Ora2Pg export complete! SQL loaded into Workspace.');
            log_audit(state.currentClientId, 'run_ora2pg_success', 'Single-file export successful.');
            switchTab('workspace');
        }

    } catch (error) {
        showToast(error.message, true);
        log_audit(state.currentClientId, 'run_ora2pg_failed', `Ora2Pg export failed: ${error.message}`);
    } finally {
        toggleButtonLoading(button, false);
    }
}

async function handleFileClick(filename) {
    if (!filename) return;

    showToast(`Loading ${filename}...`);
    try {
        const data = await apiFetch('/get_exported_file', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename: filename })
        });

        editors.original.setValue(data.content || '');
        editors.corrected.setValue('-- AI-corrected PostgreSQL will appear here...');
        switchTab('workspace');

    } catch (error) {
        showToast(`Failed to load file: ${error.message}`, true);
    }
}

async function handleGenerateReport() {
    if (!state.currentClientId) {
        showToast('Please select a client first.', true);
        return;
    }
    const button = document.getElementById('run-report-btn');
    toggleButtonLoading(button, true, button.innerHTML);
    try {
        const response = await apiFetch(`/api/client/${state.currentClientId}/generate_report`, {
            method: 'POST'
        });

        if (response.error) {
            await log_audit(state.currentClientId, 'generate_report_failed', `Report generation failed: ${response.error}`);
            showToast(`Failed to generate report: ${response.error}`, true);
            return;
        }

        if (!response || typeof response.objects === 'undefined') {
            await log_audit(state.currentClientId, 'generate_report_failed', 'Report generation failed: Invalid data structure in response.');
            showToast('Failed to generate report: Invalid data received.', true);
            return;
        }

        state.currentReportData = response;
        renderReportTable(response); // This function will now handle showing the container
        
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
    if (!state.currentClientId) return;
    const originalSql = editors.original.getValue();
    if (!originalSql || originalSql.trim() === '' || originalSql.startsWith('-- Oracle SQL')) {
        showToast('No original SQL to correct.', true);
        return;
    }
    const button = document.getElementById('correct-ai-btn');
    toggleButtonLoading(button, true, 'Correct with AI');
    try {
        showToast('AI correction in progress...');
        const data = await apiFetch(`/api/correct_sql`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql: originalSql, client_id: state.currentClientId })
        });
        editors.corrected.setValue(data.corrected_sql || '');
        showToast(`AI correction complete. Tokens used: ${data.metrics.tokens_used}`);
    } catch(error) {
        showToast(error.message, true);
    } finally {
        toggleButtonLoading(button, false);
    }
}

export async function handleValidateSql() {
    if (!state.currentClientId) return;
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
        const data = await apiFetch(`/api/validate`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                sql: correctedSql, 
                client_id: state.currentClientId,
                clean_slate: isCleanSlate,
                auto_create_ddl: isAutoCreateDdl
            })
        });
        if (data.corrected_sql) {
            editors.corrected.setValue(data.corrected_sql);
        }
        showToast(data.message, data.status !== 'success');
    } catch (error) {
        showToast(error.message, true);
    } finally {
        toggleButtonLoading(button, false);
    }
}

export async function handleSaveSql() {
    if (!state.currentClientId) return;
    const filenameInput = document.getElementById('save-filename-input');
    const filename = filenameInput.value.trim();
    if (!filename) {
        showToast('Filename cannot be empty.', true);
        return;
    }
    const button = document.getElementById('save-btn');
    toggleButtonLoading(button, true, 'Save');
    try {
        const originalSql = editors.original.getValue();
        const correctedSql = editors.corrected.getValue();
        const data = await apiFetch(`/api/save`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                original_sql: originalSql, 
                corrected_sql: correctedSql, 
                client_id: state.currentClientId,
                filename: filename 
            })
        });
        showToast(data.message);
    } catch(error) {
        showToast(error.message, true);
    } finally {
        toggleButtonLoading(button, false);
    }
}

export async function handleTestPgConnection() {
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
            headers: { 'Content-Type': 'application/json' },
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
    try {
        const data = await apiFetch(`/api/client/${state.currentClientId}/test_ora2pg_connection`, {
            method: 'POST'
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
            headers: { 'Content-Type': 'application/json' },
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
                headers: { 'Content-Type': 'application/json' },
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
        const button = e.target.closest('button, a.file-item');
        if (!button) return;
        
        if (button.classList.contains('file-item')) {
            e.preventDefault();
            const filename = button.dataset.filename;
            handleFileClick(filename);
            return;
        }

        switch (button.id) {
            case 'run-report-btn': handleGenerateReport(); break;
            case 'export-report-btn': handleExportReport(); break;
            case 'run-ora2pg-btn': handleRunOra2Pg(); break;
            case 'test-pg-conn-btn': handleTestPgConnection(); break;
            case 'test-ora-conn-btn': handleTestOra2PgConnection(); break;
            case 'add-client-btn': handleAddClient(); break;
            case 'load-file-proxy-btn': dom.filePicker.click(); break;
            case 'correct-ai-btn': handleCorrectWithAI(); break;
            case 'validate-btn': handleValidateSql(); break;
            case 'save-btn': handleSaveSql(); break;
        }
    });
    
    dom.filePicker.addEventListener('change', handleFileSelect);
    dom.settingsForm.addEventListener('submit', handleSaveSettings);
}
