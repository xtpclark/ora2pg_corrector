// Monaco Editor Loader
let originalEditor, correctedEditor;

const monacoPath = 'https://cdn.jsdelivr.net/npm/monaco-editor@0.33.0/min/vs';
require.config({ paths: { 'vs': monacoPath }});
window.MonacoEnvironment = {
    getWorkerUrl: function (workerId, label) {
        return `${monacoPath}/editor/editor.worker.js`;
    }
};

require(['vs/editor/editor.main'], function() {
    originalEditor = monaco.editor.create(document.getElementById('original-editor'), {
        value: '-- Oracle SQL will appear here...',
        language: 'sql',
        theme: 'vs-dark',
        readOnly: false,
        automaticLayout: true
    });
    correctedEditor = monaco.editor.create(document.getElementById('corrected-editor'), {
        value: '-- AI-corrected PostgreSQL will appear here...',
        language: 'sql',
        theme: 'vs-dark',
        automaticLayout: true
    });
});

// App Logic
document.addEventListener('DOMContentLoaded', () => {
    let currentClientId = null;
    let clients = [];
    let aiProviders = [];
    let ora2pgOptions = [];
    let appSettings = {};

    // DOM Elements
    const clientListEl = document.getElementById('client-list');
    const mainContentEl = document.getElementById('main-content');
    const welcomeMessageEl = document.getElementById('welcome-message');
    const clientNameHeaderEl = document.getElementById('client-name-header');
    const tabsEl = document.getElementById('main-tabs');
    const settingsForm = document.getElementById('settings-form');
    const filePicker = document.getElementById('sql-file-picker');
    const loadFileProxyBtn = document.getElementById('load-file-proxy-btn');
    
    // --- Helper Functions ---
    function showToast(message, isError = false) {
        const toast = document.getElementById('toast');
        const toastMessage = document.getElementById('toast-message');
        toastMessage.textContent = message;
        toast.className = `toast ${isError ? 'bg-red-600' : 'bg-green-600'} border-transparent text-white py-3 px-5 rounded-lg shadow-lg`;
        toast.classList.add('show');
        setTimeout(() => toast.classList.remove('show'), 3000);
    }

    function toggleButtonLoading(button, isLoading, originalContent = null) {
        const textSpan = button.querySelector('span');
        if (isLoading) {
            button.disabled = true;
            if (textSpan) {
                 if (originalContent) button.dataset.originalContent = originalContent;
                 textSpan.innerHTML = '<i class="fas fa-spinner spinner"></i>';
            }
        } else {
            button.disabled = false;
            if (textSpan && button.dataset.originalContent) {
                textSpan.innerHTML = button.dataset.originalContent;
            }
        }
    }

    async function apiFetch(url, options = {}) {
        try {
            const response = await fetch(url, options);
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `HTTP error! status: ${response.status}`);
            }
            const contentType = response.headers.get("content-type");
            if (contentType && contentType.indexOf("application/json") !== -1) {
                return response.json();
            }
            return;
        } catch (error) {
            showToast(error.message, true);
            console.error('API Fetch Error:', error);
            throw error;
        }
    }
    
    // --- UI Rendering ---
    function renderClients() {
        clientListEl.innerHTML = '';
        clients.forEach(client => {
            const clientItem = document.createElement('a');
            clientItem.href = '#';
            clientItem.className = 'sidebar-item block text-gray-300 hover:bg-gray-700 hover:text-white rounded-md px-3 py-2 text-sm font-medium';
            clientItem.textContent = client.client_name;
            clientItem.dataset.clientId = client.client_id;
            if (client.client_id === currentClientId) {
                clientItem.classList.add('active');
            }
            clientListEl.appendChild(clientItem);
        });
    }

    function switchTab(tabName) {
        document.querySelectorAll('.tab-button').forEach(btn => btn.classList.toggle('active', btn.dataset.tab === tabName));
        document.querySelectorAll('#tab-content .tab-pane').forEach(pane => pane.classList.toggle('hidden', pane.id !== `${tabName}-tab`));
        if (tabName === 'audit') fetchAndRenderAuditLogs();
    }

    function renderSettingsForms(config) {
        const aiContainer = document.getElementById('ai-settings-container');
        aiContainer.innerHTML = '<h3 class="text-xl font-semibold mb-4 border-b border-gray-700 pb-2">AI Provider Settings</h3>';
        let providerOptionsHtml = aiProviders.map(p => `<option value="${p.name}" ${config.ai_provider === p.name ? 'selected' : ''}>${p.name}</option>`).join('');
        
        aiContainer.insertAdjacentHTML('beforeend', `
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                <div>
                    <label for="ai_provider" class="block text-sm font-medium text-gray-300 mb-1">AI Provider</label>
                    <select name="ai_provider" id="ai_provider" class="form-input w-full rounded-md">${providerOptionsHtml}</select>
                </div>
                <div>
                    <label for="ai_model" class="block text-sm font-medium text-gray-300 mb-1">AI Model</label>
                    <input type="text" name="ai_model" id="ai_model" class="form-input w-full rounded-md" value="${config.ai_model || ''}">
                </div>
                 <div>
                    <label for="ai_api_key" class="block text-sm font-medium text-gray-300 mb-1">AI API Key</label>
                    <input type="password" name="ai_api_key" id="ai_api_key" class="form-input w-full rounded-md" value="${config.ai_api_key || ''}">
                </div>
                <div>
                    <label for="ai_endpoint" class="block text-sm font-medium text-gray-300 mb-1">AI Endpoint</label>
                    <input type="text" name="ai_endpoint" id="ai_endpoint" class="form-input w-full rounded-md" value="${config.ai_endpoint || ''}">
                </div>
                <div>
                    <label for="ai_temperature" class="block text-sm font-medium text-gray-300 mb-1">Temperature</label>
                    <input type="number" step="0.1" name="ai_temperature" id="ai_temperature" class="form-input w-full rounded-md" value="${config.ai_temperature || '0.2'}">
                </div>
                <div>
                    <label for="ai_max_output_tokens" class="block text-sm font-medium text-gray-300 mb-1">Max Output Tokens</label>
                    <input type="number" step="1" name="ai_max_output_tokens" id="ai_max_output_tokens" class="form-input w-full rounded-md" value="${config.ai_max_output_tokens || '8192'}">
                </div>
            </div>
        `);

        const ora2pgContainer = document.getElementById('ora2pg-settings-container');
        ora2pgContainer.innerHTML = '<h3 class="text-xl font-semibold mb-4 border-b border-gray-700 pb-2">Ora2Pg Settings</h3>';
        const ora2pgGrid = document.createElement('div');
        ora2pgGrid.className = 'grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6';
        ora2pgOptions.forEach(option => {
            const key = option.option_name.toLowerCase();
            const value = config[key] ?? option.default_value;
            let inputHtml = '';
            if (option.option_type === 'checkbox') {
                inputHtml = `<input type="checkbox" name="${key}" class="form-input rounded mt-1" ${value ? 'checked' : ''}>`;
            } else {
                 inputHtml = `<input type="text" name="${key}" class="form-input w-full rounded-md" value="${value || ''}">`;
            }
            ora2pgGrid.innerHTML += `
                <div>
                    <label for="${key}" class="block text-sm font-medium text-gray-300 mb-1">${option.description}</label>
                    ${inputHtml}
                </div>`;
        });
        ora2pgContainer.appendChild(ora2pgGrid);
        
        document.getElementById('validation_pg_dsn').value = config.validation_pg_dsn || appSettings.validation_pg_dsn || '';
    }

    async function fetchAndRenderAuditLogs() {
        if (!currentClientId) return;
        const logs = await apiFetch(`/api/client/${currentClientId}/audit_logs`);
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
    }

    // --- Event Handlers & Business Logic ---
    async function selectClient(clientId) {
        currentClientId = clientId;
        if (!clientId) {
            mainContentEl.classList.add('hidden');
            welcomeMessageEl.classList.remove('hidden');
            currentClientId = null;
            renderClients();
            return;
        }
        
        const selectedClient = clients.find(c => c.client_id === clientId);
        clientNameHeaderEl.textContent = selectedClient.client_name;
        
        renderClients();
        mainContentEl.classList.remove('hidden');
        welcomeMessageEl.classList.add('hidden');
        switchTab('migration');

        const config = await apiFetch(`/api/client/${currentClientId}/config`);
        renderSettingsForms(config);
        originalEditor.setValue('-- Oracle SQL will appear here...');
        correctedEditor.setValue('-- AI-corrected PostgreSQL will appear here...');
    }

    function handleFileSelect(event) {
        const file = event.target.files[0];
        if (!file) return;

        const reader = new FileReader();
        reader.onload = function(e) {
            const content = e.target.result;
            originalEditor.setValue(content);
            correctedEditor.setValue('-- AI-corrected PostgreSQL will appear here...');
            showToast(`Loaded ${file.name} successfully.`);
            log_audit(currentClientId, 'load_sql_file', `Loaded SQL from file: ${file.name}`);
            event.target.value = '';
        };
        reader.onerror = () => showToast('Failed to read the file.', true);
        reader.readAsText(file);
    }

    async function handleRunOra2Pg() {
        if (!currentClientId) return;
        const button = document.getElementById('run-ora2pg-btn');
        toggleButtonLoading(button, true, 'Run Ora2Pg Export');
        
        try {
            showToast('Ora2Pg export in progress...');
            const data = await apiFetch(`/api/client/${currentClientId}/run_ora2pg`, {
                method: 'POST',
            });
            
            originalEditor.setValue(data.sql_output || '');
            showToast('Ora2Pg export complete! SQL loaded into Workspace.');
            log_audit(currentClientId, 'run_ora2pg_success', 'Ora2Pg export successful.');
            switchTab('workspace');
        } catch (error) {
            log_audit(currentClientId, 'run_ora2pg_failed', `Ora2Pg export failed: ${error.message}`);
        } finally {
            toggleButtonLoading(button, false);
        }
    }
    
    async function handleCorrectWithAI() {
        if (!currentClientId) return;
        const originalSql = originalEditor.getValue();
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
                body: JSON.stringify({ sql: originalSql, client_id: currentClientId })
            });
            correctedEditor.setValue(data.corrected_sql || '');
            showToast(`AI correction complete. Tokens used: ${data.metrics.tokens_used}`);
        } finally {
            toggleButtonLoading(button, false);
        }
    }

    async function handleValidateSql() {
        if (!currentClientId) return;
        const correctedSql = correctedEditor.getValue();
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
                    client_id: currentClientId,
                    clean_slate: isCleanSlate,
                    auto_create_ddl: isAutoCreateDdl
                })
            });
            
            if (data.corrected_sql) {
                correctedEditor.setValue(data.corrected_sql);
            }
            showToast(data.message, data.status !== 'success');
        } finally {
            toggleButtonLoading(button, false);
        }
    }

    async function handleSaveSql() {
        if (!currentClientId) return;
        const button = document.getElementById('save-btn');
        
        const filenameInput = document.getElementById('save-filename-input');
        const filename = filenameInput.value.trim();
        if (!filename) {
            showToast('Filename cannot be empty.', true);
            return;
        }
        
        toggleButtonLoading(button, true, 'Save');

        try {
            const originalSql = originalEditor.getValue();
            const correctedSql = correctedEditor.getValue();
            const data = await apiFetch(`/api/save`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ 
                    original_sql: originalSql, 
                    corrected_sql: correctedSql, 
                    client_id: currentClientId,
                    filename: filename 
                })
            });
            showToast(data.message);
        } finally {
            toggleButtonLoading(button, false);
        }
    }

    async function handleTestPgConnection() {
        const pgDsn = document.getElementById('validation_pg_dsn').value;
        if (!pgDsn) {
            showToast('PostgreSQL DSN is required.', true);
            return;
        }
        const data = await apiFetch('/api/test_pg_connection', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ pg_dsn: pgDsn })
        });
        if (data) {
            showToast(data.message, data.status !== 'success');
        }
    }
    
    async function handleSaveSettings(e) {
        e.preventDefault();
        if (!currentClientId) return;
        const formData = new FormData(settingsForm);
        const config = {};
        for (let [key, value] of formData.entries()) {
            const input = settingsForm.querySelector(`[name="${key}"]`);
            if (input.type === 'checkbox') {
                config[key] = input.checked;
            } else {
                config[key] = value;
            }
        }
        await apiFetch(`/api/client/${currentClientId}/config`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        showToast('Settings saved successfully.');
    }

    async function handleAddClient() {
        const clientName = prompt('Enter the new client name:');
        if (clientName && clientName.trim()) {
            const newClient = await apiFetch('/api/clients', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ client_name: clientName.trim() })
            });
            clients.push(newClient);
            clients.sort((a, b) => a.client_name.localeCompare(b.client_name));
            selectClient(newClient.client_id);
        }
    }

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

    // --- Initialization ---
    async function initializeApp() {
        try {
            const results = await Promise.all([
                apiFetch('/api/clients'),
                apiFetch('/api/ai_providers'),
                apiFetch('/api/ora2pg_config_options'),
                apiFetch('/api/app_settings')
            ]);
            clients = results[0];
            aiProviders = results[1];
            ora2pgOptions = results[2];
            appSettings = results[3];
            renderClients();
        } catch (error) {
            welcomeMessageEl.innerHTML = `<div class="text-center text-red-400">
                <i class="fas fa-exclamation-triangle text-5xl mb-4"></i>
                <h2 class="text-2xl">Failed to initialize application</h2>
                <p class="text-red-500 mt-2">Could not connect to the backend. Please ensure the server is running and accessible.</p>
            </div>`;
            welcomeMessageEl.classList.remove('hidden');
        }
    }
    
    // --- Event Listeners ---
    clientListEl.addEventListener('click', e => {
        if (e.target && e.target.matches('.sidebar-item')) {
            e.preventDefault();
            selectClient(parseInt(e.target.dataset.clientId));
        }
    });

    tabsEl.addEventListener('click', e => {
        if (e.target && e.target.matches('.tab-button')) {
            switchTab(e.target.dataset.tab);
        }
    });

    settingsForm.addEventListener('change', e => {
        if (e.target.id === 'ai_provider') {
            const selectedProviderName = e.target.value;
            const provider = aiProviders.find(p => p.name === selectedProviderName);
            if (provider) {
                const modelInput = document.getElementById('ai_model');
                const endpointInput = document.getElementById('ai_endpoint');
                if (modelInput) {
                    modelInput.value = provider.default_model || '';
                }
                if (endpointInput) {
                    endpointInput.value = provider.api_endpoint || '';
                }
            }
        }
    });

    document.getElementById('copy-btn').addEventListener('click', () => {
        const correctedSql = correctedEditor.getValue();
        navigator.clipboard.writeText(correctedSql).then(() => {
            showToast('Copied to clipboard!');
        }, () => {
            showToast('Failed to copy text.', true);
        });
    });

    document.addEventListener('click', e => {
        const targetId = e.target.id || e.target.parentElement.id;
        switch (targetId) {
            case 'run-ora2pg-btn':
                handleRunOra2Pg();
                break;
            case 'test-pg-conn-btn':
                handleTestPgConnection();
                break;
            case 'add-client-btn':
                handleAddClient();
                break;
            case 'load-file-proxy-btn':
                filePicker.click();
                break;
            case 'correct-ai-btn':
                handleCorrectWithAI();
                break;
            case 'validate-btn':
                handleValidateSql();
                break;
            case 'save-btn':
                handleSaveSql();
                break;
        }
    });
    
    filePicker.addEventListener('change', handleFileSelect);
    settingsForm.addEventListener('submit', handleSaveSettings);

    initializeApp();
});
