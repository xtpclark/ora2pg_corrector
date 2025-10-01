import { state, dom } from './state.js';

export function showToast(message, isError = false) {
    const toast = document.getElementById('toast');
    const toastMessage = document.getElementById('toast-message');
    toastMessage.textContent = message;
    toast.className = `toast ${isError ? 'bg-red-600' : 'bg-green-600'} border-transparent text-white py-3 px-5 rounded-lg shadow-lg`;
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 3000);
}

export function toggleButtonLoading(button, isLoading, originalContent = null) {
    if (!button) return;
    const textSpan = button.querySelector('span');
    const icon = button.querySelector('i');

    if (isLoading) {
        button.disabled = true;
        if (textSpan) {
            if (originalContent) button.dataset.originalContent = originalContent;
            textSpan.textContent = 'Processing...';
        }
        if (icon) {
            button.dataset.originalIcon = icon.className;
            icon.className = 'fas fa-spinner spinner mr-2';
        }
    } else {
        button.disabled = false;
        if (textSpan && button.dataset.originalContent) {
            textSpan.innerHTML = button.dataset.originalContent;
        }
        if (icon && button.dataset.originalIcon) {
            icon.className = button.dataset.originalIcon;
        }
    }
}


export function renderClients() {
    dom.clientListEl.innerHTML = '';
    state.clients.forEach(client => {
        const clientItem = document.createElement('a');
        clientItem.href = '#';
        clientItem.className = 'sidebar-item block text-gray-300 hover:bg-gray-700 hover:text-white rounded-md px-3 py-2 text-sm font-medium';
        clientItem.textContent = client.client_name;
        clientItem.dataset.clientId = client.client_id;
        if (client.client_id === state.currentClientId) {
            clientItem.classList.add('active');
        }
        dom.clientListEl.appendChild(clientItem);
    });
}

export function switchTab(tabName) {
    document.querySelectorAll('.tab-button').forEach(btn => btn.classList.toggle('active', btn.dataset.tab === tabName));
    document.querySelectorAll('#tab-content .tab-pane').forEach(pane => pane.classList.toggle('hidden', pane.id !== `${tabName}-tab`));
    if (tabName === 'audit') {
        // This is handled by an event listener in handlers.js
    }
}

export function renderSettingsForms(config) {
    const aiContainer = document.getElementById('ai-settings-container');
    aiContainer.innerHTML = '<h3 class="text-xl font-semibold mb-4 border-b border-gray-700 pb-2">AI Provider Settings</h3>';
    let providerOptionsHtml = state.aiProviders.map(p => `<option value="${p.name}" ${config.ai_provider === p.name ? 'selected' : ''}>${p.name}</option>`).join('');
    
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
    
    const gridItemsHtml = [];
    state.ora2pgOptions.forEach(option => {
        const key = option.option_name.toLowerCase();
        let value = config[key];
        if (value === undefined) {
             value = option.default_value;
        }
        let isChecked = option.option_type === 'checkbox' ? (String(value).toLowerCase() === 'true' || String(value) === '1') : false;

        let inputHtml = '';
        if (option.option_type === 'checkbox') {
            inputHtml = `<input type="checkbox" name="${key}" id="${key}" class="form-input rounded mt-1" ${isChecked ? 'checked' : ''}>`;
        } else if (option.option_type === 'dropdown') {
            const optionsArray = option.allowed_values ? option.allowed_values.split(',') : [];
            const optionsHtml = optionsArray.map(choice =>
                `<option value="${choice.trim()}" ${choice.trim() === value ? 'selected' : ''}>${choice.trim()}</option>`
            ).join('');
            inputHtml = `<select name="${key}" id="${key}" class="form-input w-full rounded-md">${optionsHtml}</select>`;
        } else {
             const inputType = option.option_type === 'password' ? 'password' : 'text';
             inputHtml = `<input type="${inputType}" name="${key}" id="${key}" class="form-input w-full rounded-md" value="${value || ''}">`;
        }
        
        gridItemsHtml.push(`
            <div>
                <label for="${key}" class="block text-sm font-medium text-gray-300 mb-1">${option.description}</label>
                ${inputHtml}
            </div>
        `);
    });

    ora2pgGrid.innerHTML = gridItemsHtml.join('');
    ora2pgContainer.appendChild(ora2pgGrid);
    
    document.getElementById('validation_pg_dsn').value = config.validation_pg_dsn || state.appSettings.validation_pg_dsn || '';
}

export function renderReportTable(reportData) {
    const reportContainer = document.getElementById('report-container');
    reportContainer.classList.remove('hidden');
    document.getElementById('export-report-btn').disabled = false;
    
    let headerHtml = `
        <div class="mb-4">
            <h2 class="text-xl font-semibold text-gray-200">Migration Assessment Report</h2>
            <p class="text-sm text-gray-400">Schema: ${reportData.Schema || 'N/A'}</p>
            <p class="text-sm text-gray-400">Database: ${reportData.Version || 'N/A'}</p>
            <p class="text-sm text-gray-400">Size: ${reportData.Size || 'N/A'}</p>
            <p class="text-sm text-gray-400">Total Cost: ${reportData['total cost'] || '0'} (Estimated: ${reportData['human days cost'] || 'N/A'})</p>
            <p class="text-sm text-gray-400">Migration Level: ${reportData['migration level'] || 'N/A'}</p>
        </div>
    `;

    let tableHtml = `
        <table class="w-full text-left border-collapse">
            <thead>
                <tr class="bg-gray-800">
                    <th class="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Object</th>
                    <th class="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Number</th>
                    <th class="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Invalid</th>
                    <th class="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Cost</th>
                    <th class="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Comments</th>
                </tr>
            </thead>
            <tbody class="bg-gray-900 divide-y divide-gray-700">
    `;

    if (!reportData.objects || reportData.objects.length === 0) {
        tableHtml += `
            <tr>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-400" colspan="5">No objects found in the report.</td>
            </tr>
        `;
    } else {
        reportData.objects.forEach(item => {
            tableHtml += `
                <tr>
                    <td class="px-6 py-4 whitespace-nowrap text-sm font-semibold text-blue-400">${item.object || ''}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-300">${item.number || '0'}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-300">${item.invalid || '0'}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-300">${item['cost value'] || '0.00'}</td>
                    <td class="px-6 py-4 text-sm text-gray-400">${item.comment || ''}</td>
                </tr>
            `;
        });
    }
    tableHtml += `</tbody></table>`;
    
    reportContainer.innerHTML = headerHtml + tableHtml;
}

export function renderFileBrowser(files) {
    const fileListContainer = document.getElementById('migration-file-list');
    const fileBrowserContainer = document.getElementById('file-browser-container');

    if (!files || files.length === 0) {
        fileListContainer.innerHTML = '<p class="text-gray-500 col-span-full">No SQL files found for this session.</p>';
        fileBrowserContainer.classList.remove('hidden');
        return;
    }

    const statusColors = {
        'generated': 'border-gray-700',
        'corrected': 'border-blue-500',
        'validated': 'border-green-500',
        'failed': 'border-red-500'
    };

    const fileItemsHtml = files.map(file => `
        <a href="#" data-file-id="${file.file_id}" class="file-item bg-gray-800 p-3 rounded-lg text-gray-300 hover:bg-blue-600 hover:text-white transition-colors duration-200 truncate flex items-center border-l-4 ${statusColors[file.status] || 'border-gray-700'}">
            <i class="fas fa-file-alt mr-2 text-gray-500"></i>
            <span class="truncate" title="${file.filename}">${file.filename}</span>
        </a>
    `).join('');
    
    fileListContainer.innerHTML = fileItemsHtml;
    fileBrowserContainer.classList.remove('hidden');
}

export function renderObjectSelector() {
    const container = document.getElementById('object-selector-container');
    const listEl = document.getElementById('object-list');
    listEl.innerHTML = ''; 

    if (!state.objectList || state.objectList.length === 0) {
        listEl.innerHTML = '<p class="text-gray-500 col-span-full">No objects found in the schema.</p>';
        container.classList.remove('hidden');
        return;
    }

    state.objectList.forEach(objectName => {
        const item = document.createElement('div');
        item.className = 'flex items-center';
        item.innerHTML = `
            <input id="obj-${objectName}" name="object" value="${objectName}" type="checkbox" checked class="h-4 w-4 rounded border-gray-600 bg-gray-700 text-indigo-600 focus:ring-indigo-500">
            <label for="obj-${objectName}" class="ml-3 block text-sm font-medium text-gray-300">${objectName}</label>
        `;
        listEl.appendChild(item);
    });

    container.classList.remove('hidden');
}

// --- UPDATED: Renders the session list with the new export_type ---
export function renderSessionHistory() {
    const container = document.getElementById('session-history-container');
    const listEl = document.getElementById('session-list');
    listEl.innerHTML = '';

    if (!state.sessions || state.sessions.length === 0) {
        listEl.innerHTML = '<p class="text-sm text-gray-500">No previous sessions found for this client.</p>';
        container.classList.remove('hidden');
        return;
    }
    
    state.sessions.forEach(session => {
        const item = document.createElement('a');
        item.href = '#';
        item.className = 'session-item block bg-gray-800 p-3 rounded-lg hover:bg-gray-700';
        item.dataset.sessionId = session.session_id;

        if (session.session_id === state.currentSessionId) {
            item.classList.add('active-session');
        }

        item.innerHTML = `
            <div class="flex justify-between items-center">
                <span class="font-semibold text-white">${session.session_name}</span>
                <span class="text-xs bg-gray-700 text-gray-300 px-2 py-1 rounded-full">${session.export_type || 'N/A'}</span>
            </div>
            <div class="text-xs text-gray-400 mt-1">${new Date(session.created_at).toLocaleString()}</div>
        `;
        listEl.appendChild(item);
    });

    container.classList.remove('hidden');
}

