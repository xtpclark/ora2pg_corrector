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
        const value = config[key] ?? option.default_value;
        let inputHtml = '';

        if (option.option_type === 'checkbox') {
            inputHtml = `<input type="checkbox" name="${key}" class="form-input rounded mt-1" ${value ? 'checked' : ''}>`;
        } else if (option.option_type === 'dropdown') {
            const optionsArray = option.allowed_values ? option.allowed_values.split(',') : [];
            const optionsHtml = optionsArray.map(choice =>
                `<option value="${choice.trim()}" ${choice.trim() === value ? 'selected' : ''}>${choice.trim()}</option>`
            ).join('');
            inputHtml = `<select name="${key}" id="${key}" class="form-input w-full rounded-md">${optionsHtml}</select>`;
        } else {
             const inputType = option.option_type === 'password' ? 'password' : 'text';
             inputHtml = `<input type="${inputType}" name="${key}" class="form-input w-full rounded-md" value="${value || ''}">`;
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

// --- CHANGE: Reworked to add header and fix the cost bug ---
export function renderReportTable(reportData) {
    const reportContainer = document.getElementById('report-container');
    
    // Build the header section
    let headerHtml = `
        <div class="mb-4 p-4 border border-gray-700 rounded-lg bg-gray-800">
            <h3 class="text-lg font-bold text-white mb-2">Migration Summary</h3>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-2 text-sm">
                <div><span class="font-semibold text-gray-400">Schema:</span> <span class="text-gray-200">${reportData.Schema || 'N/A'}</span></div>
                <div><span class="font-semibold text-gray-400">Size:</span> <span class="text-gray-200">${reportData.Size || 'N/A'}</span></div>
                <div><span class="font-semibold text-gray-400">Migration Level:</span> <span class="text-gray-200">${reportData['migration level'] || 'N/A'}</span></div>
                <div><span class="font-semibold text-gray-400">Estimated Cost:</span> <span class="text-gray-200">${reportData['human days cost'] || 'N/A'}</span></div>
            </div>
        </div>
    `;

    // Build the main table
    let tableHtml = `
        <table class="min-w-full divide-y divide-gray-700">
            <thead class="bg-gray-800">
                <tr>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Object</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Number</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Invalid</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Cost</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Comments</th>
                </tr>
            </thead>
            <tbody class="bg-gray-900 divide-y divide-gray-700">
    `;
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
    tableHtml += `</tbody></table>`;
    
    // Combine header and table
    reportContainer.innerHTML = headerHtml + tableHtml;
}
