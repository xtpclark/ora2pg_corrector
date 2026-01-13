import { state, dom } from './state.js';

/**
 * Displays a toast notification at the bottom of the screen.
 * @export
 * @param {string} message - The message to display in the toast.
 * @param {boolean} [isError=false] - If true, the toast will have a red error style.
 */
export function showToast(message, isError = false) {
    const toast = document.getElementById('toast');
    const toastMessage = document.getElementById('toast-message');
    toastMessage.textContent = message;
    toast.className = `toast ${isError ? 'bg-red-600' : 'bg-green-600'} border-transparent text-white py-3 px-5 rounded-lg shadow-lg`;
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 3000);
}

/**
 * Shows a confirmation modal dialog (replaces browser confirm).
 * @param {Object} options - Configuration options
 * @param {string} options.title - Modal title
 * @param {string} options.message - Confirmation message
 * @param {string} [options.confirmText] - Text for confirm button (default: "Confirm")
 * @param {string} [options.confirmClass] - CSS class for confirm button (default: red)
 * @returns {Promise<boolean>} - Resolves with true if confirmed, false if cancelled
 */
export function showConfirmModal({ title, message, confirmText = 'Confirm', confirmClass = 'bg-red-600 hover:bg-red-700' }) {
    return new Promise((resolve) => {
        const modal = document.getElementById('confirm-modal');
        const titleEl = document.getElementById('confirm-modal-title');
        const messageEl = document.getElementById('confirm-modal-message');
        const confirmBtn = document.getElementById('confirm-modal-confirm');
        const cancelBtn = document.getElementById('confirm-modal-cancel');

        titleEl.textContent = title;
        messageEl.innerHTML = message.replace(/\n/g, '<br>');
        confirmBtn.textContent = confirmText;
        confirmBtn.className = `px-4 py-2 text-white rounded transition-colors ${confirmClass}`;

        modal.classList.remove('hidden');
        modal.classList.add('flex');

        const cleanup = () => {
            modal.classList.add('hidden');
            modal.classList.remove('flex');
            confirmBtn.removeEventListener('click', handleConfirm);
            cancelBtn.removeEventListener('click', handleCancel);
            modal.removeEventListener('click', handleBackdrop);
            document.removeEventListener('keydown', handleKeydown);
        };

        const handleConfirm = () => {
            cleanup();
            resolve(true);
        };

        const handleCancel = () => {
            cleanup();
            resolve(false);
        };

        const handleKeydown = (e) => {
            if (e.key === 'Escape') {
                handleCancel();
            } else if (e.key === 'Enter') {
                handleConfirm();
            }
        };

        const handleBackdrop = (e) => {
            if (e.target === modal) {
                handleCancel();
            }
        };

        confirmBtn.addEventListener('click', handleConfirm);
        cancelBtn.addEventListener('click', handleCancel);
        modal.addEventListener('click', handleBackdrop);
        document.addEventListener('keydown', handleKeydown);
    });
}

/**
 * Shows an input modal dialog (replaces browser prompt).
 * @param {Object} options - Configuration options
 * @param {string} options.title - Modal title
 * @param {string} [options.message] - Optional message/description
 * @param {string} [options.placeholder] - Input placeholder text
 * @param {string} [options.defaultValue] - Default input value
 * @returns {Promise<string|null>} - Resolves with input value or null if cancelled
 */
export function showInputModal({ title, message = '', placeholder = '', defaultValue = '' }) {
    return new Promise((resolve) => {
        const modal = document.getElementById('input-modal');
        const titleEl = document.getElementById('input-modal-title');
        const messageEl = document.getElementById('input-modal-message');
        const input = document.getElementById('input-modal-input');
        const confirmBtn = document.getElementById('input-modal-confirm');
        const cancelBtn = document.getElementById('input-modal-cancel');

        titleEl.textContent = title;
        messageEl.textContent = message;
        messageEl.style.display = message ? 'block' : 'none';
        input.placeholder = placeholder;
        input.value = defaultValue;

        modal.classList.remove('hidden');
        modal.classList.add('flex');
        input.focus();
        input.select();

        const cleanup = () => {
            modal.classList.add('hidden');
            modal.classList.remove('flex');
            confirmBtn.removeEventListener('click', handleConfirm);
            cancelBtn.removeEventListener('click', handleCancel);
            input.removeEventListener('keydown', handleKeydown);
            modal.removeEventListener('click', handleBackdrop);
        };

        const handleConfirm = () => {
            const value = input.value.trim();
            cleanup();
            resolve(value || null);
        };

        const handleCancel = () => {
            cleanup();
            resolve(null);
        };

        const handleKeydown = (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                handleConfirm();
            } else if (e.key === 'Escape') {
                handleCancel();
            }
        };

        const handleBackdrop = (e) => {
            if (e.target === modal) {
                handleCancel();
            }
        };

        confirmBtn.addEventListener('click', handleConfirm);
        cancelBtn.addEventListener('click', handleCancel);
        input.addEventListener('keydown', handleKeydown);
        modal.addEventListener('click', handleBackdrop);
    });
}

/**
 * Toggles the loading state of a button, showing a spinner and disabling it.
 * @export
 * @param {HTMLElement} button - The button element to toggle.
 * @param {boolean} isLoading - If true, sets the button to a loading state.
 * @param {string} [originalContent=null] - The original text/HTML of the button to restore.
 */
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


/**
 * Renders the list of clients into the new dropdown selector.
 * @export
 */
export function renderClients() {
    const selector = document.getElementById('client-selector');
    if (!selector) return;

    selector.innerHTML = ''; // Clear existing options

    // Add a default, disabled placeholder option
    const placeholder = document.createElement('option');
    placeholder.textContent = 'Select a Client...';
    placeholder.value = '';
    placeholder.disabled = true;
    selector.appendChild(placeholder);

    // Populate with clients
    state.clients.forEach(client => {
        const option = document.createElement('option');
        option.value = client.client_id;
        option.textContent = client.client_name;
        if (client.client_id === state.currentClientId) {
            option.selected = true;
        }
        selector.appendChild(option);
    });

    // Add a separator and the "New Client" option
    const separator = document.createElement('option');
    separator.disabled = true;
    separator.textContent = '──────────';
    selector.appendChild(separator);

    const newClientOption = document.createElement('option');
    newClientOption.value = '--new--';
    newClientOption.textContent = 'Create New Client...';
    selector.appendChild(newClientOption);

    // If no client is selected, make the placeholder selected
    if (!state.currentClientId) {
        placeholder.selected = true;
    }
}

/**
 * Renders key-value pairs of the active client's configuration in the sidebar.
 * @export
 * @param {object} config - The client's configuration object.
 */
export function renderActiveConfig(config) {
    const container = document.getElementById('active-config-display');
    if (!container) return;

    // Key configuration items to display
    const keyConfigs = [
        { key: 'oracle_dsn', label: 'Oracle DSN' },
        { key: 'schema', label: 'Schema' },
        { key: 'validation_pg_dsn', label: 'Validation DSN' },
        { key: 'ai_provider', label: 'AI Provider' },
        { key: 'ai_model', label: 'AI Model' }
    ];

    let content = `
        <h3 class="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-2">Active Config</h3>
        <div class="space-y-2 bg-gray-50 dark:bg-gray-800 p-3 rounded text-xs">
    `;

    keyConfigs.forEach(item => {
        let value = config[item.key] || 'Not set';
        
        // Truncate long values
        if (value.length > 30) {
            value = value.substring(0, 27) + '...';
        }
        
        // Mask sensitive values
        if (item.key === 'ai_api_key' || item.key === 'oracle_pwd') {
            value = value !== 'Not set' ? '••••••••' : 'Not set';
        }
        
        content += `
            <div class="flex justify-between">
                <span class="text-gray-600 dark:text-gray-400">${item.label}:</span>
                <span class="text-gray-900 dark:text-gray-200 font-medium truncate ml-2" title="${config[item.key] || ''}">${value}</span>
            </div>
        `;
    });

    content += `
        </div>
        <button data-tab="settings" class="tab-button mt-3 text-xs w-full text-center text-blue-600 dark:text-blue-400 hover:underline">
            <i class="fas fa-cog mr-1"></i> Edit Settings
        </button>
    `;

    container.innerHTML = content;
}
/**
 * Switches the visible tab in the main content area.
 * @export
 * @param {string} tabName - The name of the tab to switch to (e.g., 'migration', 'workspace').
 */
export function switchTab(tabName) {
    document.querySelectorAll('.tab-button').forEach(btn => btn.classList.toggle('active', btn.dataset.tab === tabName));
    document.querySelectorAll('#tab-content .tab-pane').forEach(pane => pane.classList.toggle('hidden', pane.id !== `${tabName}-tab`));
}

/**
 * Populates the 'Export Type' dropdown on the migration pane based on available Ora2Pg options.
 * @export
 * @param {object} currentConfig - The current client's configuration object.
 */
export function populateTypeDropdown(currentConfig) {
    const typeDropdown = document.getElementById('migration-export-type');
    if (!typeDropdown) return;

    const typeOption = state.ora2pgOptions.find(opt => opt.option_name.toUpperCase() === 'TYPE');
    if (!typeOption || !typeOption.allowed_values) {
        console.error("Could not find TYPE options in configuration.");
        return;
    }

    const allowedTypes = typeOption.allowed_values.split(',');
    typeDropdown.innerHTML = ''; // Clear existing options

    allowedTypes.forEach(type => {
        const option = document.createElement('option');
        const trimmedType = type.trim();
        option.value = trimmedType;
        option.textContent = trimmedType;
        if (currentConfig.type === trimmedType) {
            option.selected = true;
        }
        typeDropdown.appendChild(option);
    });
}

/**
 * Renders the dynamic settings forms for AI Provider and Ora2Pg options.
 * @export
 * @param {object} config - The client's current configuration object.
 */
export function renderSettingsForms(config) {
    console.log(`renderSettingsForms called. Number of options to render: ${state.ora2pgOptions.length}`);
    
    const aiContainer = document.getElementById('ai-settings-container');
    aiContainer.innerHTML = '<h3 class="text-xl font-semibold mb-4 border-b border-gray-200 dark:border-gray-700 pb-2 text-gray-900 dark:text-white">AI Provider Settings</h3>';
    let providerOptionsHtml = state.aiProviders.map(p => `<option value="${p.name}" ${config.ai_provider === p.name ? 'selected' : ''}>${p.name}</option>`).join('');
    
    aiContainer.insertAdjacentHTML('beforeend', `
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            <div>
                <label for="ai_provider" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">AI Provider</label>
                <select name="ai_provider" id="ai_provider" class="form-input w-full rounded-md bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100">${providerOptionsHtml}</select>
            </div>
            <div>
                <label for="ai_model" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">AI Model</label>
                <div class="flex gap-2">
                    <select name="ai_model" id="ai_model" class="form-input flex-1 rounded-md bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100">
                        ${config.ai_model ? `<option value="${config.ai_model}" selected>${config.ai_model}</option>` : '<option value="">Select a model...</option>'}
                    </select>
                    <button type="button" id="refresh-models-btn" class="px-3 py-2 bg-purple-600 hover:bg-purple-700 text-white rounded-md text-sm" title="Fetch available models from API">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>
                    </button>
                </div>
            </div>
            <div>
                <label for="ai_api_key" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">AI API Key</label>
                <input type="password" name="ai_api_key" id="ai_api_key" class="form-input w-full rounded-md bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100" value="${config.ai_api_key || ''}">
            </div>
            <div>
                <label for="ai_endpoint" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">AI Endpoint</label>
                <input type="text" name="ai_endpoint" id="ai_endpoint" class="form-input w-full rounded-md bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100" value="${config.ai_endpoint || ''}">
            </div>
            <div>
                <label for="ai_temperature" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Temperature</label>
                <input type="number" step="0.1" name="ai_temperature" id="ai_temperature" class="form-input w-full rounded-md bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100" value="${config.ai_temperature || '0.2'}">
            </div>
            <div>
                <label for="ai_max_output_tokens" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Max Output Tokens</label>
                <input type="number" step="1" name="ai_max_output_tokens" id="ai_max_output_tokens" class="form-input w-full rounded-md bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100" value="${config.ai_max_output_tokens || '8192'}">
            </div>
        </div>
        <details class="mt-4">
            <summary class="cursor-pointer text-sm font-medium text-gray-600 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-200">
                Corporate Proxy Settings (Optional)
            </summary>
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mt-4 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                <div>
                    <label for="ai_user" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">AI User</label>
                    <input type="text" name="ai_user" id="ai_user" class="form-input w-full rounded-md bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100" value="${config.ai_user || 'anonymous'}" placeholder="User ID for tracking">
                </div>
                <div>
                    <label for="ai_user_header" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">User Header Name</label>
                    <input type="text" name="ai_user_header" id="ai_user_header" class="form-input w-full rounded-md bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100" value="${config.ai_user_header || ''}" placeholder="e.g., X-User-ID">
                </div>
                <div>
                    <label for="ssl_cert_path" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">SSL Cert Path</label>
                    <input type="text" name="ssl_cert_path" id="ssl_cert_path" class="form-input w-full rounded-md bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100" value="${config.ssl_cert_path || ''}" placeholder="/path/to/ca-bundle.crt">
                </div>
                <div class="flex items-center pt-6">
                    <input type="checkbox" name="ai_ssl_verify" id="ai_ssl_verify" class="form-checkbox h-4 w-4 text-purple-600 rounded" ${config.ai_ssl_verify !== false && config.ai_ssl_verify !== 'false' ? 'checked' : ''}>
                    <label for="ai_ssl_verify" class="ml-2 block text-sm text-gray-700 dark:text-gray-300">Verify SSL</label>
                </div>
            </div>
        </details>
    `);

    const ora2pgContainer = document.getElementById('ora2pg-settings-container');
    ora2pgContainer.innerHTML = '<h3 class="text-xl font-semibold mb-4 border-b border-gray-200 dark:border-gray-700 pb-2 text-gray-900 dark:text-white">Ora2Pg Settings</h3>';
    const ora2pgGrid = document.createElement('div');
    ora2pgGrid.className = 'grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6';
    
    const gridItemsHtml = [];
    state.ora2pgOptions.forEach(option => {
        if (option.option_name.toUpperCase() === 'TYPE') {
            return;
        }
        const key = option.option_name.toLowerCase();
        let value = config[key];
        if (value === undefined) {
            value = option.default_value;
        }
        let isChecked = option.option_type === 'checkbox' ? (String(value).toLowerCase() === 'true' || String(value) === '1') : false;

        let inputHtml = '';
        if (option.option_type === 'checkbox') {
            inputHtml = `<input type="checkbox" name="${key}" id="${key}" class="form-checkbox h-4 w-4 text-purple-600 rounded" ${isChecked ? 'checked' : ''}>`;
        } else if (option.option_type === 'dropdown') {
            const optionsArray = option.allowed_values ? option.allowed_values.split(',') : [];
            const optionsHtml = optionsArray.map(choice =>
                `<option value="${choice.trim()}" ${choice.trim() === value ? 'selected' : ''}>${choice.trim()}</option>`
            ).join('');
            inputHtml = `<select name="${key}" id="${key}" class="form-input w-full rounded-md bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100">${optionsHtml}</select>`;
        } else {
            const inputType = option.option_type === 'password' ? 'password' : 'text';
            inputHtml = `<input type="${inputType}" name="${key}" id="${key}" class="form-input w-full rounded-md bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100" value="${value || ''}">`;
        }
        
        gridItemsHtml.push(`
            <div>
                <label for="${key}" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">${option.description}</label>
                ${inputHtml}
            </div>
        `);
    });

    ora2pgGrid.innerHTML = gridItemsHtml.join('');
    ora2pgContainer.appendChild(ora2pgGrid);
    
    document.getElementById('validation_pg_dsn').value = config.validation_pg_dsn || state.appSettings.validation_pg_dsn || '';
}

/**
 * Renders the Ora2Pg assessment report into a table.
 * @export
 * @param {object} reportData - The JSON data returned from the Ora2Pg report.
 */
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

/**
 * Renders the list of files for a given session in the file browser.
 * @export
 * @param {Array<object>} files - An array of file objects for the session.
 */
export function renderFileBrowser(files) {
    const fileListContainer = document.getElementById('migration-file-list');
    const fileBrowserContainer = document.getElementById('file-browser-container');

    if (!files || files.length === 0) {
        fileListContainer.innerHTML = '<p class="text-gray-500 col-span-full">No SQL files found for this session.</p>';
        fileBrowserContainer.classList.remove('hidden');
        return;
    }

    const statusColors = {
        'generated': 'border-gray-400 dark:border-gray-700',
        'corrected': 'border-blue-500',
        'validated': 'border-green-500',
        'failed': 'border-red-500'
    };

    const fileItemsHtml = files.map(file => `
        <a href="#" data-file-id="${file.file_id}" class="file-item bg-gray-100 dark:bg-gray-800 p-3 rounded-lg text-gray-700 dark:text-gray-300 hover:bg-blue-500 hover:text-white transition-colors duration-200 truncate flex items-center border-l-4 cursor-pointer ${statusColors[file.status] || 'border-gray-400 dark:border-gray-700'}">
            <i class="fas fa-file-alt mr-2 text-gray-500"></i>
            <span class="truncate" title="${file.filename}">${file.filename}</span>
        </a>
    `).join('');
    
    fileListContainer.innerHTML = fileItemsHtml;
    fileBrowserContainer.classList.remove('hidden');
}

/**
 * Renders the session history list for the current client.
 * @export
 */
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
        item.className = 'session-item block bg-gray-100 dark:bg-gray-800 p-3 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700 cursor-pointer border border-gray-200 dark:border-gray-700';
        item.dataset.sessionId = session.session_id;

        if (session.session_id === state.currentSessionId) {
            item.classList.add('ring-2', 'ring-purple-500');
        }

        item.innerHTML = `
            <div class="flex justify-between items-center">
                <span class="font-semibold text-gray-900 dark:text-white">${session.session_name}</span>
                <span class="text-xs bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 px-2 py-1 rounded-full">${session.export_type || 'N/A'}</span>
            </div>
            <div class="text-xs text-gray-500 dark:text-gray-400 mt-1">${new Date(session.created_at).toLocaleString()}</div>
        `;
        listEl.appendChild(item);
    });

    container.classList.remove('hidden');
}

// --- REWRITTEN AND NEW FUNCTIONS FOR OBJECT SELECTOR ---

/**
 * Renders the list of objects for a specific type in the right-hand pane of the master-detail view.
 * @export
 * @param {string} objectType - The type of objects to render (e.g., 'TABLE').
 */
export function renderObjectList(objectType) {
    const listEl = document.getElementById('object-list');
    listEl.innerHTML = '';

    const objectsOfType = state.objectList.filter(obj => obj.type === objectType);

    if (objectsOfType.length === 0) {
        listEl.innerHTML = `<p class="text-gray-500 text-center py-16">No objects of type '${objectType}' found.</p>`;
        return;
    }

    const previouslySelected = state.selectedObjects[objectType] || [];

    objectsOfType.forEach(obj => {
        const isSupported = obj.supported;
        const isChecked = previouslySelected.includes(obj.name);
        const item = document.createElement('div');
        
        const disabledClass = !isSupported ? 'opacity-50 cursor-not-allowed' : '';
        const disabledTooltip = !isSupported ? `Object type '${obj.type}' is not supported for direct export by Ora2Pg.` : '';
        const checkboxDisabled = !isSupported ? 'disabled' : '';

        item.className = `flex items-center justify-between hover:bg-gray-700 rounded ${disabledClass}`;
        if(disabledTooltip) {
            item.setAttribute('title', disabledTooltip);
        }
        
item.innerHTML = `
    <label class="flex items-center space-x-3 p-2 flex-grow ${isSupported ? 'cursor-pointer' : ''}">
        <input name="object" value="${obj.name}" type="checkbox" ${checkboxDisabled} ${isChecked ? 'checked' : ''}
               data-object-type="${obj.type}"
               class="h-4 w-4 rounded border-gray-600 bg-gray-700 text-indigo-600 focus:ring-indigo-500">
        <span class="text-sm font-medium text-gray-300">${obj.name}</span>
    </label>
    <button class="download-ddl-btn text-gray-400 hover:text-white p-2" 
            data-object-name="${obj.name}" 
            data-object-type="${obj.type}" 
            title="Download Original Oracle DDL for ${obj.name}">
        <i class="fas fa-download"></i>
    </button>
`;
        listEl.appendChild(item);
    });
}

/**
 * Renders the collapsible object type tree in the left-hand pane of the master-detail view.
 * Replaces the old renderObjectSelector function.
 * @export
 */
export function renderObjectTypeTree() {
    const container = document.getElementById('object-selector-container');
    const treeEl = document.getElementById('object-type-tree');
    const listEl = document.getElementById('object-list');
    treeEl.innerHTML = ''; 
    state.selectedObjects = {}; // Clear previous selections when re-discovering

    if (!state.objectList || state.objectList.length === 0) {
        listEl.innerHTML = '<p class="text-gray-500 col-span-full">No objects found in the schema.</p>';
        container.classList.remove('hidden');
        return;
    }

    const groupedObjects = state.objectList.reduce((acc, obj) => {
        const type = obj.type;
        if (!acc[type]) {
            acc[type] = { count: 0, supportedCount: 0 };
        }
        acc[type].count++;
        if (obj.supported) {
            acc[type].supportedCount++;
        }
        return acc;
    }, {});

    const sortedTypes = Object.keys(groupedObjects).sort();
    
    // Create root node for the schema
    const schemaName = document.querySelector('#ora2pg-settings-container #schema').value || 'Schema';
    const rootDetails = document.createElement('details');
    rootDetails.open = true;
    
    const rootSummary = document.createElement('summary');
    rootSummary.className = 'text-lg font-bold text-white cursor-pointer';
    rootSummary.innerHTML = `<i class="fas fa-database mr-2"></i> ${schemaName}`;
    rootDetails.appendChild(rootSummary);

    // Create list for types
    const typeList = document.createElement('ul');
    typeList.className = 'ml-4 mt-2 space-y-1';
    
    sortedTypes.forEach(type => {
        const typeData = groupedObjects[type];
        const listItem = document.createElement('li');
        const typeLink = document.createElement('a');
        typeLink.href = '#';
        typeLink.className = 'object-type-link block p-1 rounded hover:bg-gray-700';
        typeLink.dataset.objectType = type;
        
        const supportedBadge = typeData.supportedCount > 0 ? `<span class="text-xs text-green-400 ml-2">(${typeData.supportedCount} supported)</span>` : '';
        
        typeLink.innerHTML = `${type} <span class="text-xs text-gray-500">(${typeData.count})</span> ${supportedBadge}`;
        
        listItem.appendChild(typeLink);
        typeList.appendChild(listItem);
    });
    
    rootDetails.appendChild(typeList);
    treeEl.appendChild(rootDetails);
    
    // Reset the right-hand pane
    listEl.innerHTML = `<p class="text-gray-500 text-center py-16">Select an object type from the tree to see the list of objects.</p>`;
    container.classList.remove('hidden');
}

/**
 * Renders the migration history table.
 * @param {Array} migrations - Array of migration objects from the API
 * @param {Function} onSessionClick - Callback when a session is clicked for details
 */
export function renderMigrationHistory(migrations, onSessionClick) {
    const container = document.getElementById('migration-history-container');
    if (!container) return;

    if (!migrations || migrations.length === 0) {
        container.innerHTML = `
            <div class="text-gray-500 text-center py-8">
                No completed migrations yet.
            </div>`;
        return;
    }

    const formatDate = (dateStr) => {
        if (!dateStr) return '-';
        const date = new Date(dateStr);
        return date.toLocaleString('en-US', {
            month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
        });
    };

    const statusBadge = (status) => {
        const badges = {
            'completed': 'bg-green-600 text-white',
            'partial': 'bg-yellow-600 text-white',
            'failed': 'bg-red-600 text-white'
        };
        return `<span class="px-2 py-0.5 rounded text-xs font-medium ${badges[status] || 'bg-gray-600 text-white'}">${status}</span>`;
    };

    const formatCost = (cost) => {
        if (!cost || cost === 0) return '-';
        return `$${cost.toFixed(4)}`;
    };

    const formatTokens = (input, output) => {
        if (input === 0 && output === 0) return '-';
        return `${(input + output).toLocaleString()}`;
    };

    let html = `
        <div class="overflow-x-auto">
            <table class="w-full text-sm">
                <thead>
                    <tr class="text-left text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700">
                        <th class="py-2 px-2">Date</th>
                        <th class="py-2 px-2">Type</th>
                        <th class="py-2 px-2">Status</th>
                        <th class="py-2 px-2 text-center">Files</th>
                        <th class="py-2 px-2 text-right">Tokens</th>
                        <th class="py-2 px-2 text-right">Est. Cost</th>
                    </tr>
                </thead>
                <tbody>`;

    migrations.forEach(m => {
        html += `
            <tr class="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer migration-history-row"
                data-session-id="${m.session_id}">
                <td class="py-2 px-2 text-gray-700 dark:text-gray-300">${formatDate(m.created_at)}</td>
                <td class="py-2 px-2 text-gray-700 dark:text-gray-300">${m.export_type}</td>
                <td class="py-2 px-2">${statusBadge(m.workflow_status)}</td>
                <td class="py-2 px-2 text-center">
                    <span class="text-green-600 dark:text-green-400">${m.successful_files}</span>/<span class="text-red-600 dark:text-red-400">${m.failed_files}</span>/<span class="text-gray-500 dark:text-gray-400">${m.total_files}</span>
                </td>
                <td class="py-2 px-2 text-right text-gray-700 dark:text-gray-300">${formatTokens(m.total_input_tokens, m.total_output_tokens)}</td>
                <td class="py-2 px-2 text-right text-gray-700 dark:text-gray-300">${formatCost(m.estimated_cost_usd)}</td>
            </tr>`;
    });

    html += `
                </tbody>
            </table>
        </div>
        <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">Click a row to view session details</p>`;

    container.innerHTML = html;

    // Add click handlers
    container.querySelectorAll('.migration-history-row').forEach(row => {
        row.addEventListener('click', () => {
            const sessionId = row.dataset.sessionId;
            if (onSessionClick) {
                onSessionClick(parseInt(sessionId));
            }
        });
    });
}

/**
 * Shows a modal with session details.
 * @param {Object} sessionData - Session data from the API
 * @param {Array} files - File details from the API
 */
export function showSessionDetailsModal(sessionData, files) {
    // Create modal if it doesn't exist
    let modal = document.getElementById('session-details-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'session-details-modal';
        modal.className = 'fixed inset-0 bg-black bg-opacity-50 hidden items-center justify-center z-50';
        modal.innerHTML = `
            <div class="bg-gray-800 rounded-lg shadow-xl max-w-4xl w-full mx-4 max-h-[80vh] overflow-hidden flex flex-col">
                <div class="flex justify-between items-center px-6 py-4 border-b border-gray-700">
                    <h3 id="session-details-title" class="text-lg font-semibold text-white">Session Details</h3>
                    <button id="session-details-close" class="text-gray-400 hover:text-white">&times;</button>
                </div>
                <div id="session-details-content" class="p-6 overflow-y-auto flex-1"></div>
            </div>`;
        document.body.appendChild(modal);

        // Close handlers
        modal.querySelector('#session-details-close').addEventListener('click', () => {
            modal.classList.add('hidden');
            modal.classList.remove('flex');
        });
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.classList.add('hidden');
                modal.classList.remove('flex');
            }
        });
    }

    const content = modal.querySelector('#session-details-content');
    const formatDate = (dateStr) => {
        if (!dateStr) return '-';
        return new Date(dateStr).toLocaleString();
    };

    let configHtml = '';
    if (sessionData.config_snapshot) {
        const config = sessionData.config_snapshot;
        configHtml = `
            <details class="mt-4">
                <summary class="cursor-pointer text-gray-400 hover:text-white">Configuration Snapshot</summary>
                <div class="mt-2 bg-gray-900 rounded p-4 text-xs font-mono overflow-x-auto">
                    <pre>${JSON.stringify(config, null, 2)}</pre>
                </div>
            </details>`;
    }

    let filesHtml = '';
    if (files && files.length > 0) {
        filesHtml = `
            <div class="mt-4">
                <h4 class="text-sm font-medium text-gray-300 mb-2">Files (${files.length})</h4>
                <div class="max-h-64 overflow-y-auto">
                    <table class="w-full text-xs">
                        <thead>
                            <tr class="text-left text-gray-500">
                                <th class="py-1 px-2">Filename</th>
                                <th class="py-1 px-2">Status</th>
                                <th class="py-1 px-2 text-right">AI Calls</th>
                                <th class="py-1 px-2 text-right">Tokens</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${files.map(f => `
                                <tr class="border-t border-gray-800">
                                    <td class="py-1 px-2 truncate max-w-xs" title="${f.filename}">${f.filename}</td>
                                    <td class="py-1 px-2">
                                        <span class="px-1.5 py-0.5 rounded text-xs ${f.status === 'validated' ? 'bg-green-600' : f.status === 'failed' ? 'bg-red-600' : 'bg-gray-600'}">${f.status}</span>
                                    </td>
                                    <td class="py-1 px-2 text-right">${f.ai_attempts || 0}</td>
                                    <td class="py-1 px-2 text-right">${((f.input_tokens || 0) + (f.output_tokens || 0)).toLocaleString()}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            </div>`;
    }

    content.innerHTML = `
        <div class="grid grid-cols-2 gap-4 text-sm">
            <div>
                <span class="text-gray-400">Session Name:</span>
                <span class="text-white ml-2">${sessionData.session_name}</span>
            </div>
            <div>
                <span class="text-gray-400">Export Type:</span>
                <span class="text-white ml-2">${sessionData.export_type}</span>
            </div>
            <div>
                <span class="text-gray-400">Status:</span>
                <span class="ml-2 px-2 py-0.5 rounded text-xs ${sessionData.workflow_status === 'completed' ? 'bg-green-600' : sessionData.workflow_status === 'failed' ? 'bg-red-600' : 'bg-yellow-600'}">${sessionData.workflow_status}</span>
            </div>
            <div>
                <span class="text-gray-400">AI Model:</span>
                <span class="text-white ml-2">${sessionData.ai_model || '-'}</span>
            </div>
            <div>
                <span class="text-gray-400">Started:</span>
                <span class="text-white ml-2">${formatDate(sessionData.created_at)}</span>
            </div>
            <div>
                <span class="text-gray-400">Completed:</span>
                <span class="text-white ml-2">${formatDate(sessionData.completed_at)}</span>
            </div>
            <div>
                <span class="text-gray-400">Input Tokens:</span>
                <span class="text-white ml-2">${(sessionData.total_input_tokens || 0).toLocaleString()}</span>
            </div>
            <div>
                <span class="text-gray-400">Output Tokens:</span>
                <span class="text-white ml-2">${(sessionData.total_output_tokens || 0).toLocaleString()}</span>
            </div>
            <div class="col-span-2">
                <span class="text-gray-400">Estimated Cost:</span>
                <span class="text-white ml-2">$${(sessionData.estimated_cost_usd || 0).toFixed(4)}</span>
            </div>
        </div>
        ${configHtml}
        ${filesHtml}`;

    modal.classList.remove('hidden');
    modal.classList.add('flex');
}

/**
 * Renders the global migration history table (all clients).
 * @param {Array} migrations - Array of migration objects from the API
 * @param {Function} onSessionClick - Callback when a session is clicked for details
 */
export function renderGlobalMigrationHistory(migrations, onSessionClick) {
    const container = document.getElementById('global-migration-history-container');
    if (!container) return;

    if (!migrations || migrations.length === 0) {
        container.innerHTML = `
            <div class="text-gray-500 dark:text-gray-400 text-center py-8">
                <i class="fas fa-history text-4xl mb-3 opacity-50"></i>
                <p>No completed migrations yet.</p>
                <p class="text-sm mt-1">Run a migration to see it here.</p>
            </div>`;
        return;
    }

    const formatDate = (dateStr) => {
        if (!dateStr) return '-';
        const date = new Date(dateStr);
        return date.toLocaleString('en-US', {
            month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
        });
    };

    const statusBadge = (status) => {
        const badges = {
            'completed': 'bg-green-600 text-white',
            'partial': 'bg-yellow-600 text-white',
            'failed': 'bg-red-600 text-white'
        };
        return `<span class="px-2 py-0.5 rounded text-xs font-medium ${badges[status] || 'bg-gray-600 text-white'}">${status}</span>`;
    };

    const formatCost = (cost) => {
        if (!cost || cost === 0) return '-';
        return `$${cost.toFixed(4)}`;
    };

    const formatTokens = (input, output) => {
        if (input === 0 && output === 0) return '-';
        return `${(input + output).toLocaleString()}`;
    };

    let html = `
        <div class="overflow-x-auto">
            <table class="w-full text-sm">
                <thead>
                    <tr class="text-left text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700">
                        <th class="py-2 px-2">Date</th>
                        <th class="py-2 px-2">Client</th>
                        <th class="py-2 px-2">Type</th>
                        <th class="py-2 px-2">Status</th>
                        <th class="py-2 px-2 text-center">Files</th>
                        <th class="py-2 px-2 text-right">Tokens</th>
                        <th class="py-2 px-2 text-right">Est. Cost</th>
                    </tr>
                </thead>
                <tbody>`;

    migrations.forEach(m => {
        html += `
            <tr class="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer global-migration-history-row"
                data-session-id="${m.session_id}" data-client-id="${m.client_id}">
                <td class="py-2 px-2 text-gray-700 dark:text-gray-300">${formatDate(m.created_at)}</td>
                <td class="py-2 px-2 text-gray-700 dark:text-gray-300 truncate max-w-[120px]" title="${m.client_name}">${m.client_name}</td>
                <td class="py-2 px-2 text-gray-700 dark:text-gray-300">${m.export_type}</td>
                <td class="py-2 px-2">${statusBadge(m.workflow_status)}</td>
                <td class="py-2 px-2 text-center">
                    <span class="text-green-600 dark:text-green-400">${m.successful_files}</span>/<span class="text-red-600 dark:text-red-400">${m.failed_files}</span>/<span class="text-gray-500 dark:text-gray-400">${m.total_files}</span>
                </td>
                <td class="py-2 px-2 text-right text-gray-700 dark:text-gray-300">${formatTokens(m.total_input_tokens, m.total_output_tokens)}</td>
                <td class="py-2 px-2 text-right text-gray-700 dark:text-gray-300">${formatCost(m.estimated_cost_usd)}</td>
            </tr>`;
    });

    html += `
                </tbody>
            </table>
        </div>
        <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">Click a row to view session details</p>`;

    container.innerHTML = html;

    // Add click handlers
    container.querySelectorAll('.global-migration-history-row').forEach(row => {
        row.addEventListener('click', () => {
            const sessionId = row.dataset.sessionId;
            if (onSessionClick) {
                onSessionClick(parseInt(sessionId));
            }
        });
    });
}
