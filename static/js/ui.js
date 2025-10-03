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

    // A list of key configuration items we want to display in the sidebar
    const keyConfigs = [
        { key: 'oracle_dsn', label: 'Oracle DSN' },
        { key: 'schema', label: 'Oracle Schema' },
        { key: 'validation_pg_dsn', label: 'Validation DSN' },
        { key: 'ai_provider', label: 'AI Provider' },
    ];

    let content = `
        <h3 class="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-3">Active Configuration</h3>
        <div class="space-y-3 bg-gray-800 p-3 rounded-md text-xs">
    `;

    keyConfigs.forEach(item => {
        const value = config[item.key] || 'Not set';
        content += `
            <div>
                <label class="block font-medium text-gray-500">${item.label}</label>
                <p class="text-gray-300 truncate" title="${value}">${value}</p>
            </div>
        `;
    });

    content += `
        </div>
        <button data-tab="settings" class="tab-button mt-4 text-sm w-full text-center text-blue-400 hover:underline">
            Edit Full Settings...
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
    console.log("Checkpoint 5: renderSettingsForms() has started.");
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
        if (option.option_name.toUpperCase() === 'TYPE') {
            return; // Skip this iteration
        }
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
    <div class="flex items-center space-x-2">
        <label class="flex items-center text-xs text-gray-400" title="Download formatted DDL without storage clauses">
            <input type="checkbox" class="ddl-pretty-checkbox mr-1" data-object-name="${obj.name}">
            Pretty
        </label>
        <button class="download-ddl-btn text-gray-400 hover:text-white p-2" 
                data-object-name="${obj.name}" 
                data-object-type="${obj.type}" 
                title="Download Original Oracle DDL for ${obj.name}">
            <i class="fas fa-download"></i>
        </button>
    </div>
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
