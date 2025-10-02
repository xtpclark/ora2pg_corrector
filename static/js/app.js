import { state, editors, dom } from './state.js';
import { initializeApp, initEventListeners } from './handlers.js';

// This is for debugging. You can remove it once the app is working.
window.state = state; 

// Monaco Editor Loader
const monacoPath = 'https://cdn.jsdelivr.net/npm/monaco-editor@0.33.0/min/vs';
require.config({ paths: { 'vs': monacoPath }});

window.MonacoEnvironment = {
    getWorkerUrl: function (workerId, label) {
        const workerScriptPath = `${monacoPath}/editor/editor.worker.js`;
        const proxyWorkerBlob = new Blob([`
            self.MonacoEnvironment = {
                baseUrl: 'https://cdn.jsdelivr.net/npm/monaco-editor@0.33.0/min/'
            };
            importScripts('${workerScriptPath}');
        `], { type: 'application/javascript' });
        return URL.createObjectURL(proxyWorkerBlob);
    }
};

require(['vs/editor/editor.main'], function() {
    editors.original = monaco.editor.create(document.getElementById('original-editor'), {
        value: '-- Oracle SQL will appear here...',
        language: 'sql',
        theme: 'vs-dark',
        readOnly: false,
        automaticLayout: true
    });
    editors.corrected = monaco.editor.create(document.getElementById('corrected-editor'), {
        value: '-- AI-corrected PostgreSQL will appear here...',
        language: 'sql',
        theme: 'vs-dark',
        automaticLayout: true
    });
});

// App Logic
document.addEventListener('DOMContentLoaded', () => {
    // Populate the dom object now that the DOM is ready
    dom.mainContentEl = document.getElementById('main-content');
    dom.welcomeMessageEl = document.getElementById('welcome-message');
    dom.clientNameHeaderEl = document.getElementById('client-name-header');
    dom.tabsEl = document.getElementById('main-tabs');
    dom.settingsForm = document.getElementById('settings-form');
    dom.filePicker = document.getElementById('sql-file-picker');
    
    // Now that dom is populated, initialize the app
    initializeApp();
    initEventListeners();
});
