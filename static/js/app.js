import { editors } from './state.js';
import { initializeApp, initEventListeners } from './handlers.js';

// Monaco Editor Loader
const monacoPath = 'https://cdn.jsdelivr.net/npm/monaco-editor@0.33.0/min/vs';
require.config({ paths: { 'vs': monacoPath }});

// --- CHANGE: Use the data URI workaround for the web worker CORS issue ---
window.MonacoEnvironment = {
    getWorkerUrl: function (workerId, label) {
        const workerScriptPath = `${monacoPath}/editor/editor.worker.js`;
        // Create a blob URL to a proxy worker script to bypass CORS issues
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
    initializeApp();
    initEventListeners();
});
