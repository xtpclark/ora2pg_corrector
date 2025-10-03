import { state, editors, dom } from './state.js';
import { initializeApp, initEventListeners } from './handlers.js';

// CodeMirror 6 imports - using esm.sh for proper dependency resolution
import { EditorView, basicSetup } from "https://esm.sh/codemirror@6.0.1";
import { sql, PostgreSQL } from "https://esm.sh/@codemirror/lang-sql@6.8.0";
import { oneDark } from "https://esm.sh/@codemirror/theme-one-dark@6.1.2";

// Custom theme to match your dark UI
const customTheme = EditorView.theme({
    "&": {
        fontSize: "14px",
        height: "100%"
    },
    ".cm-content": {
        padding: "12px",
        minHeight: "100%"
    },
    ".cm-editor": {
        height: "100%"
    },
    ".cm-editor.cm-focused": {
        outline: "2px solid #4299e1"
    },
    ".cm-scroller": {
        fontFamily: "'Consolas', 'Monaco', 'Courier New', monospace",
        scrollbarWidth: "thin"
    },
    ".cm-gutters": {
        backgroundColor: "#1a1a1a",
        borderRight: "1px solid #2d3748"
    }
});

// Initialize CodeMirror editors
function initializeEditors() {
    const originalEditorEl = document.getElementById('original-editor');
    const correctedEditorEl = document.getElementById('corrected-editor');
    
    if (!originalEditorEl || !correctedEditorEl) {
        console.error('Editor containers not found in DOM');
        return false;
    }
    
    // Clear containers in case of re-initialization
    originalEditorEl.innerHTML = '';
    correctedEditorEl.innerHTML = '';
    
    try {
        // Create original SQL editor (Oracle SQL input)
        const originalEditor = new EditorView({
            doc: '-- Oracle SQL will appear here...',
            extensions: [
                basicSetup,
                sql({ 
                    dialect: PostgreSQL,  // Using PostgreSQL for better SQL support
                    upperCaseKeywords: true
                }),
                oneDark,
                customTheme,
                EditorView.lineWrapping,
                EditorView.updateListener.of((update) => {
                    if (update.docChanged) {
                        // Could add auto-save or dirty state tracking here
                    }
                })
            ],
            parent: originalEditorEl
        });
        
        // Create corrected SQL editor (PostgreSQL output)
        const correctedEditor = new EditorView({
            doc: '-- AI-corrected PostgreSQL will appear here...',
            extensions: [
                basicSetup,
                sql({ 
                    dialect: PostgreSQL,
                    upperCaseKeywords: true
                }),
                oneDark,
                customTheme,
                EditorView.lineWrapping,
                EditorView.updateListener.of((update) => {
                    if (update.docChanged) {
                        // Could add dirty state tracking here
                    }
                })
            ],
            parent: correctedEditorEl
        });
        
        // Add compatibility methods for existing code that expects Monaco-like API
        originalEditor.getValue = function() {
            return this.state.doc.toString();
        };
        originalEditor.setValue = function(value) {
            this.dispatch({
                changes: {
                    from: 0,
                    to: this.state.doc.length,
                    insert: value || ''
                }
            });
        };
        
        correctedEditor.getValue = function() {
            return this.state.doc.toString();
        };
        correctedEditor.setValue = function(value) {
            this.dispatch({
                changes: {
                    from: 0,
                    to: this.state.doc.length,
                    insert: value || ''
                }
            });
        };
        
        // Store editors in the global editors object
        editors.original = originalEditor;
        editors.corrected = correctedEditor;
        
        // Handle window resize
        window.addEventListener('resize', () => {
            originalEditor.requestMeasure();
            correctedEditor.requestMeasure();
        });
        
        // Fire custom event to signal editors are ready
        document.dispatchEvent(new CustomEvent('editorsReady'));
        
        console.log('CodeMirror editors initialized successfully');
        return true;
        
    } catch (error) {
        console.error('Failed to initialize CodeMirror editors:', error);
        return false;
    }
}

// App Logic
document.addEventListener('DOMContentLoaded', () => {
    // Populate the dom object with references
    dom.mainContentEl = document.getElementById('main-content');
    dom.welcomeMessageEl = document.getElementById('welcome-message');
    dom.clientNameHeaderEl = document.getElementById('client-name-header');
    dom.tabsEl = document.getElementById('main-tabs');
    dom.settingsForm = document.getElementById('settings-form');
    dom.filePicker = document.getElementById('sql-file-picker');
    
    // Initialize editors synchronously
    const editorsInitialized = initializeEditors();
    
    if (!editorsInitialized) {
        console.error('Failed to initialize editors, some features may not work');
    }
    
    // Initialize app and event listeners
    initializeApp();
    initEventListeners();
});
