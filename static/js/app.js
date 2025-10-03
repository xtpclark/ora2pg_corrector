import { state, editors, dom } from './state.js';
import { initializeApp, initEventListeners } from './handlers.js';

// CodeMirror 6 imports - using esm.sh for proper dependency resolution
import { EditorView, basicSetup } from "https://esm.sh/codemirror@6.0.1";
import { sql, PostgreSQL } from "https://esm.sh/@codemirror/lang-sql@6.8.0";
import { oneDark } from "https://esm.sh/@codemirror/theme-one-dark@6.1.2";

// Light theme for CodeMirror
const lightTheme = EditorView.theme({
    "&": {
        backgroundColor: "#ffffff",
        color: "#374151"
    },
    ".cm-content": {
        caretColor: "#374151"
    },
    ".cm-cursor, .cm-dropCursor": {
        borderLeftColor: "#374151"
    },
    ".cm-selectionBackground, .cm-focused .cm-selectionBackground": {
        backgroundColor: "#bfdbfe"
    },
    ".cm-activeLine": {
        backgroundColor: "#f3f4f6"
    },
    ".cm-gutters": {
        backgroundColor: "#f9fafb",
        color: "#6b7280",
        borderRight: "1px solid #e5e7eb"
    },
    ".cm-activeLineGutter": {
        backgroundColor: "#f3f4f6"
    }
}, { dark: false });

// Dark theme customization
const darkTheme = EditorView.theme({
    "&": {
        fontSize: "14px",
        height: "100%"
    },
    ".cm-content": {
        padding: "12px",
        minHeight: "100%"
    },
    ".cm-scroller": {
        fontFamily: "'Consolas', 'Monaco', 'Courier New', monospace"
    },
    ".cm-gutters": {
        backgroundColor: "#1a1a1a",
        borderRight: "1px solid #374151"
    },
    ".cm-editor.cm-focused": {
        outline: "2px solid #8b5cf6"
    }
});

// Common theme
const commonTheme = EditorView.theme({
    "&": {
        fontSize: "14px",
        height: "100%"
    },
    ".cm-content": {
        padding: "12px",
        minHeight: "100%"
    },
    ".cm-scroller": {
        fontFamily: "'Consolas', 'Monaco', 'Courier New', monospace"
    },
    ".cm-editor.cm-focused": {
        outline: "2px solid #8b5cf6"
    }
});

/**
 * Gets the current theme (light or dark) from the document
 */
function getCurrentTheme() {
    return document.documentElement.classList.contains('dark') ? 'dark' : 'light';
}

/**
 * Creates the appropriate theme extensions based on current theme
 */
function getThemeExtensions() {
    const isDark = getCurrentTheme() === 'dark';
    return isDark ? [oneDark, darkTheme, commonTheme] : [lightTheme, commonTheme];
}

/**
 * Reconfigures an editor with a new theme
 */
function updateEditorTheme(editor) {
    if (!editor) return;
    
    const themeExtensions = getThemeExtensions();
    editor.dispatch({
        effects: EditorView.reconfigure.of([
            basicSetup,
            sql({ 
                dialect: PostgreSQL,
                upperCaseKeywords: true
            }),
            ...themeExtensions,
            EditorView.lineWrapping
        ])
    });
}

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
        const themeExtensions = getThemeExtensions();
        
        // Create original SQL editor (source input)
        const originalEditor = new EditorView({
            doc: '-- Paste or load your source SQL here...',
            extensions: [
                basicSetup,
                sql({ 
                    dialect: PostgreSQL,
                    upperCaseKeywords: true
                }),
                ...themeExtensions,
                EditorView.lineWrapping
            ],
            parent: originalEditorEl
        });
        
        // Create corrected SQL editor (PostgreSQL target)
        const correctedEditor = new EditorView({
            doc: '-- PostgreSQL-converted SQL will appear here...',
            extensions: [
                basicSetup,
                sql({ 
                    dialect: PostgreSQL,
                    upperCaseKeywords: true
                }),
                ...themeExtensions,
                EditorView.lineWrapping
            ],
            parent: correctedEditorEl
        });
        
        // Add compatibility methods for existing code
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
        
        // Listen for theme changes
        window.addEventListener('themeChanged', () => {
            updateEditorTheme(editors.original);
            updateEditorTheme(editors.corrected);
        });
        
        // Handle window resize
        window.addEventListener('resize', () => {
            originalEditor.requestMeasure();
            correctedEditor.requestMeasure();
        });
        
        console.log('CodeMirror editors initialized with', getCurrentTheme(), 'theme');
        return true;
        
    } catch (error) {
        console.error('Failed to initialize CodeMirror editors:', error);
        return false;
    }
}

// App Logic
document.addEventListener('DOMContentLoaded', () => {
    console.log('Initializing Ora2Pg AI Corrector...');
    
    // Populate the dom object with references
    dom.mainContentEl = document.getElementById('main-content');
    dom.welcomeMessageEl = document.getElementById('welcome-message');
    dom.clientNameHeaderEl = document.getElementById('client-name-header');
    dom.tabsEl = document.getElementById('main-tabs');
    dom.settingsForm = document.getElementById('settings-form');
    dom.filePicker = document.getElementById('sql-file-picker');
    
    // Initialize editors with current theme
    const editorsInitialized = initializeEditors();
    
    if (!editorsInitialized) {
        console.error('Failed to initialize editors, some features may not work');
    }
    
    // Initialize app and event listeners
    initializeApp();
    initEventListeners();
});
