/**
 * @file Central state management for the application.
 */

/**
 * The global state object.
 */
export let state = {
    currentClientId: null,
    clients: [],
    aiProviders: [],
    ora2pgOptions: [],
    appSettings: {},
    currentReportData: null,
    objectList: [],
    sessions: [],
    currentSessionId: null,
    currentFileId: null,
    selectedObjects: {}
};

/**
 * Holds references to the Monaco editor instances.
 */
export let editors = {
    original: null,
    corrected: null
};

/**
 * Holds references to key DOM elements.
 */

export const dom = {};

/* 
export const dom = {
    // clientListEl is removed as the element no longer exists
    mainContentEl: document.getElementById('main-content'),
    welcomeMessageEl: document.getElementById('welcome-message'),
    clientNameHeaderEl: document.getElementById('client-name-header'),
    tabsEl: document.getElementById('main-tabs'),
    settingsForm: document.getElementById('settings-form'),
    filePicker: document.getElementById('sql-file-picker')
};
*/
