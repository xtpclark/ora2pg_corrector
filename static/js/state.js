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
 * Holds references to key DOM elements. This object is populated in app.js after the DOM is loaded.
 */
export const dom = {};
