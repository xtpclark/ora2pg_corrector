export let state = {
    currentClientId: null,
    clients: [],
    aiProviders: [],
    ora2pgOptions: [],
    appSettings: {},
    currentReportData: null
};

export let editors = {
    original: null,
    corrected: null
};

export const dom = {
    clientListEl: document.getElementById('client-list'),
    mainContentEl: document.getElementById('main-content'),
    welcomeMessageEl: document.getElementById('welcome-message'),
    clientNameHeaderEl: document.getElementById('client-name-header'),
    tabsEl: document.getElementById('main-tabs'),
    settingsForm: document.getElementById('settings-form'),
    filePicker: document.getElementById('sql-file-picker')
};
