// Extract token from URL query parameters
function getAuthToken() {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get('token');
}

// Store token for the session
const authToken = getAuthToken();

export async function apiFetch(url, options = {}) {
    // Add token to the request
    if (authToken) {
        // Add token as query parameter to the URL
        const separator = url.includes('?') ? '&' : '?';
        url = `${url}${separator}token=${authToken}`;
    }
    
    // Add Content-Type header if body exists
    if (options.body && (!options.headers || !options.headers['Content-Type'])) {
        options.headers = {
            ...options.headers,
            'Content-Type': 'application/json'
        };
    }

    try {
        const response = await fetch(url, options);
        if (!response.ok) {
            const errorData = await response.json();
            // Create an error object that preserves all response data
            const error = new Error(errorData.error || errorData.message || `HTTP error! status: ${response.status}`);
            // Copy additional properties from the error response
            Object.assign(error, errorData);
            throw error;
        }
        const contentType = response.headers.get("content-type");
        if (contentType && contentType.indexOf("application/json") !== -1) {
            return response.json();
        }
        return;
    } catch (error) {
        console.error('API Fetch Error:', error);
        throw error; // Re-throw the error for the caller to handle
    }
}

// Migration History API functions
export async function getMigrationHistory(clientId, limit = 5) {
    return apiFetch(`/api/client/${clientId}/migration_history?limit=${limit}`);
}

export async function getSessionDetails(sessionId) {
    return apiFetch(`/api/session/${sessionId}/details`);
}

export async function getAllMigrationHistory(limit = 20) {
    return apiFetch(`/api/migrations/history?limit=${limit}`);
}
