// static/js/auth.js
window.Auth = {
    TOKEN_KEY: 'auth_token',
    getToken() {
        return localStorage.getItem(this.TOKEN_KEY);
    },
    setToken(token) {
        localStorage.setItem(this.TOKEN_KEY, token);
    },
    removeToken() {
        localStorage.removeItem(this.TOKEN_KEY);
    },
    getHeaders() {
        const token = this.getToken();
        return token ? { 'Authorization': 'Bearer ' + token } : {};
    },
    requireAuth() {
        if (!this.getToken()) {
            window.location.href = '/login.html';
            return false;
        }
        return true;
    },
    logout() {
        this.removeToken();
        window.location.href = '/login.html';
    }
};