
(function () {
    'use strict';

    // ============================================================
    // AUTHENTICATION GUARD
    // Protects all pages from direct URL access
    // ============================================================

    const LOGIN_PAGE = 'login.html';

    // These are the values created by your login.html
    const username = sessionStorage.getItem('username');
    const userContext = sessionStorage.getItem('userContext');
    const currentUser = sessionStorage.getItem('currentUser');
    const user = sessionStorage.getItem('user');

    // Check whether the user has a valid browser session
    const isAuthenticated =
        username &&
        userContext &&
        currentUser &&
        user;

    if (!isAuthenticated) {
        // No login session → immediately return to login
        window.location.replace(LOGIN_PAGE);
        return;
    }

    // Optional: make the authenticated username available globally
    window.AUTH_USER = username;
})();
