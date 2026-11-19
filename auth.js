    // ============================================================================
    // DOLE CALABARZON / PESO - CENTRAL AUTHENTICATION MODULE
    // ============================================================================
    // Original/default users remain in users.js.
    // Changed passwords and newly registered user passwords are stored centrally
    // in Google Sheets through Apps Script.
    // No passwords.json file is required by this module.
    // ============================================================================

    const https = require('https');

    const {
      getAllUsersMap,
      normalizeLoginUsername,
      userExists,
      getProvinceForMunicipality
    } = require('./users');

    const AUTH_SHEET_API_URL =
      'https://script.google.com/macros/s/AKfycbx7nM-WiEmGx3duwdkOKTgmgPblXkQ72FjkCEKi9cA3nBM5tgRhKoL-ErqCPHOIgCPD/exec';

    function requestJson(urlString, options, body, redirectsLeft = 5) {
      return new Promise((resolve, reject) => {
        const url = new URL(urlString);

        const req = https.request({
          hostname: url.hostname,
          port: 443,
          path: url.pathname + url.search,
          method: options.method || 'GET',
          headers: options.headers || {}
        }, res => {
          let data = '';

          res.on('data', chunk => {
            data += chunk.toString();
          });

          res.on('end', () => {
            if (
              res.statusCode >= 300 &&
              res.statusCode < 400 &&
              res.headers.location
            ) {
              if (redirectsLeft <= 0) {
                return reject(new Error('Too many Google redirects.'));
              }

              return requestJson(
                res.headers.location,
                {
                  method: 'GET',
                  headers: { Accept: 'application/json' }
                },
                null,
                redirectsLeft - 1
              ).then(resolve).catch(reject);
            }

            if (res.statusCode < 200 || res.statusCode >= 300) {
              return reject(
                new Error(
                  `Authentication database HTTP ${res.statusCode}: ${data}`
                )
              );
            }

            try {
              resolve(JSON.parse(data || '{}'));
            } catch (e) {
              reject(
                new Error('Invalid response from authentication database.')
              );
            }
          });
        });

        req.on('error', reject);

        if (body) {
          req.write(body);
        }

        req.end();
      });
    }

    function postToAuthSheet(payload) {
      const body = JSON.stringify(payload || {});

      return requestJson(
        AUTH_SHEET_API_URL,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(body),
            'Accept': 'application/json'
          }
        },
        body
      );
    }

    async function getSavedUserPassword(cleanUser) {
      const username = normalizeLoginUsername(cleanUser);

      if (!username) {
        return null;
      }

      try {
        const result = await postToAuthSheet({
          action: 'getpassword',
          username
        });

        if (
          result &&
          result.success === true &&
          result.hasPassword &&
          result.password !== undefined &&
          result.password !== null &&
          String(result.password) !== ''
        ) {
          return String(result.password);
        }

        return null;
      } catch (error) {
        console.error(
          'Error reading saved password from Google Sheet:',
          error.message
        );

        return null;
      }
    }

    function getDefaultUserPassword(cleanUser) {
      const userKey = normalizeLoginUsername(cleanUser);

      if (!userKey) {
        return null;
      }

      if (userKey === 'DOLE4A') {
        return 'dole4a@2026';
      }

      const usersMap = getAllUsersMap();

      return usersMap[userKey] || null;
    }

    async function getUserPassword(cleanUser) {
      const userKey = normalizeLoginUsername(cleanUser);

      if (!userKey) {
        return null;
      }

      const savedPassword = await getSavedUserPassword(userKey);

      if (
        savedPassword !== null &&
        savedPassword !== ''
      ) {
        return savedPassword;
      }

      return getDefaultUserPassword(userKey);
    }

    async function accountExists(cleanUser) {
      const username = normalizeLoginUsername(cleanUser);

      if (!username) {
        return false;
      }

      if (userExists(username)) {
        return true;
      }

      const savedPassword = await getSavedUserPassword(username);

      return (
        savedPassword !== null &&
        savedPassword !== ''
      );
    }

    function buildAuthenticatedUser(cleanUser) {
      const username = normalizeLoginUsername(cleanUser);

      if (
        username === 'DOLE4A' ||
        username === 'ADMIN'
      ) {
        return {
          success: true,
          role: 'admin',
          province: 'ALL',
          municipality: 'Administrator',
          username,
          user: {
            username,
            name: username,
            role: 'admin',
            province: 'ALL',
            municipality: 'Administrator'
          }
        };
      }

      const province =
        getProvinceForMunicipality(username) || 'ALL';

      return {
        success: true,
        role: 'user',
        province,
        municipality: username,
        username,
        user: {
          username,
          name: username,
          role: 'user',
          province,
          municipality: username
        }
      };
    }

    async function authenticateUser(username, password) {
      try {
        const cleanUser =
          normalizeLoginUsername(username);

        const cleanPass =
          String(password || '');

        if (!cleanUser || !cleanPass) {
          return {
            success: false,
            message:
              'Invalid Username or Password. Please try again.'
          };
        }

        // First check the central Google Sheet.
        // This allows newly registered users to log in even when they are not
        // hard-coded in users.js.
        const savedPassword =
          await getSavedUserPassword(cleanUser);

        if (
          savedPassword !== null &&
          savedPassword !== ''
        ) {
          if (String(savedPassword) !== cleanPass) {
            return {
              success: false,
              message:
                'Invalid Username or Password. Please try again.'
            };
          }

          return buildAuthenticatedUser(cleanUser);
        }

        // No central password exists. Fall back to original users.js accounts.
        if (!userExists(cleanUser)) {
          return {
            success: false,
            message:
              'Invalid Username or Password. Please try again.'
          };
        }

        const defaultPassword =
          getDefaultUserPassword(cleanUser);

        if (
          !defaultPassword ||
          String(defaultPassword) !== cleanPass
        ) {
          return {
            success: false,
            message:
              'Invalid Username or Password. Please try again.'
          };
        }

        return buildAuthenticatedUser(cleanUser);

      } catch (error) {
        console.error('Authentication error:', error);

        return {
          success: false,
          message:
            'Unable to authenticate user. Please try again.'
        };
      }
    }

    async function savePasswordToGoogleSheet(username, password) {
      try {
        const cleanUser =
          normalizeLoginUsername(username);

        const cleanPassword =
          String(password || '');

        if (!cleanUser) {
          return {
            success: false,
            message: 'Username is required.'
          };
        }

        if (!cleanPassword) {
          return {
            success: false,
            message: 'Password is required.'
          };
        }

        const result = await postToAuthSheet({
          action: 'setpassword',
          username: cleanUser,
          password: cleanPassword
        });

        if (!result || result.success !== true) {
          return {
            success: false,
            message:
              result && result.message
                ? result.message
                : 'Unable to save password.'
          };
        }

        return {
          success: true,
          message:
            result.message ||
            'Password saved successfully.'
        };

      } catch (error) {
        console.error(
          'Google Sheet password save error:',
          error.message
        );

        return {
          success: false,
          message:
            'Unable to save password to the central database.'
        };
      }
    }

    async function changeUserPassword(
      username,
      currentPassword,
      newPassword
    ) {
      try {
        const cleanUser =
          normalizeLoginUsername(username);

        const cleanCurrentPassword =
          String(currentPassword || '').trim();

        const cleanNewPassword =
          String(newPassword || '').trim();

        if (!cleanUser) {
          return {
            success: false,
            message: 'Please enter your username.'
          };
        }

        if (!cleanCurrentPassword) {
          return {
            success: false,
            message:
              'Please enter your current password.'
          };
        }

        if (!cleanNewPassword) {
          return {
            success: false,
            message:
              'Please enter a new password.'
          };
        }

        if (cleanNewPassword.length < 6) {
          return {
            success: false,
            message:
              'Your new password must be at least 6 characters.'
          };
        }

        const exists =
          await accountExists(cleanUser);

        if (!exists) {
          return {
            success: false,
            message: 'Username not found.'
          };
        }

        const correctPassword =
          await getUserPassword(cleanUser);

        if (
          !correctPassword ||
          cleanCurrentPassword !== String(correctPassword)
        ) {
          return {
            success: false,
            message:
              'Your current password is incorrect.'
          };
        }

        if (
          cleanNewPassword ===
          String(correctPassword)
        ) {
          return {
            success: false,
            message:
              'Your new password must be different from your current password.'
          };
        }

        const saved =
          await savePasswordToGoogleSheet(
            cleanUser,
            cleanNewPassword
          );

        if (!saved.success) {
          return saved;
        }

        return {
          success: true,
          message:
            'Your password has been changed successfully.'
        };

      } catch (error) {
        console.error('Change password error:', error);

        return {
          success: false,
          message:
            'Unable to change password: ' +
            error.message
        };
      }
    }

    async function resetUserPassword(username) {
      try {
        const cleanUser =
          normalizeLoginUsername(username);

        if (!cleanUser) {
          return {
            success: false,
            message: 'Username is required.'
          };
        }

        const exists =
          await accountExists(cleanUser);

        if (!exists) {
          return {
            success: false,
            message: 'Username not found.'
          };
        }

        const isOriginalUser =
          cleanUser === 'DOLE4A' ||
          cleanUser === 'ADMIN' ||
          userExists(cleanUser);

        if (!isOriginalUser) {
          return {
            success: false,
            message:
              'This registered account has no users.js default password. Set a new password instead.'
          };
        }

        const result = await postToAuthSheet({
          action: 'deletepassword',
          username: cleanUser
        });

        if (!result || result.success !== true) {
          return {
            success: false,
            message:
              result && result.message
                ? result.message
                : 'Unable to reset password.'
          };
        }

        return {
          success: true,
          message: 'Password reset successfully.'
        };

      } catch (error) {
        console.error('Reset password error:', error);

        return {
          success: false,
          message:
            'Unable to reset password: ' +
            error.message
        };
      }
    }

    async function setUserPassword(username, newPassword) {
      try {
        const cleanUser =
          normalizeLoginUsername(username);

        const cleanNewPassword =
          String(newPassword || '').trim();

        if (!cleanUser) {
          return {
            success: false,
            message: 'Username is required.'
          };
        }

        if (
          !cleanNewPassword ||
          cleanNewPassword.length < 6
        ) {
          return {
            success: false,
            message:
              'Password must be at least 6 characters.'
          };
        }

        const exists =
          await accountExists(cleanUser);

        if (!exists) {
          return {
            success: false,
            message: 'Username not found.'
          };
        }

        return await savePasswordToGoogleSheet(
          cleanUser,
          cleanNewPassword
        );

      } catch (error) {
        console.error('Set password error:', error);

        return {
          success: false,
          message:
            'Unable to save password: ' +
            error.message
        };
      }
    }

    module.exports = {
      getUserPassword,
      getSavedUserPassword,
      authenticateUser,
      changeUserPassword,
      resetUserPassword,
      setUserPassword,
      savePasswordToGoogleSheet
    };
