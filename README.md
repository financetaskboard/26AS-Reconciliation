# 26AS TDS Reconciliation Portal — Web Version

Web-based 26AS reconciliation portal with Odoo ERP sync, Gmail integration, and team sync via Firebase.

Converted from Electron desktop app to web app deployable on Render.com.

---

## Architecture

```
Browser (React app)
  ├── localStorage (instant cache)
  ├── Push/Pull ↔ Firebase Firestore (team sync)
  └── API calls → Express Server
                    ├── /api/odoo/*     → Odoo XML-RPC proxy
                    ├── /api/gmail/*    → Gmail OAuth + API proxy
                    └── /api/state/*    → Firebase state persistence
```

---

## Quick Start (Local)

### 1. Install dependencies
```bash
npm install
```

### 2. Set up Firebase (optional for local dev)
- Create a Firebase project at https://console.firebase.google.com
- Download service account key → save as `serviceAccountKey.json` in project root
- Or skip Firebase for local-only mode (data stays in localStorage)

### 3. Build the React app
```bash
npm run build
```

### 4. Start the server
```bash
npm start
```

### 5. Open
Go to → **http://localhost:3003**

---

## Deploy to Render.com

### 1. Push to GitHub
```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/tds-recon-portal.git
git push -u origin main
```

### 2. Create Render Web Service
- Go to https://dashboard.render.com → New → Web Service
- Connect your GitHub repo
- **Build Command:** `npm install && npm run build`
- **Start Command:** `npm start`

### 3. Set Environment Variables in Render
| Variable | Value |
|---|---|
| `FIREBASE_SERVICE_ACCOUNT` | Paste entire `serviceAccountKey.json` content |
| `GMAIL_CLIENT_ID` | Your Google OAuth client ID (optional) |
| `GMAIL_CLIENT_SECRET` | Your Google OAuth client secret (optional) |

---

## Project Structure

```
tds-recon-portal/
├── tds-server.js          # Express server (Odoo proxy + Gmail + Firebase)
├── package.json           # Dependencies
├── vite.config.js         # Vite build config
├── serviceAccountKey.json # Firebase credentials (gitignored)
├── src/
│   ├── index.html         # HTML entry point
│   ├── index.js           # React entry
│   ├── App.jsx            # Main React app (from Electron version)
│   ├── odooAPI.js         # Odoo XML-RPC helpers (client-side)
│   └── webAdapters.js     # Storage, Odoo proxy, Gmail, file helpers
└── dist/                  # Built React app (auto-generated)
```

---

## Migration from Electron

The web version replaces Electron APIs with web equivalents:

| Electron API | Web Replacement |
|---|---|
| `electron-store` | localStorage + Firebase sync |
| `ipcMain.handle('odoo-sync-tds')` | `POST /api/odoo/sync-tds` (server proxy) |
| `BrowserWindow` (TRACES) | Not available on web (manual upload instead) |
| `google-oauth-start` | `/api/gmail/auth-url` + popup window |
| `openFileDialog` | `<input type="file">` |
| `saveFile` | `downloadFile()` helper |

### What to change in App.jsx:

1. **Replace storage helpers:**
```js
// OLD (Electron)
import { saveToStore, loadFromStore } from './electronStore';

// NEW (Web)
import { saveToStore, loadFromStore, pushToServer, pullFromServer } from './webAdapters';
```

2. **Replace Odoo sync:**
```js
// OLD (Electron — direct XML-RPC)
import { syncTDSFromOdoo } from './odooAPI';

// NEW (Web — via server proxy)
import { syncTDSFromOdoo } from './webAdapters';
```

3. **Replace file dialogs:**
```js
// OLD
const result = await window.electronAPI.openFileDialog(['txt','csv','xlsx']);

// NEW — use <input type="file"> in JSX
<input type="file" accept=".txt,.csv,.xlsx,.zip" onChange={handleFileUpload} />
```

4. **Remove TRACES portal** — users upload 26AS files manually (download from TRACES → upload here)

5. **Add team sync buttons** — Push to Server / Pull from Server in settings

---

## API Endpoints

| Method | URL | Description |
|---|---|---|
| `GET` | `/api/state` | Load all saved state from Firebase |
| `POST` | `/api/state/:key` | Save a single state key |
| `DELETE` | `/api/state` | Wipe all state |
| `POST` | `/api/odoo/test` | Test Odoo connection |
| `POST` | `/api/odoo/sync-tds` | Full TDS sync from Odoo |
| `POST` | `/api/odoo/xmlrpc` | Generic Odoo XML-RPC proxy |
| `POST` | `/api/gmail/auth-url` | Get Gmail OAuth URL |
| `POST` | `/api/gmail/exchange` | Exchange OAuth code for tokens |
| `POST` | `/api/gmail/refresh` | Refresh Gmail access token |
| `POST` | `/api/gmail/api` | Proxy any Gmail API call |
| `GET` | `/api/gmail/callback` | OAuth callback (popup closes) |
| `GET` | `/health` | Health check |

---

## Gmail Setup

1. Go to https://console.cloud.google.com
2. Create OAuth 2.0 credentials (Web Application type)
3. Add authorized redirect URI: `https://YOUR-APP.onrender.com/api/gmail/callback`
4. Copy Client ID and Client Secret
5. In the portal settings, enter these credentials
6. Click "Connect Gmail" → sign in via popup → done

---

## Team Sync

Same pattern as GST Portal:
- **Push to Server**: Saves all localStorage data to Firebase
- **Pull from Server**: Loads latest data from Firebase (server-newer wins)
- **Auto-sync**: Polls every 30 seconds for changes
- **Backup/Restore**: Export/import JSON files for offline transfer
