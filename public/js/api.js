// api.js — helper HTTP para BoostSpeed
const API = {
  _headers(token) {
    const h = { 'Content-Type': 'application/json' };
    if (token) h['Authorization'] = 'Bearer ' + token;
    return h;
  },
  _token() { return localStorage.getItem('bs_token') || ''; },

  async _req(method, url, body, token) {
    try {
      const opts = { method, headers: this._headers(token || this._token()) };
      if (body) opts.body = JSON.stringify(body);
      const r = await fetch(url, opts);
      const d = await r.json();
      if (!d.ok && !d.error) d.error = 'Error desconocido';
      return d;
    } catch (e) {
      return { ok: false, error: 'Error de conexión: ' + e.message };
    }
  },

  get(url)                          { return this._req('GET',    url, null, null); },
  post(url, body)                   { return this._req('POST',   url, body, null); },
  put(url, body)                    { return this._req('PUT',    url, body, null); },
  delete(url)                       { return this._req('DELETE', url, null, null); },

  getAdmin(url, tok)                { return this._req('GET',    url, null, tok); },
  postAdmin(url, body, tok)         { return this._req('POST',   url, body, tok); },
  putAdmin(url, body, tok)          { return this._req('PUT',    url, body, tok); },
  deleteAdmin(url, tok)             { return this._req('DELETE', url, null, tok); },
};
