require('dotenv').config();
const express = require('express');
const cors    = require('cors');
const bcrypt  = require('bcryptjs');
const jwt     = require('jsonwebtoken');
const https   = require('https');
const { Pool } = require('pg');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Base de datos (optimizado para 100k+ usuarios) ────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 30,
  min: 2,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 8000,
  acquireTimeoutMillis: 8000,
});

pool.on('error', (err) => {
  console.error('PostgreSQL pool error:', err.message);
});

app.use(cors());
app.use(express.json({ limit: '50kb' }));
app.use(express.static(path.join(__dirname, '../public')));

// ── Rate limiting en memoria ──────────────────────────────────
const rateLimitMap = new Map();
const RATE_WINDOW  = 60 * 1000;

function rateLimit(max) {
  return (req, res, next) => {
    const ip  = req.ip || req.connection.remoteAddress || 'unknown';
    const now = Date.now();
    let entry = rateLimitMap.get(ip);
    if (!entry || now > entry.resetAt) {
      entry = { count: 0, resetAt: now + RATE_WINDOW };
      rateLimitMap.set(ip, entry);
    }
    entry.count++;
    if (entry.count > max) {
      return res.status(429).json({ error: 'Demasiadas solicitudes. Intenta en un momento.' });
    }
    next();
  };
}

setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of rateLimitMap.entries()) {
    if (now > entry.resetAt) rateLimitMap.delete(ip);
  }
}, 5 * 60 * 1000);

// ── Cooldowns ─────────────────────────────────────────────────
const cooldowns = new Map();

// ── Último key_usage reportado por la API de FF ───────────────
let lastKeyUsage = null;

function getCooldown(uid) {
  const exp = cooldowns.get(uid);
  if (!exp) return 0;
  if (Date.now() > exp) { cooldowns.delete(uid); return 0; }
  return exp;
}
function setCooldown(uid, ms) {
  cooldowns.set(uid, Date.now() + ms);
}

setInterval(() => {
  const now = Date.now();
  for (const [uid, exp] of cooldowns.entries()) {
    if (now > exp) cooldowns.delete(uid);
  }
}, 10 * 60 * 1000);

// ── Crear tablas ──────────────────────────────────────────────
async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS usuarios (
        id                  SERIAL PRIMARY KEY,
        uid                 VARCHAR(20)  UNIQUE NOT NULL,
        username            VARCHAR(50)  UNIQUE NOT NULL,
        password            TEXT         NOT NULL,
        contact             VARCHAR(100) NOT NULL,
        plan_activo         BOOLEAN      DEFAULT false,
        plan_nombre         VARCHAR(100) DEFAULT 'Sin plan',
        plan_tipo           VARCHAR(20)  DEFAULT 'dias',
        likes_disponibles   INTEGER      DEFAULT 0,
        likes_limite_plan   INTEGER      DEFAULT 0,
        likes_enviados_plan INTEGER      DEFAULT 0,
        envios_por_dia      INTEGER      DEFAULT 0,
        envios_hoy          INTEGER      DEFAULT 0,
        fecha_ultimo_envio  DATE,
        plan_vence          TIMESTAMP,
        ilimitado           BOOLEAN      DEFAULT false,
        creado_en           TIMESTAMP    DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS codigos (
        id          SERIAL PRIMARY KEY,
        codigo      VARCHAR(20) UNIQUE NOT NULL,
        tipo        VARCHAR(20)  DEFAULT 'dias',
        dias        INTEGER NOT NULL DEFAULT 0,
        likes       INTEGER NOT NULL DEFAULT 0,
        envios_dia  INTEGER NOT NULL,
        ilimitado   BOOLEAN   DEFAULT false,
        usado       BOOLEAN   DEFAULT false,
        usado_por   VARCHAR(20),
        usado_en    TIMESTAMP,
        creado_en   TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS historial (
        id              SERIAL PRIMARY KEY,
        usuario_id      INTEGER REFERENCES usuarios(id) ON DELETE CASCADE,
        ff_uid          VARCHAR(30) NOT NULL,
        player_name     VARCHAR(100),
        likes_antes     INTEGER,
        likes_despues   INTEGER,
        likes_agregados INTEGER,
        nivel           INTEGER,
        region          VARCHAR(10),
        fecha           TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS codigos_recuperacion (
        id          SERIAL PRIMARY KEY,
        usuario_id  INTEGER REFERENCES usuarios(id) ON DELETE CASCADE,
        codigo      VARCHAR(20) UNIQUE NOT NULL,
        usado       BOOLEAN DEFAULT false,
        creado_en   TIMESTAMP DEFAULT NOW(),
        expira_en   TIMESTAMP DEFAULT NOW() + INTERVAL '48 hours'
      );
      CREATE TABLE IF NOT EXISTS notificaciones_likes (
        id              SERIAL PRIMARY KEY,
        username        VARCHAR(50) NOT NULL,
        ff_uid          VARCHAR(30),
        player_name     VARCHAR(100),
        likes_agregados INTEGER,
        creado_en       TIMESTAMP DEFAULT NOW()
      );
    `);

    await client.query(`
      ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS plan_tipo VARCHAR(20) DEFAULT 'dias';
      ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS likes_limite_plan INTEGER DEFAULT 0;
      ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS likes_enviados_plan INTEGER DEFAULT 0;
      ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS ilimitado BOOLEAN DEFAULT false;
      ALTER TABLE codigos ADD COLUMN IF NOT EXISTS tipo VARCHAR(20) DEFAULT 'dias';
      ALTER TABLE codigos ADD COLUMN IF NOT EXISTS ilimitado BOOLEAN DEFAULT false;
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_historial_usuario ON historial(usuario_id);
      CREATE INDEX IF NOT EXISTS idx_historial_fecha   ON historial(fecha DESC);
      CREATE INDEX IF NOT EXISTS idx_notif_fecha       ON notificaciones_likes(creado_en DESC);
      CREATE INDEX IF NOT EXISTS idx_usuarios_username ON usuarios(LOWER(username));
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS auto_ids (
        id             SERIAL PRIMARY KEY,
        usuario_id     INTEGER REFERENCES usuarios(id) ON DELETE CASCADE,
        ff_uid         VARCHAR(30) NOT NULL,
        player_name    VARCHAR(100),
        nivel          INTEGER,
        region         VARCHAR(10) DEFAULT 'BR',
        likes_enviados INTEGER     DEFAULT 0,
        likes_meta     INTEGER     DEFAULT 0,
        dias_meta      INTEGER     DEFAULT 0,
        ultimo_envio   TIMESTAMP,
        proximo_envio  TIMESTAMP   DEFAULT NOW(),
        activo         BOOLEAN     DEFAULT true,
        creado_en      TIMESTAMP   DEFAULT NOW()
      );
    `);

    await client.query(`
      ALTER TABLE historial ADD COLUMN IF NOT EXISTS auto_envio BOOLEAN DEFAULT false;
      ALTER TABLE auto_ids  ADD COLUMN IF NOT EXISTS likes_meta INTEGER DEFAULT 0;
      ALTER TABLE auto_ids  ADD COLUMN IF NOT EXISTS dias_meta  INTEGER DEFAULT 0;
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_auto_ids_usuario ON auto_ids(usuario_id);
      CREATE INDEX IF NOT EXISTS idx_auto_ids_proximo ON auto_ids(proximo_envio) WHERE activo = true;
    `);

    console.log('✅ Base de datos lista');
  } finally {
    client.release();
  }
}

setInterval(async () => {
  try {
    await pool.query(
      `DELETE FROM notificaciones_likes WHERE creado_en < NOW() - INTERVAL '24 hours'`
    );
  } catch (e) {
    console.error('Cleanup notificaciones error:', e.message);
  }
}, 60 * 60 * 1000);

// ── Helpers ───────────────────────────────────────────────────
function genUID() {
  const c = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let r = 'BS-';
  for (let i = 0; i < 6; i++) r += c[Math.floor(Math.random() * c.length)];
  return r;
}
function genCodigo(custom) {
  if (custom && custom.trim()) return custom.trim().toUpperCase();
  const c = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let r = '';
  for (let i = 0; i < 8; i++) r += c[Math.floor(Math.random() * c.length)];
  return r;
}
function authMiddleware(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Token requerido' });
  try {
    req.user = jwt.verify(token, process.env.JWT_SECRET || 'bs_secret_2026');
    next();
  } catch {
    res.status(401).json({ error: 'Token inválido o expirado' });
  }
}
function adminMiddleware(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Token requerido' });
  try {
    const d = jwt.verify(token, process.env.JWT_SECRET || 'bs_secret_2026');
    if (!d.isAdmin) return res.status(403).json({ error: 'Acceso denegado' });
    req.user = d;
    next();
  } catch {
    res.status(401).json({ error: 'Token inválido' });
  }
}

// ── API de Free Fire ──────────────────────────────────────────
function llamarApiFF(uid, server = 'BR') {
  return new Promise((resolve, reject) => {
    const apiKey  = process.env.FF_API_KEY;
    const apiBase = (process.env.FF_API_URL || 'https://rtpysistemsapi.squareweb.app').replace(/\/+$/, '');
    if (!apiKey) return reject(new Error('FF_API_KEY no configurada en variables de entorno'));
    const params  = `uid=${encodeURIComponent(uid)}&apikey=${encodeURIComponent(apiKey)}&server=${encodeURIComponent(server)}`;
    const fullUrl = `${apiBase}/like?${params}`;
    console.log(`[API FF] Llamando: ${apiBase}/like?uid=${uid}&server=${server}&apikey=***`);
    const req = https.get(fullUrl, { timeout: 30000 }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        console.log(`[API FF] Status: ${res.statusCode} | ${data.slice(0, 200)}`);
        try {
          const parsed = JSON.parse(data);
          parsed._httpStatus = res.statusCode;
          if (parsed.key_usage) lastKeyUsage = String(parsed.key_usage);

          const errMsg = String(parsed.error || '').toLowerCase();
          if (res.statusCode === 500 && errMsg.includes('server_unavailable') && server !== 'BR') {
            console.log(`[API FF] Servidor ${server} no disponible, reintentando con BR...`);
            llamarApiFF(uid, 'BR').then(resolve).catch(reject);
            return;
          }

          resolve(parsed);
        } catch {
          reject(new Error(`Respuesta inválida de la API: ${data.slice(0, 100)}`));
        }
      });
    });
    req.on('error',   (err) => { console.error('[API FF] Error:', err.message); reject(err); });
    req.on('timeout', ()    => { req.destroy(); reject(new Error('Tiempo de espera agotado (30s)')); });
  });
}

function interpretarRespuestaFF(apiData) {
  const added  = parseInt(apiData.likes_added  || 0, 10);
  const before = parseInt(apiData.likes_before || 0, 10);
  const after  = parseInt(apiData.likes_after  || 0, 10);
  const msgRaw = String((apiData.message || '') + (apiData.error || '')).toLowerCase();

  if (apiData.status === 2) return { tipo: 'ya_recibio' };

  if (added === 0 && before > 0 && after === before) return { tipo: 'ya_recibio' };

  if (apiData._httpStatus === 429 || apiData._limit === true ||
      ['limit','already','wait','espera','daily'].some(k => msgRaw.includes(k)))
    return { tipo: 'limite' };

  if (apiData._httpStatus === 401 || msgRaw.includes('api key') || msgRaw.includes('apikey') ||
      msgRaw.includes('unauthorized') || msgRaw.includes('access denied') || msgRaw.includes('denegado'))
    return { tipo: 'auth_error' };

  if (apiData.status === 1 || added > 0) return { tipo: 'ok' };

  return { tipo: 'error' };
}

// ════════════════════════════════════════════════════════════════
//  PÚBLICO
// ════════════════════════════════════════════════════════════════

app.get('/api/public-stats', async (req, res) => {
  try {
    const [usuarios, likesTotal, likesHoy] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM usuarios'),
      pool.query('SELECT COALESCE(SUM(likes_agregados),0) AS total FROM historial'),
      pool.query(`SELECT COALESCE(SUM(likes_agregados),0) AS total FROM historial WHERE fecha::date = CURRENT_DATE`),
    ]);
    res.json({
      ok: true,
      usuarios:  parseInt(usuarios.rows[0].count, 10),
      likes:     parseInt(likesTotal.rows[0].total, 10),
      likes_hoy: parseInt(likesHoy.rows[0].total, 10),
    });
  } catch (err) {
    res.json({ ok: false, usuarios: 0, likes: 0, likes_hoy: 0 });
  }
});

app.get('/api/top-usuarios', async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT u.username, COALESCE(SUM(h.likes_agregados), 0) AS total_likes
      FROM usuarios u LEFT JOIN historial h ON h.usuario_id = u.id
      WHERE u.ilimitado = false
      GROUP BY u.id, u.username
      HAVING COALESCE(SUM(h.likes_agregados), 0) > 0
      ORDER BY total_likes DESC LIMIT 10
    `);
    res.json({ ok: true, top: r.rows });
  } catch (err) {
    console.error(err);
    res.json({ ok: false, top: [] });
  }
});

app.get('/api/notificaciones-likes', async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT username, player_name, likes_agregados, creado_en FROM notificaciones_likes ORDER BY creado_en DESC LIMIT 20`
    );
    res.json({ ok: true, notificaciones: r.rows });
  } catch (err) {
    res.json({ ok: false, notificaciones: [] });
  }
});

// ════════════════════════════════════════════════════════════════
//  USUARIOS
// ════════════════════════════════════════════════════════════════

app.post('/api/registro', rateLimit(8), async (req, res) => {
  try {
    const { username, password, contact } = req.body;
    if (!username || !password || !contact)
      return res.status(400).json({ error: 'Completa todos los campos' });
    if (username.length < 3)
      return res.status(400).json({ error: 'El usuario debe tener al menos 3 caracteres' });
    if (password.length < 6)
      return res.status(400).json({ error: 'La contraseña debe tener mínimo 6 caracteres' });

    const existe = await pool.query('SELECT id FROM usuarios WHERE LOWER(username)=LOWER($1)', [username]);
    if (existe.rows.length) return res.status(400).json({ error: 'Ese nombre de usuario ya está en uso' });

    const hash = await bcrypt.hash(password, 10);
    let uid, intentos = 0;
    do {
      uid = genUID();
      const chk = await pool.query('SELECT id FROM usuarios WHERE uid=$1', [uid]);
      if (!chk.rows.length) break;
    } while (++intentos < 20);

    const result = await pool.query(
      `INSERT INTO usuarios (uid,username,password,contact) VALUES ($1,$2,$3,$4)
       RETURNING id,uid,username,contact,plan_activo,plan_nombre,likes_disponibles,envios_por_dia,plan_vence,creado_en`,
      [uid, username, hash, contact]
    );
    const user  = result.rows[0];
    const token = jwt.sign(
      { id: user.id, uid: user.uid, username: user.username },
      process.env.JWT_SECRET || 'bs_secret_2026',
      { expiresIn: '365d' }
    );
    res.json({ ok: true, token, user });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});

app.post('/api/login', rateLimit(15), async (req, res) => {
  try {
    const { username, password } = req.body;
    if (!username || !password)
      return res.status(400).json({ error: 'Ingresa usuario y contraseña' });

    const result = await pool.query('SELECT * FROM usuarios WHERE LOWER(username)=LOWER($1)', [username]);
    if (!result.rows.length)
      return res.status(400).json({ error: 'Usuario o contraseña incorrectos' });

    const user = result.rows[0];
    if (!await bcrypt.compare(password, user.password))
      return res.status(400).json({ error: 'Usuario o contraseña incorrectos' });

    const token = jwt.sign(
      { id: user.id, uid: user.uid, username: user.username },
      process.env.JWT_SECRET || 'bs_secret_2026',
      { expiresIn: '365d' }
    );
    const { password: _, ...userSafe } = user;
    res.json({ ok: true, token, user: userSafe });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});

app.post('/api/recuperar', async (req, res) => {
  try {
    const { contact } = req.body;
    if (!contact) return res.status(400).json({ error: 'Ingresa tu correo o teléfono' });
    const result = await pool.query('SELECT uid, username FROM usuarios WHERE contact=$1', [contact]);
    if (!result.rows.length)
      return res.status(404).json({ error: 'No se encontró ninguna cuenta con ese contacto' });
    const u = result.rows[0];
    res.json({ ok: true, message: `Tu usuario es "${u.username}" y tu ID único es: ${u.uid}. Contacta al administrador para recuperar tu contraseña.` });
  } catch (err) {
    res.status(500).json({ error: 'Error interno' });
  }
});

app.post('/api/recuperar-check', async (req, res) => {
  try {
    const { contact } = req.body;
    if (!contact) return res.status(400).json({ error: 'Ingresa tu correo o teléfono' });
    const r = await pool.query('SELECT id, uid, username FROM usuarios WHERE contact=$1', [contact]);
    if (!r.rows.length)
      return res.status(404).json({ error: 'No se encontró cuenta con ese contacto' });
    const u = r.rows[0];
    res.json({ ok: true, userId: u.id, username: u.username, uid: u.uid });
  } catch (err) {
    res.status(500).json({ error: 'Error interno' });
  }
});

app.post('/api/recuperar-verificar', async (req, res) => {
  try {
    const { userId, codigo } = req.body;
    if (!userId || !codigo) return res.status(400).json({ error: 'Datos incompletos' });
    const r = await pool.query(
      `SELECT * FROM codigos_recuperacion WHERE usuario_id=$1 AND codigo=$2 AND usado=false AND expira_en > NOW()`,
      [userId, codigo.toUpperCase()]
    );
    if (!r.rows.length) return res.status(400).json({ error: 'Código inválido o expirado' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'Error interno' });
  }
});

app.post('/api/recuperar-cambiar', async (req, res) => {
  try {
    const { userId, password_nueva } = req.body;
    if (!userId || !password_nueva) return res.status(400).json({ error: 'Datos incompletos' });
    if (password_nueva.length < 6) return res.status(400).json({ error: 'Mínimo 6 caracteres' });
    const hash = await bcrypt.hash(password_nueva, 10);
    await pool.query('UPDATE usuarios SET password=$1 WHERE id=$2', [hash, userId]);
    await pool.query('UPDATE codigos_recuperacion SET usado=true WHERE usuario_id=$1', [userId]);
    res.json({ ok: true, message: 'Contraseña actualizada correctamente' });
  } catch (err) {
    res.status(500).json({ error: 'Error interno' });
  }
});

app.get('/api/perfil', authMiddleware, async (req, res) => {
  try {
    const today = new Date().toISOString().slice(0, 10);
    await pool.query(
      `UPDATE usuarios SET envios_hoy=0, fecha_ultimo_envio=$1
       WHERE id=$2 AND (fecha_ultimo_envio IS NULL OR fecha_ultimo_envio < $1)`,
      [today, req.user.id]
    );

    const result = await pool.query(
      `SELECT id,uid,username,contact,plan_activo,plan_nombre,plan_tipo,
              likes_disponibles,likes_limite_plan,likes_enviados_plan,
              envios_por_dia,envios_hoy,plan_vence,ilimitado,creado_en
       FROM usuarios WHERE id=$1`,
      [req.user.id]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Usuario no encontrado' });

    const u = result.rows[0];
    if (u.plan_activo && !u.ilimitado && u.plan_tipo === 'dias' && u.plan_vence && new Date(u.plan_vence) < new Date()) {
      await pool.query('UPDATE usuarios SET plan_activo=false WHERE id=$1', [u.id]);
      u.plan_activo = false;
    }

    const [hist, totalLikes] = await Promise.all([
      pool.query(
        `SELECT ff_uid,player_name,likes_antes,likes_despues,likes_agregados,nivel,region,auto_envio,fecha
         FROM historial WHERE usuario_id=$1 ORDER BY fecha DESC LIMIT 30`,
        [req.user.id]
      ),
      pool.query(
        `SELECT COALESCE(SUM(likes_agregados),0) AS total FROM historial WHERE usuario_id=$1`,
        [req.user.id]
      ),
    ]);

    u.total_likes_enviados = parseInt(totalLikes.rows[0].total, 10);
    res.json({ ok: true, user: u, historial: hist.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Error interno' });
  }
});

app.post('/api/canjear', authMiddleware, async (req, res) => {
  try {
    const { codigo } = req.body;
    if (!codigo) return res.status(400).json({ error: 'Ingresa un código' });

    const codResult = await pool.query('SELECT * FROM codigos WHERE codigo=$1', [codigo.toUpperCase()]);
    if (!codResult.rows.length) return res.status(400).json({ error: 'Código inválido o inexistente' });

    const cod = codResult.rows[0];
    if (cod.usado && !cod.ilimitado) return res.status(400).json({ error: 'Este código ya fue utilizado' });

    const user = await pool.query('SELECT uid FROM usuarios WHERE id=$1', [req.user.id]);

    if (cod.ilimitado) {
      await pool.query(
        `UPDATE usuarios SET plan_activo=true, plan_nombre='Plan Ilimitado', plan_tipo='ilimitado',
         likes_disponibles=999999, likes_limite_plan=999999, likes_enviados_plan=0,
         envios_por_dia=999, plan_vence=NULL, ilimitado=true WHERE id=$1`,
        [req.user.id]
      );
      return res.json({ ok: true, message: '🚀 Plan Ilimitado activado.' });
    }

    if (cod.tipo === 'likes') {
      await pool.query(
        `UPDATE usuarios SET plan_activo=true, plan_nombre=$1, plan_tipo='likes',
         likes_disponibles=$2, likes_limite_plan=$2, likes_enviados_plan=0,
         envios_por_dia=$3, plan_vence=NULL, ilimitado=false WHERE id=$4`,
        [`Plan ${cod.likes} Likes`, cod.likes, cod.envios_dia, req.user.id]
      );
      await pool.query('UPDATE codigos SET usado=true, usado_por=$1, usado_en=NOW() WHERE codigo=$2', [user.rows[0].uid, cod.codigo]);
      return res.json({ ok: true, message: `✅ Plan activado: ${cod.likes.toLocaleString()} likes · ${cod.envios_dia} envíos/día.` });
    }

    const vence = new Date(Date.now() + cod.dias * 86400000).toISOString();
    await pool.query(
      `UPDATE usuarios SET plan_activo=true, plan_nombre=$1, plan_tipo='dias',
       likes_disponibles=0, likes_limite_plan=0, likes_enviados_plan=0,
       envios_por_dia=$2, plan_vence=$3, ilimitado=false WHERE id=$4`,
      [`Plan ${cod.dias} días`, cod.envios_dia, vence, req.user.id]
    );
    await pool.query('UPDATE codigos SET usado=true, usado_por=$1, usado_en=NOW() WHERE codigo=$2', [user.rows[0].uid, cod.codigo]);
    res.json({ ok: true, message: `✅ Plan activado: ${cod.dias} días · ${cod.envios_dia} envíos/día.` });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Error interno' });
  }
});

// ── Envío MANUAL ──────────────────────────────────────────────
app.post('/api/enviar-likes', authMiddleware, async (req, res) => {
  try {
    const { ff_uid, server = 'BR' } = req.body;
    if (!ff_uid) return res.status(400).json({ error: 'Ingresa el UID de Free Fire' });
    if (!/^\d+$/.test(ff_uid.trim())) return res.status(400).json({ error: 'El UID solo debe contener números' });

    const today = new Date().toISOString().slice(0, 10);
    await pool.query(
      `UPDATE usuarios SET envios_hoy=0, fecha_ultimo_envio=$1
       WHERE id=$2 AND (fecha_ultimo_envio IS NULL OR fecha_ultimo_envio < $1)`,
      [today, req.user.id]
    );

    const result = await pool.query('SELECT * FROM usuarios WHERE id=$1', [req.user.id]);
    const u = result.rows[0];

    if (!u.plan_activo) return res.status(400).json({ error: 'Necesitas un plan activo para enviar likes' });

    if (!u.ilimitado) {
      if (u.plan_tipo === 'dias' && u.plan_vence && new Date(u.plan_vence) < new Date()) {
        await pool.query('UPDATE usuarios SET plan_activo=false WHERE id=$1', [u.id]);
        return res.status(400).json({ error: 'Tu plan de días ha vencido. Canjea un nuevo código.' });
      }
      if (u.envios_hoy >= u.envios_por_dia)
        return res.status(400).json({ error: `Límite diario alcanzado (${u.envios_por_dia} envíos/día). Vuelve mañana.` });
      if (u.plan_tipo === 'likes' && u.likes_disponibles <= 0) {
        await pool.query('UPDATE usuarios SET plan_activo=false WHERE id=$1', [u.id]);
        return res.status(400).json({ error: 'Has completado todos tus likes del plan. ¡Plan finalizado!' });
      }
    }

    const serversValidos = ['BR', 'IND', 'US', 'SAC', 'NA'];
    const serverFinal = serversValidos.includes((server || 'BR').toUpperCase()) ? server.toUpperCase() : 'BR';

    let apiData;
    try {
      apiData = await llamarApiFF(ff_uid.trim(), serverFinal);
    } catch (apiErr) {
      console.error('[enviar-likes] Error API:', apiErr.message);
      return res.status(500).json({ error: 'Error al contactar la API: ' + apiErr.message });
    }

    const interpretado = interpretarRespuestaFF(apiData);
    console.log(`[enviar-likes] Interpretación: ${interpretado.tipo}`, apiData);

    if (interpretado.tipo === 'auth_error')
      return res.status(500).json({ error: '❌ Error de autenticación con la API. Verifica que FF_API_KEY esté configurada.' });

    if (interpretado.tipo === 'limite')
      return res.status(400).json({ error: '⚠️ Este ID ya recibió likes recientemente. Intenta en unas horas.' });

    if (interpretado.tipo === 'ya_recibio') {
      const d      = apiData;
      const player = d.player || d.nickname || ff_uid.trim();
      const level  = d.level  || '—';
      const region = d.region || serverFinal;
      const before = parseInt(d.likes_before || 0, 10);
      const ahora  = new Date();
      const manana = new Date(Date.UTC(
        ahora.getUTCFullYear(),
        ahora.getUTCMonth(),
        ahora.getUTCDate() + 1,
        0, 0, 0, 0
      ));
      const diffMs = manana - ahora;
      const horas  = Math.floor(diffMs / 3600000);
      const mins   = Math.floor((diffMs % 3600000) / 60000);
      const tiempoRestante = horas > 0 ? `${horas}h ${mins}m` : `${mins}m`;

      return res.status(400).json({
        error: `Este UID ya recibió likes hoy. Disponible en ${tiempoRestante}.`,
        data: { jugador: player, uid: d.uid || ff_uid.trim(), nivel: level, region, likes_antes: before, likes_despues: before, likes_agregados: 0, tiempo_restante: tiempoRestante, ya_recibio: true },
      });
    }

    if (interpretado.tipo === 'error') {
      const motivo = apiData.message || apiData.error || 'UID no encontrado o respuesta inesperada';
      return res.status(400).json({ error: '❌ ' + motivo });
    }

    // ── Éxito ─────────────────────────────────────────────────
    const d      = apiData;
    const player = d.player || d.nickname || ff_uid.trim();
    const level  = d.level  || '—';
    const region = d.region || serverFinal;
    const before = parseInt(d.likes_before || 0, 10);
    const after  = parseInt(d.likes_after  || 0, 10);
    const tiempo = d.processing_time_seconds ? `${d.processing_time_seconds}s` : '—';

    const fromDiff       = (after > 0 && before >= 0 && after > before) ? (after - before) : 0;
    const fromAdded      = parseInt(d.likes_added      || 0, 10);
    const fromSuccessful = parseInt(d.successful_likes || 0, 10);
    let likesAdded = fromDiff > 0 ? fromDiff : Math.max(fromAdded, fromSuccessful);

    console.log(`[enviar-likes] diff=${fromDiff} added=${fromAdded} successful=${fromSuccessful} → final=${likesAdded}`);

    if (likesAdded > 0) {
      if (u.ilimitado) {
        await pool.query(
          `UPDATE usuarios SET envios_hoy=envios_hoy+1, likes_enviados_plan=likes_enviados_plan+$1, fecha_ultimo_envio=$2 WHERE id=$3`,
          [likesAdded, today, req.user.id]
        );
      } else if (u.plan_tipo === 'likes') {
        const newDisp = Math.max((u.likes_disponibles || 0) - likesAdded, 0);
        const newEnv  = (u.likes_enviados_plan || 0) + likesAdded;
        await pool.query(
          `UPDATE usuarios SET envios_hoy=envios_hoy+1, likes_disponibles=$1, likes_enviados_plan=$2, plan_activo=$3, fecha_ultimo_envio=$4 WHERE id=$5`,
          [newDisp, newEnv, newDisp > 0, today, req.user.id]
        );
      } else {
        await pool.query(
          `UPDATE usuarios SET envios_hoy=envios_hoy+1, likes_enviados_plan=likes_enviados_plan+$1, fecha_ultimo_envio=$2 WHERE id=$3`,
          [likesAdded, today, req.user.id]
        );
      }

      await pool.query(
        `INSERT INTO historial (usuario_id,ff_uid,player_name,likes_antes,likes_despues,likes_agregados,nivel,region,auto_envio)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,false)`,
        [req.user.id, ff_uid.trim(), player, before, after, likesAdded, level, region]
      );

      pool.query(
        `INSERT INTO notificaciones_likes (username, ff_uid, player_name, likes_agregados) VALUES ($1,$2,$3,$4)`,
        [u.username, ff_uid.trim(), player, likesAdded]
      ).catch(() => {});
    }

    res.json({
      ok: true,
      data: { jugador: player, uid: d.uid || ff_uid.trim(), nivel: level, region, likes_antes: before, likes_despues: after, likes_agregados: likesAdded, tiempo },
      message: `✅ ¡${likesAdded} likes enviados a ${player}!`,
    });
  } catch (err) {
    console.error('[enviar-likes] Error interno:', err);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});

app.post('/api/cambiar-pass', authMiddleware, async (req, res) => {
  try {
    const { password_actual, password_nueva } = req.body;
    if (!password_actual || !password_nueva) return res.status(400).json({ error: 'Completa ambos campos' });
    if (password_nueva.length < 6) return res.status(400).json({ error: 'Mínimo 6 caracteres' });
    const result = await pool.query('SELECT password FROM usuarios WHERE id=$1', [req.user.id]);
    if (!await bcrypt.compare(password_actual, result.rows[0].password))
      return res.status(400).json({ error: 'La contraseña actual es incorrecta' });
    await pool.query('UPDATE usuarios SET password=$1 WHERE id=$2', [await bcrypt.hash(password_nueva, 10), req.user.id]);
    res.json({ ok: true, message: '✅ Contraseña actualizada correctamente' });
  } catch (err) {
    res.status(500).json({ error: 'Error interno' });
  }
});

// ════════════════════════════════════════════════════════════════
//  AUTO LIKES — COMPARTE EL CONTADOR DIARIO CON EL MANUAL
//
//  Horario fijo hora Colombia (UTC-5):
//    Intento 1: 05:00 COL (10:00 UTC)
//    Intento 2: 12:00 COL (17:00 UTC)  ← si el 1 falló (0 likes)
//    Intento 3: 17:00 COL (22:00 UTC)  ← si el 2 falló
//    Intento 4: 00:00 COL (05:00 UTC)  ← si el 3 falló
//  Si un intento es exitoso → próximo envío = hora_exacta_de_exito + 24h
//  Si todos fallan → vuelve al intento 1 del día siguiente
// ════════════════════════════════════════════════════════════════

const SLOTS_UTC = [5, 10, 17, 22];

function nextSlot(desde, skipCurrent = false) {
  const utcH = desde.getUTCHours();
  const utcM = desde.getUTCMinutes();

  for (const h of SLOTS_UTC) {
    const pasado = h < utcH || (h === utcH && utcM > 0);
    if (!pasado) {
      const t = new Date(Date.UTC(
        desde.getUTCFullYear(), desde.getUTCMonth(), desde.getUTCDate(),
        h, 0, 0, 0
      ));
      if (skipCurrent && t.getTime() <= desde.getTime()) continue;
      if (t.getTime() > desde.getTime()) return t;
    }
  }
  return new Date(Date.UTC(
    desde.getUTCFullYear(), desde.getUTCMonth(), desde.getUTCDate() + 1,
    SLOTS_UTC[0], 0, 0, 0
  ));
}

// ── CAMBIO 1: slots = envios_por_dia (sin tope artificial).
//    Ilimitado = sin límite de slots (9999 como tope práctico).
function calcularSlots(usuario) {
  if (usuario.ilimitado) return 9999;
  if (!usuario.plan_activo) return 0;
  return Math.max(1, usuario.envios_por_dia);
}

// ── Función central AUTO ──────────────────────────────────────
async function procesarAutoID(autoId) {
  const today = new Date().toISOString().slice(0, 10);

  const aiRes = await pool.query(
    `SELECT ai.*,
            u.username, u.ilimitado, u.plan_activo, u.plan_tipo, u.plan_vence,
            u.likes_disponibles, u.likes_enviados_plan,
            u.envios_hoy, u.envios_por_dia, u.fecha_ultimo_envio
     FROM auto_ids ai
     JOIN usuarios u ON u.id = ai.usuario_id
     WHERE ai.id = $1`,
    [autoId]
  );
  if (!aiRes.rows.length) {
    console.log(`[AUTO] ID ${autoId} no encontrado en BD`);
    return;
  }
  const row = aiRes.rows[0];

  if (!row.activo) {
    console.log(`[AUTO] ID ${autoId} ya no está activo`);
    return;
  }

  if (!row.plan_activo && !row.ilimitado) {
    console.log(`[AUTO] ${row.username} sin plan activo, desactivando auto ID ${row.ff_uid}`);
    await pool.query('UPDATE auto_ids SET activo=false WHERE id=$1', [autoId]);
    return;
  }

  if (!row.ilimitado && row.plan_tipo === 'dias' && row.plan_vence && new Date(row.plan_vence) < new Date()) {
    await pool.query('UPDATE usuarios SET plan_activo=false WHERE id=$1', [row.usuario_id]);
    await pool.query('UPDATE auto_ids SET activo=false WHERE usuario_id=$1', [row.usuario_id]);
    console.log(`[AUTO] Plan de días vencido para ${row.username}`);
    return;
  }

  if (!row.ilimitado && row.plan_tipo === 'likes' && (row.likes_disponibles || 0) <= 0) {
    await pool.query('UPDATE auto_ids SET activo=false WHERE id=$1', [autoId]);
    console.log(`[AUTO] Sin likes disponibles para ${row.username}, desactivando ID ${row.ff_uid}`);
    return;
  }

  if (row.likes_meta > 0 && (row.likes_enviados || 0) >= row.likes_meta) {
    await pool.query('UPDATE auto_ids SET activo=false WHERE id=$1', [autoId]);
    console.log(`[AUTO] Meta de likes cumplida para ${row.ff_uid}`);
    return;
  }

  // ── CAMBIO 2: verificar límite diario compartido con el manual ──
  // Resetear envios_hoy si el día cambió
  if (!row.ilimitado) {
    const fechaUltimo = row.fecha_ultimo_envio
      ? new Date(row.fecha_ultimo_envio).toISOString().slice(0, 10)
      : null;
    const enviosHoyActual = fechaUltimo === today ? (row.envios_hoy || 0) : 0;

    if (enviosHoyActual >= row.envios_por_dia) {
      // Límite diario alcanzado — esperar al siguiente slot
      const siguiente = nextSlot(new Date(), true);
      await pool.query('UPDATE auto_ids SET proximo_envio=$1 WHERE id=$2', [siguiente.toISOString(), autoId]);
      console.log(`[AUTO] ${row.ff_uid} (${row.username}) → límite diario alcanzado (${enviosHoyActual}/${row.envios_por_dia}). Próximo slot: ${siguiente.toISOString()}`);
      return;
    }
  }

  // ── Llamar a la API de Free Fire ───────────────────────────
  let apiData;
  try {
    apiData = await llamarApiFF(row.ff_uid, row.region || 'BR');
  } catch (apiErr) {
    const siguiente = nextSlot(new Date(), true);
    await pool.query('UPDATE auto_ids SET proximo_envio=$1 WHERE id=$2', [siguiente.toISOString(), autoId]);
    console.error(`[AUTO] Error de red para ${row.ff_uid}: ${apiErr.message}. Próximo slot: ${siguiente.toISOString()}`);
    return;
  }

  const interpretado = interpretarRespuestaFF(apiData);
  const player = apiData.player || apiData.nickname || row.player_name || row.ff_uid;
  const level  = apiData.level  || row.nivel || null;
  const region = apiData.region || row.region || 'BR';

  console.log(`[AUTO] ${row.ff_uid} (${row.username}) → ${interpretado.tipo}`);

  if (interpretado.tipo === 'auth_error') {
    console.error('[AUTO] Error de autenticación con la API — revisar FF_API_KEY');
    return;
  }

  if (interpretado.tipo === 'ya_recibio' || interpretado.tipo === 'limite') {
    const ahora     = new Date();
    const siguiente = nextSlot(ahora, true);
    const diffMs    = siguiente - ahora;
    const hS = Math.floor(diffMs / 3600000);
    const mS = Math.floor((diffMs % 3600000) / 60000);
    await pool.query(
      `UPDATE auto_ids SET player_name=$1, nivel=$2, region=$3, proximo_envio=$4 WHERE id=$5`,
      [player, level, region, siguiente.toISOString(), autoId]
    );
    console.log(`[AUTO] ${row.ff_uid} ya recibió (${interpretado.tipo}). Próximo intento en ${hS}h ${mS}m`);
    return;
  }

  // ── Envío exitoso ──────────────────────────────────────────
  if (interpretado.tipo === 'ok') {
    const before = parseInt(apiData.likes_before || 0, 10);
    const after  = parseInt(apiData.likes_after  || 0, 10);
    const fromDiff   = (after > 0 && before >= 0 && after > before) ? (after - before) : 0;
    const likesAdded = fromDiff > 0
      ? fromDiff
      : Math.max(parseInt(apiData.likes_added || 0, 10), parseInt(apiData.successful_likes || 0, 10));

    if (likesAdded > 0) {
      // ── CAMBIO 3: el auto SÍ consume envios_hoy (contador compartido) ──
      if (!row.ilimitado && row.plan_tipo === 'likes') {
        const newDisp = Math.max((row.likes_disponibles || 0) - likesAdded, 0);
        await pool.query(
          `UPDATE usuarios
           SET likes_disponibles=$1,
               likes_enviados_plan=likes_enviados_plan+$2,
               plan_activo=$3,
               envios_hoy=envios_hoy+1,
               fecha_ultimo_envio=$4
           WHERE id=$5`,
          [newDisp, likesAdded, newDisp > 0, today, row.usuario_id]
        );
        if (newDisp <= 0) {
          await pool.query('UPDATE auto_ids SET activo=false WHERE id=$1', [autoId]);
          console.log(`[AUTO] Likes agotados para ${row.username}, desactivando ID ${row.ff_uid}`);
        }
      } else {
        // plan dias o ilimitado
        await pool.query(
          `UPDATE usuarios
           SET likes_enviados_plan=likes_enviados_plan+$1,
               envios_hoy=CASE WHEN ilimitado THEN envios_hoy ELSE envios_hoy+1 END,
               fecha_ultimo_envio=CASE WHEN ilimitado THEN fecha_ultimo_envio ELSE $2 END
           WHERE id=$3`,
          [likesAdded, today, row.usuario_id]
        );
      }

      await pool.query(
        `INSERT INTO historial (usuario_id,ff_uid,player_name,likes_antes,likes_despues,likes_agregados,nivel,region,auto_envio)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,true)`,
        [row.usuario_id, row.ff_uid, player, before, after, likesAdded, level, region]
      );

      const ahoraExito = new Date();
      const proximo24  = new Date(ahoraExito.getTime() + 24 * 60 * 60 * 1000);
      await pool.query(
        `UPDATE auto_ids
         SET player_name=$1, nivel=$2, region=$3,
             likes_enviados=likes_enviados+$4,
             ultimo_envio=$5,
             proximo_envio=$6
         WHERE id=$7`,
        [player, level, region, likesAdded, ahoraExito.toISOString(), proximo24.toISOString(), autoId]
      );

      pool.query(
        `INSERT INTO notificaciones_likes (username,ff_uid,player_name,likes_agregados) VALUES ($1,$2,$3,$4)`,
        [row.username, row.ff_uid, player, likesAdded]
      ).catch(() => {});

      console.log(`[AUTO] ✅ +${likesAdded} → ${player} (${row.ff_uid}) | proximo=${proximo24.toISOString()}`);

    } else {
      const ahora     = new Date();
      const siguiente = nextSlot(ahora, true);
      const diffMs    = siguiente - ahora;
      const hS = Math.floor(diffMs / 3600000);
      const mS = Math.floor((diffMs % 3600000) / 60000);
      await pool.query(
        `UPDATE auto_ids SET player_name=$1, nivel=$2, region=$3, proximo_envio=$4 WHERE id=$5`,
        [player, level, region, siguiente.toISOString(), autoId]
      );
      console.log(`[AUTO] ${row.ff_uid} → 0 likes recibidos. Próximo slot en ${hS}h ${mS}m`);
    }
    return;
  }

  // ── Error genérico → siguiente slot ────────────────────────
  const siguiente = nextSlot(new Date(), true);
  await pool.query('UPDATE auto_ids SET proximo_envio=$1 WHERE id=$2', [siguiente.toISOString(), autoId]);
  console.warn(`[AUTO] Respuesta inesperada para ${row.ff_uid}: ${interpretado.tipo}. Próximo slot: ${siguiente.toISOString()}`);
}

// ── GET /api/auto/ids ─────────────────────────────────────────
app.get('/api/auto/ids', authMiddleware, async (req, res) => {
  try {
    const uRes = await pool.query('SELECT * FROM usuarios WHERE id=$1', [req.user.id]);
    if (!uRes.rows.length) return res.status(404).json({ error: 'Usuario no encontrado' });
    const usuario = uRes.rows[0];
    const maxSlots = calcularSlots(usuario);

    const [ids, log] = await Promise.all([
      pool.query('SELECT * FROM auto_ids WHERE usuario_id=$1 AND activo=true ORDER BY creado_en ASC', [req.user.id]),
      pool.query(
        `SELECT ff_uid, player_name, likes_agregados, nivel, region, fecha
         FROM historial
         WHERE usuario_id=$1 AND auto_envio=true
         ORDER BY fecha DESC LIMIT 10`,
        [req.user.id]
      ),
    ]);

    res.json({ ok: true, ids: ids.rows, log: log.rows, max_slots: maxSlots });
  } catch (err) {
    console.error('[auto GET]', err);
    res.status(500).json({ error: 'Error interno' });
  }
});

// ── POST /api/auto/ids ────────────────────────────────────────
app.post('/api/auto/ids', authMiddleware, async (req, res) => {
  try {
    const { ff_uid, likes_meta = 0, dias_meta = 0 } = req.body;
    if (!ff_uid) return res.status(400).json({ error: 'Ingresa el UID de Free Fire' });
    if (!/^\d+$/.test(ff_uid.trim())) return res.status(400).json({ error: 'El UID solo debe contener números' });

    const uRes = await pool.query('SELECT * FROM usuarios WHERE id=$1', [req.user.id]);
    if (!uRes.rows.length) return res.status(404).json({ error: 'Usuario no encontrado' });
    const usuario = uRes.rows[0];

    if (!usuario.plan_activo && !usuario.ilimitado)
      return res.status(400).json({ error: 'Necesitas un plan activo para usar Auto Likes' });

    const maxSlots = calcularSlots(usuario);
    const actuales = await pool.query('SELECT COUNT(*) FROM auto_ids WHERE usuario_id=$1 AND activo=true', [req.user.id]);
    if (parseInt(actuales.rows[0].count, 10) >= maxSlots)
      return res.status(400).json({ error: `Slots llenos. Tu plan permite máximo ${maxSlots === 9999 ? 'ilimitados' : maxSlots} IDs en automático.` });

    const existe = await pool.query('SELECT id FROM auto_ids WHERE usuario_id=$1 AND ff_uid=$2 AND activo=true', [req.user.id, ff_uid.trim()]);
    if (existe.rows.length)
      return res.status(400).json({ error: 'Ese UID ya está registrado en modo automático' });

    // ── CAMBIO 4: verificar si tiene envíos disponibles HOY antes de insertar ──
    // Si no tiene envíos disponibles hoy, igual se agrega pero con proximo_envio al siguiente slot
    const today = new Date().toISOString().slice(0, 10);
    const fechaUltimo = usuario.fecha_ultimo_envio
      ? new Date(usuario.fecha_ultimo_envio).toISOString().slice(0, 10)
      : null;
    const enviosHoyActual = fechaUltimo === today ? (usuario.envios_hoy || 0) : 0;

    let proximoEnvio;
    if (!usuario.ilimitado && enviosHoyActual >= usuario.envios_por_dia) {
      // Sin envíos disponibles hoy → programar para el siguiente slot
      proximoEnvio = nextSlot(new Date(), true).toISOString();
    } else {
      // Tiene envíos disponibles → enviar ahora
      proximoEnvio = new Date().toISOString();
    }

    const result = await pool.query(
      `INSERT INTO auto_ids (usuario_id, ff_uid, likes_meta, dias_meta, proximo_envio)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [req.user.id, ff_uid.trim(), likes_meta, dias_meta, proximoEnvio]
    );
    const autoId = result.rows[0];

    const puedeEnviarAhora = !(!usuario.ilimitado && enviosHoyActual >= usuario.envios_por_dia);
    const msgExtra = puedeEnviarAhora
      ? ' Enviando likes ahora...'
      : ` Límite diario alcanzado, próximo envío programado.`;

    res.json({ ok: true, id: autoId, message: `✅ ID agregado.${msgExtra}` });

    if (puedeEnviarAhora) {
      setImmediate(async () => {
        try {
          await procesarAutoID(autoId.id);
        } catch (e) {
          console.error('[AUTO POST background]', e.message);
        }
      });
    }

  } catch (err) {
    console.error('[auto POST]', err);
    res.status(500).json({ error: 'Error interno' });
  }
});

// ── DELETE /api/auto/ids/:id ──────────────────────────────────
app.delete('/api/auto/ids/:id', authMiddleware, async (req, res) => {
  try {
    const check = await pool.query('SELECT id FROM auto_ids WHERE id=$1 AND usuario_id=$2', [req.params.id, req.user.id]);
    if (!check.rows.length) return res.status(404).json({ error: 'ID no encontrado' });
    await pool.query('UPDATE auto_ids SET activo=false WHERE id=$1', [req.params.id]);
    res.json({ ok: true, message: 'ID eliminado del modo automático' });
  } catch (err) {
    console.error('[auto DELETE]', err);
    res.status(500).json({ error: 'Error interno' });
  }
});

// ════════════════════════════════════════════════════════════════
//  CRON — Revisa auto_ids cada 2 minutos
// ════════════════════════════════════════════════════════════════

let cronRunning = false;
let cronSafetyTimer = null;

async function ejecutarAutoLikes() {
  if (cronRunning) {
    console.log('[AUTO] Ya hay un ciclo en ejecución, saltando...');
    return;
  }

  cronRunning = true;

  cronSafetyTimer = setTimeout(() => {
    if (cronRunning) {
      console.warn('[AUTO] ⚠️ Safety timeout alcanzado — liberando lock');
      cronRunning = false;
      cronSafetyTimer = null;
    }
  }, 8 * 60 * 1000);

  console.log('[AUTO] ===== Iniciando ciclo =====');

  try {
    const diag = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM auto_ids WHERE activo = true) AS total_activos,
        (SELECT COUNT(*) FROM auto_ids WHERE activo = true AND proximo_envio <= NOW()) AS listos,
        (SELECT COUNT(*) FROM usuarios WHERE plan_activo = true OR ilimitado = true) AS usuarios_con_plan
    `);
    const dg = diag.rows[0];
    console.log(`[AUTO] activos=${dg.total_activos} | listos para enviar=${dg.listos} | usuarios con plan=${dg.usuarios_con_plan}`);

    if (parseInt(dg.total_activos) > 0 && parseInt(dg.listos) === 0) {
      const next = await pool.query(
        `SELECT ai.ff_uid, ai.proximo_envio, u.username
         FROM auto_ids ai
         JOIN usuarios u ON u.id = ai.usuario_id
         WHERE ai.activo = true
         ORDER BY ai.proximo_envio ASC LIMIT 5`
      );
      next.rows.forEach(r => {
        const diffMs = new Date(r.proximo_envio) - Date.now();
        if (diffMs > 0) {
          const h = Math.floor(diffMs / 3600000);
          const m = Math.floor((diffMs % 3600000) / 60000);
          console.log(`[AUTO]   ${r.ff_uid} (${r.username}) → disponible en ${h}h ${m}m`);
        }
      });
    }

    const pendientes = await pool.query(`
      SELECT ai.id
      FROM auto_ids ai
      JOIN usuarios u ON u.id = ai.usuario_id
      WHERE ai.activo = true
        AND ai.proximo_envio <= NOW()
        AND (u.plan_activo = true OR u.ilimitado = true)
      ORDER BY ai.proximo_envio ASC
      LIMIT 100
    `);

    console.log(`[AUTO] ${pendientes.rows.length} IDs listos para procesar`);

    for (const row of pendientes.rows) {
      try {
        await procesarAutoID(row.id);
        await new Promise(r => setTimeout(r, 1500));
      } catch (itemErr) {
        console.error(`[AUTO] Error procesando ID ${row.id}:`, itemErr.message);
      }
    }

  } catch (err) {
    console.error('[AUTO] Error en ciclo principal:', err.message);
  } finally {
    if (cronSafetyTimer) {
      clearTimeout(cronSafetyTimer);
      cronSafetyTimer = null;
    }
    cronRunning = false;
    console.log('[AUTO] Ciclo finalizado');
  }
}

// ════════════════════════════════════════════════════════════════
//  ADMIN
// ════════════════════════════════════════════════════════════════

app.post('/api/admin/login', rateLimit(5), async (req, res) => {
  const { username, password } = req.body;
  if (username !== (process.env.ADMIN_USER || 'admin') || password !== (process.env.ADMIN_PASS || 'boostspeed2026'))
    return res.status(401).json({ error: 'Credenciales incorrectas' });
  const token = jwt.sign({ isAdmin: true, username }, process.env.JWT_SECRET || 'bs_secret_2026', { expiresIn: '30d' });
  res.json({ ok: true, token });
});

app.get('/api/admin/stats', adminMiddleware, async (req, res) => {
  try {
    const today = new Date().toISOString().slice(0, 10);
    const [tu, tc, uc, ap, ru, envHoy] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM usuarios'),
      pool.query('SELECT COUNT(*) FROM codigos'),
      pool.query('SELECT COUNT(*) FROM codigos WHERE usado=true'),
      pool.query('SELECT COUNT(*) FROM usuarios WHERE plan_activo=true'),
      pool.query(`SELECT id,uid,username,contact,plan_activo,plan_nombre,plan_tipo,likes_disponibles,ilimitado,creado_en FROM usuarios ORDER BY creado_en DESC LIMIT 10`),
      pool.query(`SELECT COUNT(*) AS total FROM historial WHERE fecha::date = $1`, [today]),
    ]);
    res.json({
      ok: true,
      totalUsuarios:     parseInt(tu.rows[0].count,      10),
      totalCodigos:      parseInt(tc.rows[0].count,      10),
      codigosUsados:     parseInt(uc.rows[0].count,      10),
      planesActivos:     parseInt(ap.rows[0].count,      10),
      enviosHoy:         parseInt(envHoy.rows[0].total,  10),
      keyUsage:          lastKeyUsage,
      usuariosRecientes: ru.rows,
    });
  } catch (err) {
    res.status(500).json({ error: 'Error interno' });
  }
});

app.get('/api/admin/envios-hoy', adminMiddleware, async (req, res) => {
  try {
    const today = new Date().toISOString().slice(0, 10);
    const r = await pool.query(`SELECT COUNT(*) AS total FROM historial WHERE fecha::date = $1`, [today]);
    res.json({ ok: true, envios_hoy: parseInt(r.rows[0].total, 10) });
  } catch (err) {
    res.json({ ok: false, envios_hoy: 0 });
  }
});

app.post('/api/admin/codigos', adminMiddleware, async (req, res) => {
  try {
    const { tipo, dias, likes, envios_dia, custom, ilimitado } = req.body;

    if (ilimitado) {
      let codigo, intentos = 0;
      do {
        codigo = genCodigo(intentos === 0 ? custom : '');
        const chk = await pool.query('SELECT id FROM codigos WHERE codigo=$1', [codigo]);
        if (!chk.rows.length) break;
      } while (++intentos < 20);
      const result = await pool.query(
        `INSERT INTO codigos (codigo, tipo, dias, likes, envios_dia, ilimitado) VALUES ($1, 'ilimitado', 0, 0, 999, true) RETURNING *`,
        [codigo]
      );
      return res.json({ ok: true, codigo: result.rows[0] });
    }

    if (!envios_dia) return res.status(400).json({ error: 'Completa todos los campos' });
    const tipoFinal = tipo === 'likes' ? 'likes' : 'dias';
    if (tipoFinal === 'dias'  && !dias)  return res.status(400).json({ error: 'Ingresa los días del plan' });
    if (tipoFinal === 'likes' && !likes) return res.status(400).json({ error: 'Ingresa la cantidad de likes' });

    let codigo, intentos = 0;
    do {
      codigo = genCodigo(intentos === 0 ? custom : '');
      const chk = await pool.query('SELECT id FROM codigos WHERE codigo=$1', [codigo]);
      if (!chk.rows.length) break;
    } while (++intentos < 20);

    const result = await pool.query(
      `INSERT INTO codigos (codigo, tipo, dias, likes, envios_dia) VALUES ($1, $2, $3, $4, $5) RETURNING *`,
      [codigo, tipoFinal, tipoFinal === 'dias' ? dias : 0, tipoFinal === 'likes' ? likes : 0, envios_dia]
    );
    res.json({ ok: true, codigo: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') return res.status(400).json({ error: 'Ese código ya existe' });
    res.status(500).json({ error: 'Error interno' });
  }
});

app.get('/api/admin/codigos', adminMiddleware, async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM codigos ORDER BY creado_en DESC');
    res.json({ ok: true, codigos: r.rows });
  } catch (err) { res.status(500).json({ error: 'Error interno' }); }
});

app.delete('/api/admin/codigos/:codigo', adminMiddleware, async (req, res) => {
  try {
    await pool.query('DELETE FROM codigos WHERE codigo=$1', [req.params.codigo]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: 'Error interno' }); }
});

app.get('/api/admin/usuarios', adminMiddleware, async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT id,uid,username,contact,plan_activo,plan_nombre,plan_tipo,likes_disponibles,likes_limite_plan,likes_enviados_plan,envios_por_dia,envios_hoy,plan_vence,ilimitado,creado_en FROM usuarios ORDER BY creado_en DESC`
    );
    res.json({ ok: true, usuarios: r.rows });
  } catch (err) { res.status(500).json({ error: 'Error interno' }); }
});

app.get('/api/admin/usuarios/buscar', adminMiddleware, async (req, res) => {
  try {
    const { q } = req.query;
    const r = await pool.query(
      `SELECT id,uid,username,contact,plan_activo,plan_nombre,plan_tipo,likes_disponibles,likes_limite_plan,likes_enviados_plan,envios_por_dia,envios_hoy,plan_vence,ilimitado,creado_en FROM usuarios WHERE uid ILIKE $1 OR LOWER(username) ILIKE LOWER($1) LIMIT 10`,
      [`%${q}%`]
    );
    res.json({ ok: true, usuarios: r.rows });
  } catch (err) { res.status(500).json({ error: 'Error interno' }); }
});

app.put('/api/admin/usuarios/:id', adminMiddleware, async (req, res) => {
  try {
    const {
      plan_tipo,
      dias_plan,
      likes_plan,
      envios_por_dia,
      plan_activo,
      dias_adicionales,
      likes_adicionales,
    } = req.body;

    const id  = req.params.id;
    const cur = await pool.query('SELECT * FROM usuarios WHERE id=$1', [id]);
    if (!cur.rows.length) return res.status(404).json({ error: 'Usuario no encontrado' });
    const u = cur.rows[0];

    const diasFinal  = dias_plan  !== undefined ? parseInt(dias_plan,  10) : (dias_adicionales  !== undefined ? parseInt(dias_adicionales,  10) : null);
    const likesFinal = likes_plan !== undefined ? parseInt(likes_plan, 10) : (likes_adicionales !== undefined ? parseInt(likes_adicionales, 10) : null);

    const tipoFinal    = plan_tipo    !== undefined ? plan_tipo    : u.plan_tipo;
    const activoFinal  = plan_activo  !== undefined ? plan_activo  : u.plan_activo;
    const enviosFinal  = envios_por_dia ? parseInt(envios_por_dia, 10) : u.envios_por_dia;

    let sets = [], params = [], idx = 1;

    sets.push(`plan_activo=$${idx++}`);
    params.push(activoFinal);

    sets.push(`envios_por_dia=$${idx++}`);
    params.push(enviosFinal);

    sets.push(`ilimitado=$${idx++}`);
    params.push(false);

    if (tipoFinal === 'dias' && diasFinal !== null) {
      const vence = diasFinal > 0
        ? new Date(Date.now() + diasFinal * 86400000).toISOString()
        : null;

      sets.push(`plan_tipo=$${idx++}`);    params.push('dias');
      sets.push(`plan_vence=$${idx++}`);   params.push(vence);
      sets.push(`plan_nombre=$${idx++}`);  params.push(`Plan ${diasFinal} días (Admin)`);
      sets.push(`likes_disponibles=$${idx++}`);    params.push(0);
      sets.push(`likes_limite_plan=$${idx++}`);    params.push(0);
      sets.push(`likes_enviados_plan=$${idx++}`);  params.push(0);

    } else if (tipoFinal === 'likes' && likesFinal !== null) {
      sets.push(`plan_tipo=$${idx++}`);            params.push('likes');
      sets.push(`likes_disponibles=$${idx++}`);    params.push(likesFinal);
      sets.push(`likes_limite_plan=$${idx++}`);    params.push(likesFinal);
      sets.push(`likes_enviados_plan=$${idx++}`);  params.push(0);
      sets.push(`plan_nombre=$${idx++}`);          params.push(`Plan ${likesFinal} Likes (Admin)`);
      sets.push(`plan_vence=$${idx++}`);           params.push(null);

    } else if (!activoFinal) {
      sets.push(`plan_vence=$${idx++}`);  params.push(null);
    }

    params.push(id);
    await pool.query(`UPDATE usuarios SET ${sets.join(',')} WHERE id=$${idx}`, params);

    const updated = await pool.query(
      `SELECT id,uid,username,contact,plan_activo,plan_nombre,plan_tipo,likes_disponibles,likes_limite_plan,likes_enviados_plan,envios_por_dia,envios_hoy,plan_vence,ilimitado,creado_en FROM usuarios WHERE id=$1`,
      [id]
    );
    res.json({ ok: true, usuario: updated.rows[0] });
  } catch (err) {
    console.error('[PUT usuario admin]', err);
    res.status(500).json({ error: 'Error interno' });
  }
});

app.put('/api/admin/usuarios/:id/ilimitado', adminMiddleware, async (req, res) => {
  try {
    const id = req.params.id;
    await pool.query(
      `UPDATE usuarios SET plan_activo=true, plan_nombre='Plan Ilimitado', plan_tipo='ilimitado',
       likes_disponibles=999999, likes_limite_plan=999999, likes_enviados_plan=0,
       envios_por_dia=999, plan_vence=NULL, ilimitado=true WHERE id=$1`,
      [id]
    );

    const updated = await pool.query(
      `SELECT id,uid,username,contact,plan_activo,plan_nombre,plan_tipo,likes_disponibles,likes_limite_plan,likes_enviados_plan,envios_por_dia,envios_hoy,plan_vence,ilimitado,creado_en FROM usuarios WHERE id=$1`,
      [id]
    );
    res.json({ ok: true, usuario: updated.rows[0] });
  } catch (err) {
    res.status(500).json({ error: 'Error interno' });
  }
});

app.delete('/api/admin/usuarios/:id', adminMiddleware, async (req, res) => {
  try {
    await pool.query('DELETE FROM usuarios WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: 'Error interno' }); }
});

app.post('/api/admin/codigos-recuperacion', adminMiddleware, async (req, res) => {
  try {
    const { usuario_id, codigo_custom } = req.body;
    if (!usuario_id) return res.status(400).json({ error: 'Usuario requerido' });
    const c = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let codigo = codigo_custom ? codigo_custom.trim().toUpperCase() : '';
    if (!codigo) { for (let i = 0; i < 8; i++) codigo += c[Math.floor(Math.random() * c.length)]; }
    await pool.query('DELETE FROM codigos_recuperacion WHERE usuario_id=$1', [usuario_id]);
    await pool.query(`INSERT INTO codigos_recuperacion (usuario_id, codigo) VALUES ($1, $2)`, [usuario_id, codigo]);
    res.json({ ok: true, codigo });
  } catch (err) {
    res.status(500).json({ error: 'Error interno' });
  }
});

app.post('/api/admin/auto/reset', adminMiddleware, async (req, res) => {
  try {
    const r = await pool.query(`
      UPDATE auto_ids SET proximo_envio = NOW() - INTERVAL '1 minute'
      WHERE activo = true
        AND usuario_id IN (SELECT id FROM usuarios WHERE plan_activo = true OR ilimitado = true)
      RETURNING id, ff_uid
    `);
    console.log(`[ADMIN] Reset auto_ids: ${r.rows.length} IDs reseteados a NOW()`);
    setImmediate(ejecutarAutoLikes);
    res.json({ ok: true, message: `✅ ${r.rows.length} IDs reseteados. Enviando ahora...`, ids: r.rows });
  } catch (err) {
    console.error('[ADMIN auto reset]', err);
    res.status(500).json({ error: 'Error interno' });
  }
});

app.get('/api/admin/auto/estado', adminMiddleware, async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT ai.id, ai.ff_uid, ai.activo, ai.proximo_envio, ai.likes_enviados,
             ai.player_name, u.username, u.plan_activo, u.ilimitado,
             u.envios_hoy, u.envios_por_dia,
             (ai.proximo_envio <= NOW()) AS listo
      FROM auto_ids ai
      JOIN usuarios u ON u.id = ai.usuario_id
      ORDER BY ai.proximo_envio ASC
    `);
    res.json({
      ok: true,
      total: r.rows.length,
      listos: r.rows.filter(x => x.listo).length,
      ahora: new Date().toISOString(),
      cronRunning,
      ids: r.rows
    });
  } catch (err) {
    res.status(500).json({ error: 'Error interno' });
  }
});

app.get('/api/admin/auto/debug', adminMiddleware, async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT
        ai.id, ai.ff_uid, ai.activo, ai.proximo_envio,
        ai.likes_enviados, ai.player_name,
        u.username, u.plan_activo, u.ilimitado, u.envios_hoy, u.envios_por_dia,
        (ai.proximo_envio <= NOW()) AS listo_now,
        NOW() AS db_now
      FROM auto_ids ai
      JOIN usuarios u ON u.id = ai.usuario_id
      ORDER BY ai.proximo_envio ASC
    `);
    res.json({ ok: true, total: r.rows.length, cronRunning, rows: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Fallback SPA ──────────────────────────────────────────────
app.get('*', (req, res) => res.sendFile(path.join(__dirname, '../public', 'index.html')));

// ════════════════════════════════════════════════════════════════
//  INICIAR SERVIDOR
// ════════════════════════════════════════════════════════════════
initDB().then(() => {
  app.listen(PORT, () => {
    console.log(`🚀 BoostSpeed corriendo en puerto ${PORT}`);

    // CAMBIO 1: Se eliminó el bloque que reseteaba proximo_envio al arrancar.
    // Esto evitaba que cada deploy en GitHub disparara envíos masivos.

    // Primer ciclo del cron a los 5 segundos de arrancar
    setTimeout(() => {
      console.log('[AUTO] Primer ciclo post-arranque...');
      ejecutarAutoLikes();
    }, 5000);

    // Cron cada 2 minutos
    setInterval(ejecutarAutoLikes, 2 * 60 * 1000);
  });
}).catch(err => {
  console.error('❌ Error conectando a la base de datos:', err.message);
  process.exit(1);
});
