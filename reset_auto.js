require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

async function reset() {
  try {
    const nextSlotUTC = new Date();
    nextSlotUTC.setUTCHours(13, 30, 0, 0); // 8:30 AM COT
    
    // Si ya pasamos las 8:30 AM, ponerlo para mañana (aunque el usuario dice que faltan 7m)
    if (Date.now() > nextSlotUTC.getTime()) {
       // Si es solo por unos minutos, tal vez el usuario prefiere "ahora"
       nextSlotUTC.setUTCMinutes(30); 
    }

    console.log(`Reseteando IDs a: ${nextSlotUTC.toISOString()}`);
    
    const r = await pool.query(
      `UPDATE auto_ids SET proximo_envio = $1 WHERE activo = true`,
      [nextSlotUTC.toISOString()]
    );
    
    console.log(`✅ ${r.rowCount} IDs actualizados exitosamente.`);
    process.exit(0);
  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  }
}
reset();
