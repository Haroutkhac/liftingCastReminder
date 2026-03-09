/**
 * Postgres database layer for LiftAlert subscriptions.
 * Connects via DATABASE_URL env var (auto-provided by Railway Postgres plugin).
 */
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: (process.env.DATABASE_URL?.includes('.railway.internal') ||
        process.env.DATABASE_URL?.includes('localhost')) ? false
     : { rejectUnauthorized: false },
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 20000,
  max: 5,
  keepAlive: true,
  keepAliveInitialDelayMillis: 10000,
});

// Prevent pool errors from crashing the process; stale connections will be replaced
pool.on('error', (err) => {
  console.error('[DB] Pool connection error (will reconnect):', err.message);
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS subscriptions (
      id SERIAL PRIMARY KEY,
      email TEXT NOT NULL,
      lifter_name TEXT NOT NULL,
      meet_id TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(email, lifter_name, meet_id)
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS persistent_subscriptions (
      id SERIAL PRIMARY KEY,
      email TEXT NOT NULL,
      lifter_name TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS persistent_subs_email_name
      ON persistent_subscriptions (email, LOWER(lifter_name))
  `);
  console.log('[DB] Subscriptions + persistent_subscriptions tables ready');
}

async function getSubscriptions(meetId) {
  const { rows } = await pool.query(
    'SELECT email, lifter_name, meet_id FROM subscriptions WHERE meet_id = $1',
    [meetId]
  );
  return rows;
}

async function getAllMeetIds() {
  const { rows } = await pool.query('SELECT DISTINCT meet_id FROM subscriptions');
  return rows.map(r => r.meet_id);
}

async function addSubscription(email, lifterName, meetId) {
  await pool.query(
    `INSERT INTO subscriptions (email, lifter_name, meet_id)
     VALUES ($1, $2, $3)
     ON CONFLICT (email, lifter_name, meet_id) DO NOTHING`,
    [email, lifterName, meetId]
  );
}

async function getSubscriptionsByEmail(email) {
  const { rows } = await pool.query(
    'SELECT email, lifter_name, meet_id, created_at FROM subscriptions WHERE email = $1 ORDER BY created_at DESC',
    [email]
  );
  return rows;
}

async function removeSubscription(email, lifterName, meetId) {
  const { rowCount } = await pool.query(
    'DELETE FROM subscriptions WHERE email = $1 AND lifter_name = $2 AND meet_id = $3',
    [email, lifterName, meetId]
  );
  return rowCount > 0;
}

async function addPersistentSubscription(email, lifterName) {
  await pool.query(
    `INSERT INTO persistent_subscriptions (email, lifter_name)
     VALUES ($1, $2)
     ON CONFLICT (email, LOWER(lifter_name)) DO NOTHING`,
    [email, lifterName]
  );
}

async function removePersistentSubscription(email, lifterName) {
  const { rowCount } = await pool.query(
    'DELETE FROM persistent_subscriptions WHERE email = $1 AND LOWER(lifter_name) = LOWER($2)',
    [email, lifterName]
  );
  return rowCount > 0;
}

async function getPersistentSubscriptionsByEmail(email) {
  const { rows } = await pool.query(
    'SELECT email, lifter_name, created_at FROM persistent_subscriptions WHERE email = $1 ORDER BY created_at DESC',
    [email]
  );
  return rows;
}

async function getAllPersistentSubscriptions() {
  const { rows } = await pool.query('SELECT email, lifter_name FROM persistent_subscriptions');
  return rows;
}

async function getStats() {
  const subs = await pool.query('SELECT COUNT(DISTINCT email) as users, COUNT(*) as total FROM subscriptions');
  let persistent = { rows: [{ users: 0, total: 0 }] };
  try {
    persistent = await pool.query('SELECT COUNT(DISTINCT email) as users, COUNT(*) as total FROM persistent_subscriptions');
  } catch (_) { /* table may not exist */ }
  return {
    subscriptions: { uniqueUsers: Number(subs.rows[0].users), totalRows: Number(subs.rows[0].total) },
    persistentSubscriptions: { uniqueUsers: Number(persistent.rows[0].users), totalRows: Number(persistent.rows[0].total) },
  };
}

module.exports = {
  initDB, getSubscriptions, getAllMeetIds, addSubscription, removeSubscription, getSubscriptionsByEmail,
  addPersistentSubscription, removePersistentSubscription, getPersistentSubscriptionsByEmail, getAllPersistentSubscriptions,
  getStats,
};
