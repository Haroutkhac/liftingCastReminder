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
  console.log('[DB] Subscriptions table ready');
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
    'SELECT lifter_name, meet_id, created_at FROM subscriptions WHERE email = $1 ORDER BY created_at DESC',
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

module.exports = { initDB, getSubscriptions, getAllMeetIds, addSubscription, removeSubscription, getSubscriptionsByEmail };
