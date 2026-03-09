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
  await pool.query(`
    CREATE TABLE IF NOT EXISTS attempt_timestamps (
      id SERIAL PRIMARY KEY,
      meet_id TEXT NOT NULL,
      platform_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      lifter_id TEXT NOT NULL,
      lifter_name TEXT NOT NULL,
      lift_name TEXT NOT NULL,
      attempt_number TEXT NOT NULL,
      weight NUMERIC,
      wall_clock_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(meet_id, platform_id, attempt_id)
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS meet_videos (
      meet_id TEXT PRIMARY KEY,
      youtube_video_id TEXT NOT NULL,
      youtube_url TEXT NOT NULL,
      stream_start_epoch BIGINT NOT NULL,
      meet_name TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  console.log('[DB] All tables ready');
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

async function logAttemptTimestamp(meetId, platformId, attemptId, lifterId, lifterName, liftName, attemptNumber, weight) {
  await pool.query(
    `INSERT INTO attempt_timestamps (meet_id, platform_id, attempt_id, lifter_id, lifter_name, lift_name, attempt_number, weight)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
     ON CONFLICT (meet_id, platform_id, attempt_id) DO NOTHING`,
    [meetId, platformId, attemptId, lifterId, lifterName, liftName, attemptNumber, weight || null]
  );
}

async function getAttemptTimestamps(meetId, lifterId) {
  const { rows } = await pool.query(
    `SELECT * FROM attempt_timestamps WHERE meet_id = $1 AND lifter_id = $2 ORDER BY wall_clock_time ASC`,
    [meetId, lifterId]
  );
  return rows;
}

async function getAttemptTimestampsByMeet(meetId) {
  const { rows } = await pool.query(
    `SELECT * FROM attempt_timestamps WHERE meet_id = $1 ORDER BY wall_clock_time ASC`,
    [meetId]
  );
  return rows;
}

async function setMeetVideo(meetId, youtubeVideoId, youtubeUrl, streamStartEpoch, meetName) {
  await pool.query(
    `INSERT INTO meet_videos (meet_id, youtube_video_id, youtube_url, stream_start_epoch, meet_name)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (meet_id) DO UPDATE SET youtube_video_id = $2, youtube_url = $3, stream_start_epoch = $4, meet_name = COALESCE($5, meet_videos.meet_name)`,
    [meetId, youtubeVideoId, youtubeUrl, streamStartEpoch, meetName || null]
  );
}

async function getMeetVideo(meetId) {
  const { rows } = await pool.query('SELECT * FROM meet_videos WHERE meet_id = $1', [meetId]);
  return rows[0] || null;
}

async function getMeetVideos() {
  const { rows } = await pool.query('SELECT * FROM meet_videos ORDER BY created_at DESC');
  return rows;
}

module.exports = {
  initDB, getSubscriptions, getAllMeetIds, addSubscription, removeSubscription, getSubscriptionsByEmail,
  addPersistentSubscription, removePersistentSubscription, getPersistentSubscriptionsByEmail, getAllPersistentSubscriptions,
  getStats, logAttemptTimestamp, getAttemptTimestamps, getAttemptTimestampsByMeet,
  setMeetVideo, getMeetVideo, getMeetVideos,
};
