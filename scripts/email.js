/**
 * Email notifications via Resend.
 * Rate-limits to avoid duplicate emails (10-min TTL per recipient+lifter+position key).
 */
const { Resend } = require('resend');

let resend = null;
const fromEmail = process.env.RESEND_FROM || 'onboarding@resend.dev';

function getResend() {
  if (!resend) {
    if (!process.env.RESEND_API_KEY) return null;
    resend = new Resend(process.env.RESEND_API_KEY);
  }
  return resend;
}

// In-memory dedup: key -> timestamp
const recentlySent = new Map();
const DEDUP_TTL = 10 * 60 * 1000; // 10 minutes

function cleanupSentCache() {
  const now = Date.now();
  for (const [key, ts] of recentlySent) {
    if (now - ts > DEDUP_TTL) recentlySent.delete(key);
  }
}

// Run cleanup every 5 minutes
setInterval(cleanupSentCache, 5 * 60 * 1000).unref();

function youtubeSearchUrl(meetName) {
  if (!meetName) return null;
  return `https://www.youtube.com/results?search_query=${encodeURIComponent(meetName)}&sp=EgJAAQ%3D%3D`;
}

async function sendOnDeckEmail(toEmail, lifterName, meetName, liftName, position, meetId, subLifterName, details) {
  const dedupKey = `${toEmail}:${meetId}:${lifterName}:${liftName}:${position}`;
  if (recentlySent.has(dedupKey)) return false;

  const positionLabel =
    position === 'lifting' ? 'LIFTING NOW' :
    position === 'on-deck' ? 'ON DECK (next to lift)' :
    'IN THE HOLE (2 away)';

  const baseUrl = process.env.RAILWAY_PUBLIC_DOMAIN
    ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}`
    : `http://localhost:${process.env.PORT || 3000}`;

  // Use the subscriber's original lifter_name for unsubscribe (matches DB key)
  const unsubLifter = subLifterName || lifterName;
  const unsubLink = `${baseUrl}/unsubscribe?email=${encodeURIComponent(toEmail)}&lifter=${encodeURIComponent(unsubLifter)}&meet=${encodeURIComponent(meetId || '')}`;

  // Build attempt details section
  const d = details || {};
  const liftLabel = d.liftName ? d.liftName.charAt(0).toUpperCase() + d.liftName.slice(1) : liftName || 'Unknown';
  const attemptLine = d.attemptNumber ? `${liftLabel} — Attempt ${d.attemptNumber}` : liftLabel;
  const weightLine = d.weight ? `<strong>${d.weight} kg</strong>` : '';

  let placeLine = '';
  if (d.currentPlace) {
    placeLine = `Currently in <strong>${ordinal(d.currentPlace)} place</strong>`;
    if (d.projectedPlace && d.projectedPlace !== d.currentPlace) {
      placeLine += ` — moves to <strong>${ordinal(d.projectedPlace)} place</strong> if successful`;
    } else if (d.projectedPlace && d.projectedPlace === d.currentPlace) {
      placeLine += ` — stays in <strong>${ordinal(d.projectedPlace)} place</strong> if successful`;
    }
  } else if (d.projectedPlace) {
    placeLine = `Moves to <strong>${ordinal(d.projectedPlace)} place</strong> if successful`;
  }

  const client = getResend();
  if (!client) {
    console.log(`[EMAIL] Skipping (no RESEND_API_KEY): ${positionLabel} for ${lifterName} -> ${toEmail}`);
    return false;
  }

  try {
    await client.emails.send({
      from: fromEmail,
      to: toEmail,
      subject: `LiftAlert: ${lifterName} is ${positionLabel}`,
      html: `
        <h2>LiftAlert Notification</h2>
        <p><strong>${lifterName}</strong> is <strong>${positionLabel}</strong>!</p>
        <table style="margin:12px 0;border-collapse:collapse;">
          <tr><td style="padding:4px 12px 4px 0;color:#888;">Meet</td><td style="padding:4px 0;">${meetName || 'Unknown'}</td></tr>
          <tr><td style="padding:4px 12px 4px 0;color:#888;">Lift</td><td style="padding:4px 0;">${attemptLine}</td></tr>
          ${weightLine ? `<tr><td style="padding:4px 12px 4px 0;color:#888;">Weight</td><td style="padding:4px 0;">${weightLine}</td></tr>` : ''}
          ${placeLine ? `<tr><td style="padding:4px 12px 4px 0;color:#888;">Standings</td><td style="padding:4px 0;">${placeLine}</td></tr>` : ''}
        </table>
        ${youtubeSearchUrl(meetName) ? `<p style="margin:12px 0;"><a href="${youtubeSearchUrl(meetName)}" style="color:#3b82f6;font-weight:600;">Watch Live on YouTube</a></p>` : ''}
        <hr>
        <p style="font-size:12px;color:#888;">
          <a href="${unsubLink}">Unsubscribe from alerts for ${lifterName}</a>
        </p>
      `,
    });
    recentlySent.set(dedupKey, Date.now());
    console.log(`[EMAIL] Sent "${positionLabel}" alert to ${toEmail} for ${lifterName}`);
    return true;
  } catch (err) {
    console.error(`[EMAIL ERROR] Failed to send to ${toEmail}: ${err.message}`);
    return false;
  }
}

function ordinal(n) {
  const s = ['th', 'st', 'nd', 'rd'];
  const v = n % 100;
  return n + (s[(v - 20) % 10] || s[v] || s[0]);
}

async function sendSubscriptionConfirmation(toEmail, lifterName, meetName, meetDate, meetId) {
  const client = getResend();
  if (!client) {
    console.log(`[EMAIL] Skipping confirmation (no RESEND_API_KEY): ${lifterName} -> ${toEmail}`);
    return false;
  }

  const baseUrl = process.env.RAILWAY_PUBLIC_DOMAIN
    ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}`
    : `http://localhost:${process.env.PORT || 3000}`;

  const unsubLink = `${baseUrl}/unsubscribe?email=${encodeURIComponent(toEmail)}&lifter=${encodeURIComponent(lifterName)}&meet=${encodeURIComponent(meetId)}`;

  try {
    await client.emails.send({
      from: fromEmail,
      to: toEmail,
      subject: `LiftAlert: Subscription confirmed for ${lifterName}`,
      html: `
        <h2>You're subscribed!</h2>
        <p>You'll receive alerts when <strong>${lifterName}</strong> is on deck, in the hole, or lifting.</p>
        <table style="margin:16px 0;border-collapse:collapse;">
          <tr><td style="padding:4px 12px 4px 0;color:#888;">Meet</td><td style="padding:4px 0;">${meetName}</td></tr>
          ${meetDate ? `<tr><td style="padding:4px 12px 4px 0;color:#888;">Date</td><td style="padding:4px 0;">${meetDate}</td></tr>` : ''}
        </table>
        <p style="font-size:13px;color:#888;">No action needed — we'll email you automatically when it's almost time for ${lifterName} to lift.</p>
        ${youtubeSearchUrl(meetName) ? `<p style="margin:12px 0;"><a href="${youtubeSearchUrl(meetName)}" style="color:#3b82f6;font-weight:600;">Watch Live on YouTube</a></p>` : ''}
        <p style="font-size:13px;color:#e6a817;background:#2a2a1a;padding:8px 12px;border-radius:6px;margin:12px 0;"><strong>Important:</strong> Check your spam/junk folder and mark this email as "Not Spam" so you don't miss alerts!</p>
        <hr>
        <p style="font-size:12px;color:#888;">
          <a href="${unsubLink}">Unsubscribe from alerts for ${lifterName}</a>
        </p>
      `,
    });
    console.log(`[EMAIL] Sent subscription confirmation to ${toEmail} for ${lifterName}`);
    return true;
  } catch (err) {
    console.error(`[EMAIL ERROR] Failed to send confirmation to ${toEmail}: ${err.message}`);
    return false;
  }
}

module.exports = { sendOnDeckEmail, sendSubscriptionConfirmation };
