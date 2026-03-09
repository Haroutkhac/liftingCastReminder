#!/usr/bin/env node
/**
 * Meet Recap Generator
 *
 * Standalone CLI script to generate per-lifter YouTube timestamp links
 * from attempt_timestamps logged during a live meet.
 *
 * Usage:
 *   node scripts/meet_recap.js --meet-id=<id> [options]
 *
 * Options:
 *   --meet-id=<id>         Meet ID (required)
 *   --youtube-url=<url>    YouTube VOD URL (skips search)
 *   --lifter=<name>        Filter to a single lifter (partial match, case-insensitive)
 *   --timestamps-only      Just print raw timestamps, no YouTube offset calc
 *   --clip                 Download video clips per lifter (requires yt-dlp + ffmpeg)
 *   --output-dir=<dir>     Output directory for clips (default: ./recaps)
 *   --pre-buffer=<sec>     Seconds before attempt for clips (default: 30)
 *   --post-buffer=<sec>    Seconds after attempt for clips (default: 20)
 *   --set-video            Store YouTube URL + stream start in DB for web recap page
 *   --meet-name=<name>     Meet name (used with --set-video)
 */
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// Parse CLI args
const args = {};
process.argv.slice(2).forEach(arg => {
  const match = arg.match(/^--([^=]+)(?:=(.*))?$/);
  if (match) args[match[1]] = match[2] !== undefined ? match[2] : true;
});

if (!args['meet-id']) {
  console.error('Usage: node scripts/meet_recap.js --meet-id=<id> [--youtube-url=<url>] [--lifter=<name>] [--timestamps-only] [--clip]');
  process.exit(1);
}

const meetId = args['meet-id'];
const youtubeUrl = args['youtube-url'] || null;
const lifterFilter = args['lifter'] || null;
const timestampsOnly = !!args['timestamps-only'];
const shouldClip = !!args['clip'];
const outputDir = args['output-dir'] || './recaps';
const preBuffer = parseInt(args['pre-buffer'] || '30', 10);
const postBuffer = parseInt(args['post-buffer'] || '20', 10);

// Require DATABASE_URL
if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL env var required. Export it or run with:\n  DATABASE_URL=<url> node scripts/meet_recap.js ...');
  process.exit(1);
}

const { getAttemptTimestamps, getAttemptTimestampsByMeet, setMeetVideo } = require('./db');

function formatTime(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  return h > 0 ? `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}` : `${m}:${String(s).padStart(2, '0')}`;
}

function getYoutubeVideoId(url) {
  const match = url.match(/(?:youtu\.be\/|youtube\.com\/(?:watch\?v=|embed\/|live\/))([^?&/]+)/);
  return match ? match[1] : null;
}

function getStreamStartTime(url) {
  try {
    const json = execSync(`yt-dlp --dump-json --no-download "${url}" 2>/dev/null`, { encoding: 'utf8', maxBuffer: 10 * 1024 * 1024 });
    const data = JSON.parse(json);
    // For live streams, release_timestamp is when the stream started
    return data.release_timestamp || data.timestamp || null;
  } catch (err) {
    console.error(`[WARN] Could not get stream metadata: ${err.message}`);
    return null;
  }
}

function clipAttempts(videoUrl, lifterName, attempts, streamStart) {
  const safeLifterName = lifterName.replace(/[^a-zA-Z0-9_-]/g, '_');
  const lifterDir = path.join(outputDir, safeLifterName);
  fs.mkdirSync(lifterDir, { recursive: true });

  const clipFiles = [];
  attempts.forEach((a, i) => {
    const offset = Math.floor((new Date(a.wall_clock_time).getTime() / 1000) - streamStart);
    const start = Math.max(0, offset - preBuffer);
    const end = offset + postBuffer;
    const clipFile = path.join(lifterDir, `${String(i + 1).padStart(2, '0')}_${a.lift_name}_${a.attempt_number}.mp4`);
    clipFiles.push(clipFile);

    console.log(`  Clipping ${a.lift_name} attempt ${a.attempt_number} (${formatTime(start)} - ${formatTime(end)})...`);
    try {
      execSync(`yt-dlp --download-sections "*${start}-${end}" -o "${clipFile}" "${videoUrl}" 2>/dev/null`, { stdio: 'inherit' });
    } catch (err) {
      console.error(`  [ERROR] Failed to clip: ${err.message}`);
    }
  });

  // Concatenate clips
  if (clipFiles.length > 1 && clipFiles.every(f => fs.existsSync(f))) {
    const concatFile = path.join(lifterDir, 'concat_list.txt');
    fs.writeFileSync(concatFile, clipFiles.map(f => `file '${path.resolve(f)}'`).join('\n'));
    const highlightFile = path.join(lifterDir, `${safeLifterName}_highlights.mp4`);
    console.log(`  Concatenating ${clipFiles.length} clips -> ${highlightFile}`);
    try {
      execSync(`ffmpeg -f concat -safe 0 -i "${concatFile}" -c copy "${highlightFile}" -y 2>/dev/null`, { stdio: 'inherit' });
    } catch (err) {
      console.error(`  [ERROR] Failed to concatenate: ${err.message}`);
    }
    fs.unlinkSync(concatFile);
  }
}

async function setVideo() {
  if (!youtubeUrl) {
    console.error('--youtube-url is required with --set-video');
    process.exit(1);
  }
  const videoId = getYoutubeVideoId(youtubeUrl);
  if (!videoId) {
    console.error('Could not parse YouTube video ID from URL');
    process.exit(1);
  }
  console.log(`Fetching stream metadata for ${youtubeUrl}...`);
  const streamStart = getStreamStartTime(youtubeUrl);
  if (!streamStart) {
    console.error('Could not determine stream start time. Provide a VOD URL (not a live stream that just started).');
    process.exit(1);
  }
  const meetName = args['meet-name'] || null;
  await setMeetVideo(meetId, videoId, youtubeUrl, streamStart, meetName);
  console.log(`Saved video for meet ${meetId}:`);
  console.log(`  Video ID: ${videoId}`);
  console.log(`  Stream start: ${new Date(streamStart * 1000).toISOString()}`);
  if (meetName) console.log(`  Meet name: ${meetName}`);
  console.log('Done — recap page will now show YouTube links for this meet.');
}

async function main() {
  if (args['set-video']) {
    await setVideo();
    return;
  }

  // Fetch timestamps
  let timestamps;
  if (lifterFilter) {
    // Get all timestamps then filter by name
    const all = await getAttemptTimestampsByMeet(meetId);
    const filterLower = lifterFilter.toLowerCase();
    timestamps = all.filter(t => t.lifter_name.toLowerCase().includes(filterLower));
  } else {
    timestamps = await getAttemptTimestampsByMeet(meetId);
  }

  if (timestamps.length === 0) {
    console.error(`No timestamps found for meet ${meetId}${lifterFilter ? ` matching "${lifterFilter}"` : ''}`);
    process.exit(1);
  }

  console.log(`Found ${timestamps.length} attempt timestamps for meet ${meetId}\n`);

  // Group by lifter
  const byLifter = {};
  for (const t of timestamps) {
    if (!byLifter[t.lifter_id]) byLifter[t.lifter_id] = { name: t.lifter_name, attempts: [] };
    byLifter[t.lifter_id].attempts.push(t);
  }

  // Timestamps-only mode: just print and exit
  if (timestampsOnly) {
    for (const [lifterId, data] of Object.entries(byLifter)) {
      console.log(`\n${data.name} (${lifterId}):`);
      for (const a of data.attempts) {
        const w = a.weight ? ` @ ${a.weight}kg` : '';
        console.log(`  ${a.lift_name} attempt ${a.attempt_number}${w} — ${new Date(a.wall_clock_time).toLocaleString()}`);
      }
    }
    process.exit(0);
  }

  // Resolve YouTube VOD
  let videoUrl = youtubeUrl;
  if (!videoUrl) {
    console.log('No --youtube-url provided. Searching YouTube...');
    try {
      const videoId = execSync(`yt-dlp "ytsearch1:${meetId}" --print id --no-download 2>/dev/null`, { encoding: 'utf8' }).trim();
      if (videoId) {
        videoUrl = `https://youtu.be/${videoId}`;
        console.log(`Found: ${videoUrl}\n`);
      }
    } catch (err) {
      console.error('Could not find YouTube VOD automatically. Provide --youtube-url=<url>');
      process.exit(1);
    }
  }

  if (!videoUrl) {
    console.error('No YouTube VOD found. Provide --youtube-url=<url>');
    process.exit(1);
  }

  const videoId = getYoutubeVideoId(videoUrl);

  // Get stream start time
  console.log('Fetching stream metadata...');
  const streamStart = getStreamStartTime(videoUrl);
  if (!streamStart) {
    console.error('Could not determine stream start time. The VOD may not have release_timestamp metadata.');
    console.error('Falling back to the earliest logged timestamp as stream start.\n');
  }

  const effectiveStart = streamStart || Math.floor(new Date(timestamps[0].wall_clock_time).getTime() / 1000);

  // Generate recap
  for (const [lifterId, data] of Object.entries(byLifter)) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`${data.name}`);
    console.log('='.repeat(60));

    for (const a of data.attempts) {
      const wallEpoch = Math.floor(new Date(a.wall_clock_time).getTime() / 1000);
      const offset = Math.max(0, wallEpoch - effectiveStart);
      const w = a.weight ? ` @ ${a.weight}kg` : '';
      const timeStr = formatTime(offset);
      const ytLink = videoId ? `https://youtu.be/${videoId}?t=${offset}` : `offset: ${timeStr}`;
      console.log(`  ${a.lift_name} attempt ${a.attempt_number}${w} — ${timeStr}  ${ytLink}`);
    }

    // Clip if requested
    if (shouldClip) {
      console.log(`\n  Generating clips for ${data.name}...`);
      clipAttempts(videoUrl, data.name, data.attempts, effectiveStart);
    }
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`Recap complete. ${Object.keys(byLifter).length} lifters, ${timestamps.length} attempts.`);
  if (!streamStart) {
    console.log('NOTE: Stream start time was estimated from earliest timestamp — offsets may be approximate.');
  }
}

main().catch(err => {
  console.error(`Fatal error: ${err.message}`);
  process.exit(1);
}).finally(() => {
  // Let the pool drain
  setTimeout(() => process.exit(0), 500);
});
