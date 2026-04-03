# LiftingCast Reminder

Track a lifter during a meet and get notified when they are on deck. This service watches LiftingCast's public readonly meet data, keeps subscriptions in Postgres, and can send notifications through email.

[Deploy on Railway](https://railway.com/deploy?repo=https://github.com/Haroutkhac/liftingCastReminder) · [Source](https://github.com/Haroutkhac/liftingCastReminder)

## What it does

- Watches a LiftingCast meet in real time using the public readonly CouchDB feed
- Tracks specific lifters by name or subscription
- Sends notifications and recap emails for tracked meets
- Runs as a small Node service with Postgres-backed subscription state

## Deploy

One-click Railway deploy:

[Deploy on Railway](https://railway.com/deploy?repo=https://github.com/Haroutkhac/liftingCastReminder)

Environment variables:

```bash
MEET_ID=mfmnsrd1fve8
TRACK_LIFTER=Lifter Name
RESEND_API_KEY=your-resend-key
RESEND_FROM=alerts@example.com
DATABASE_URL=postgresql://...
```

`PORT` is provided automatically by Railway.

## Local development

```bash
npm install
npm start
```

Optional flags:

- `--meet-id=<id>` to override `MEET_ID`
- `--track="<name>"` to follow a lifter via console output
- `--list-lifters` to print the meet roster and exit
