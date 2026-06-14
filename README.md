# P2P Web Share

Direct browser-to-browser file sharing built for the MARS Open Projects 2026 web development brief. The app uses a small Node.js signaling server to create rooms and exchange WebRTC offers, answers, and ICE candidates. File bytes move only through the WebRTC data channel between browsers.

## Features

- Drag-and-drop sender flow with a generated room ID and invite link.
- Node.js + Express + Socket.IO signaling backend.
- React frontend with live connection state, transfer percentage, speed, and peer progress.
- WebRTC data-channel file transfer in 64 KB chunks.
- SHA-256 verification for every chunk before it is accepted by the receiver.
- Receiver-side auto-download after every chunk is verified.
- Graceful disconnect and room-close feedback.
- MVP file limit of 50 MB to stay within standard browser memory limits.

## Project Structure

```text
server/index.js   Socket.IO signaling server; never receives file bytes.
src/App.jsx       React app and WebRTC transfer protocol.
src/styles.css    Responsive application UI.
README.md         Setup and project notes.
SUBMISSION_INSTRUCTIONS.md  What to submit and how to submit it.
```

## Setup

Install Node.js 20 or newer, then run:

```bash
npm install
npm run dev
```

The development URLs are:

- Frontend: `http://localhost:5174`
- Signaling server: `http://localhost:3001`
- Health check: `http://localhost:3001/health`

Open the frontend in two browser windows. In the first window, choose a file and create a room. Open the generated invite link in the second window. The transfer starts after the WebRTC connection opens.

## Production Build

```bash
npm run build
NODE_ENV=production npm start
```

In production mode the Express server serves the built frontend from `dist/` and handles `/room/:id` links.

## Environment Variables

Copy `.env.example` to `.env` if you need custom ports or origins.

```text
PORT=3001
CLIENT_ORIGIN=http://localhost:5174
VITE_SIGNALING_URL=http://localhost:3001
```

## Notes And Limitations

- The signaling server only relays WebRTC setup messages. It does not read, process, or store any file chunks.
- The app uses a public STUN server. Some strict networks may require a TURN server for reliable cross-network transfers.
- Large-file streaming beyond 50 MB is listed as an optional extension in the brief and is not enabled in this MVP.
- Demo deployments usually need the frontend and signaling backend deployed separately unless a single Node host serves both.
