import cors from "cors";
import express from "express";
import { createServer } from "node:http";
import crypto from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Server } from "socket.io";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = Number(process.env.PORT || 3001);
const isProduction = process.env.NODE_ENV === "production";
const allowedOrigins = (process.env.CLIENT_ORIGIN || "http://localhost:5174,http://127.0.0.1:5174")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: isProduction
    ? undefined
    : {
        origin: allowedOrigins,
        methods: ["GET", "POST"]
      }
});

const rooms = new Map();
const ROOM_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const ROOM_ID_LENGTH = 6;

app.use(express.json());

if (!isProduction) {
  app.use(
    cors({
      origin: allowedOrigins
    })
  );
}

app.get("/health", (_request, response) => {
  response.json({
    ok: true,
    rooms: rooms.size,
    uptime: process.uptime()
  });
});

if (isProduction) {
  const distPath = path.resolve(__dirname, "../dist");
  app.use(express.static(distPath));
  app.get("*", (_request, response) => {
    response.sendFile(path.join(distPath, "index.html"));
  });
}

function createRoomId() {
  let id = "";
  do {
    id = Array.from({ length: ROOM_ID_LENGTH }, () => ROOM_ALPHABET[crypto.randomInt(ROOM_ALPHABET.length)]).join("");
  } while (rooms.has(id));
  return id;
}

function normalizeRoomId(roomId) {
  return String(roomId || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, ROOM_ID_LENGTH);
}

function cleanMetadata(metadata = {}) {
  return {
    name: String(metadata.name || "shared-file").slice(0, 180),
    size: Number.isFinite(Number(metadata.size)) ? Number(metadata.size) : 0,
    type: String(metadata.type || "application/octet-stream").slice(0, 120),
    lastModified: Number.isFinite(Number(metadata.lastModified)) ? Number(metadata.lastModified) : null
  };
}

function leaveCurrentRoom(socket, options = {}) {
  const roomId = socket.data.roomId;
  if (!roomId) {
    return;
  }

  const room = rooms.get(roomId);
  socket.leave(roomId);
  socket.data.roomId = null;
  socket.data.role = null;

  if (!room) {
    return;
  }

  room.peers.delete(socket.id);

  if (socket.id === room.ownerId) {
    socket.to(roomId).emit("room-closed", {
      reason: "The sender left the room."
    });
    rooms.delete(roomId);
    return;
  }

  if (!options.silent) {
    socket.to(roomId).emit("peer-disconnected", {
      peerId: socket.id,
      peerCount: room.peers.size
    });
  }

  if (room.peers.size === 0) {
    rooms.delete(roomId);
  }
}

io.on("connection", (socket) => {
  socket.emit("server-ready", {
    socketId: socket.id
  });

  socket.on("create-room", (payload = {}, acknowledge = () => {}) => {
    leaveCurrentRoom(socket, { silent: true });

    const roomId = createRoomId();
    const room = {
      id: roomId,
      ownerId: socket.id,
      peers: new Set([socket.id]),
      metadata: cleanMetadata(payload.metadata),
      createdAt: Date.now()
    };

    rooms.set(roomId, room);
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = "sender";

    acknowledge({
      ok: true,
      roomId,
      metadata: room.metadata
    });
  });

  socket.on("join-room", (payload = {}, acknowledge = () => {}) => {
    const roomId = normalizeRoomId(payload.roomId);
    const room = rooms.get(roomId);

    if (!room) {
      acknowledge({
        ok: false,
        error: "Room not found. Ask the sender for a fresh invite link."
      });
      return;
    }

    if (!room.peers.has(socket.id) && room.peers.size >= 2) {
      acknowledge({
        ok: false,
        error: "This room already has a sender and receiver."
      });
      return;
    }

    leaveCurrentRoom(socket, { silent: true });

    room.peers.add(socket.id);
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = "receiver";

    acknowledge({
      ok: true,
      roomId,
      metadata: room.metadata,
      peerCount: room.peers.size
    });

    socket.to(roomId).emit("peer-joined", {
      peerId: socket.id,
      peerCount: room.peers.size
    });
  });

  socket.on("signal", (payload = {}) => {
    const roomId = normalizeRoomId(payload.roomId);
    const room = rooms.get(roomId);

    if (!room || !room.peers.has(socket.id)) {
      return;
    }

    socket.to(roomId).emit("signal", {
      from: socket.id,
      type: payload.type,
      payload: payload.payload
    });
  });

  socket.on("leave-room", () => {
    leaveCurrentRoom(socket);
  });

  socket.on("disconnect", () => {
    leaveCurrentRoom(socket);
  });
});

httpServer.listen(PORT, () => {
  console.log(`P2P Web Share signaling server listening on http://localhost:${PORT}`);
});
