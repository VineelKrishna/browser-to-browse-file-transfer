import { useEffect, useMemo, useRef, useState } from "react";
import { io } from "socket.io-client";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Copy,
  Download,
  Link,
  Plug,
  Send,
  ShieldCheck,
  Upload,
  Wifi,
  XCircle
} from "lucide-react";

const MAX_FILE_SIZE = 50 * 1024 * 1024;
const CHUNK_SIZE = 64 * 1024;
const MAX_BUFFERED_AMOUNT = 8 * 1024 * 1024;
const LOW_BUFFERED_AMOUNT = 2 * 1024 * 1024;
const SIGNALING_URL =
  import.meta.env.VITE_SIGNALING_URL ||
  (import.meta.env.DEV ? `${window.location.protocol}//${window.location.hostname}:3001` : window.location.origin);

const RTC_CONFIGURATION = {
  iceServers: [{ urls: "stun:stun.l.google.com:19302" }]
};

function getRoomIdFromPath() {
  const parts = window.location.pathname.split("/").filter(Boolean);
  return parts[0] === "room" && parts[1] ? parts[1].toUpperCase() : "";
}

function emptyTransfer() {
  return {
    direction: "idle",
    status: "Idle",
    fileName: "",
    fileSize: 0,
    bytes: 0,
    totalBytes: 0,
    percent: 0,
    speed: 0,
    chunksVerified: 0,
    totalChunks: 0,
    peerBytes: 0
  };
}

function formatBytes(bytes = 0) {
  if (!bytes) {
    return "0 B";
  }
  const units = ["B", "KB", "MB", "GB"];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / 1024 ** index;
  return `${value.toFixed(value >= 10 || index === 0 ? 0 : 1)} ${units[index]}`;
}

function formatSpeed(bytesPerSecond = 0) {
  return `${formatBytes(bytesPerSecond)}/s`;
}

function toHex(buffer) {
  return Array.from(new Uint8Array(buffer))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

async function sha256(buffer) {
  const digest = await window.crypto.subtle.digest("SHA-256", buffer);
  return toHex(digest);
}

function isBinaryPayload(payload) {
  return payload instanceof ArrayBuffer || ArrayBuffer.isView(payload);
}

function createLogEntry(message, tone = "info") {
  const id = window.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`;
  const time = new Date().toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });
  return { id, time, message, tone };
}

export default function App() {
  const initialRoomId = useMemo(getRoomIdFromPath, []);
  const [mode, setMode] = useState(initialRoomId ? "receive" : "send");
  const [socketStatus, setSocketStatus] = useState("Connecting");
  const [peerStatus, setPeerStatus] = useState("No peer connected");
  const [roomId, setRoomId] = useState(initialRoomId);
  const [joinInput, setJoinInput] = useState(initialRoomId);
  const [roomLink, setRoomLink] = useState(initialRoomId ? `${window.location.origin}/room/${initialRoomId}` : "");
  const [selectedFile, setSelectedFile] = useState(null);
  const [expectedFile, setExpectedFile] = useState(null);
  const [transfer, setTransfer] = useState(emptyTransfer);
  const [logs, setLogs] = useState([createLogEntry("Client opened. Signaling is connecting.")]);
  const [error, setError] = useState("");
  const [copyLabel, setCopyLabel] = useState("Copy");
  const [downloadedFile, setDownloadedFile] = useState(null);

  const socketRef = useRef(null);
  const pcRef = useRef(null);
  const dataChannelRef = useRef(null);
  const roomIdRef = useRef(roomId);
  const modeRef = useRef(mode);
  const fileRef = useRef(selectedFile);
  const joinedInitialRoomRef = useRef(false);
  const pendingCandidatesRef = useRef([]);
  const incomingMetadataRef = useRef(null);
  const pendingChunkRef = useRef(null);
  const receivedChunksRef = useRef([]);
  const receivedBytesRef = useRef(0);
  const verifiedChunksRef = useRef(0);
  const receiveStartRef = useRef(0);
  const sendStartRef = useRef(0);
  const sendingRef = useRef(false);
  const messageQueueRef = useRef(Promise.resolve());

  useEffect(() => {
    roomIdRef.current = roomId;
  }, [roomId]);

  useEffect(() => {
    modeRef.current = mode;
  }, [mode]);

  useEffect(() => {
    fileRef.current = selectedFile;
  }, [selectedFile]);

  useEffect(() => {
    const socket = io(SIGNALING_URL, {
      transports: ["websocket", "polling"]
    });

    socketRef.current = socket;

    socket.on("connect", () => {
      setSocketStatus("Connected");
      addLog(`Connected to signaling server as ${socket.id}.`);

      if (initialRoomId && !joinedInitialRoomRef.current) {
        joinedInitialRoomRef.current = true;
        joinRoom(initialRoomId);
      }
    });

    socket.on("disconnect", () => {
      setSocketStatus("Disconnected");
      setPeerStatus("Signaling disconnected");
      addLog("Signaling server disconnected.", "warning");
    });

    socket.on("connect_error", () => {
      setSocketStatus("Connection error");
      setError(`Cannot reach signaling server at ${SIGNALING_URL}.`);
    });

    socket.on("peer-joined", () => {
      setPeerStatus("Receiver joined. Creating direct channel.");
      addLog("Receiver joined the room.");
      startSenderPeer();
    });

    socket.on("peer-disconnected", () => {
      setPeerStatus("Peer disconnected");
      setTransfer((current) => ({
        ...current,
        status: current.direction === "idle" ? "Peer disconnected" : "Interrupted"
      }));
      addLog("The other browser disconnected.", "warning");
      closePeerConnection();
    });

    socket.on("room-closed", ({ reason }) => {
      setPeerStatus("Room closed");
      setError(reason || "The room was closed.");
      addLog(reason || "The room was closed.", "warning");
      closePeerConnection();
    });

    socket.on("signal", (message) => {
      handleSignal(message);
    });

    return () => {
      socket.emit("leave-room");
      socket.disconnect();
      closePeerConnection();
    };
    // The socket lifecycle intentionally uses refs for live room and file state.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function addLog(message, tone = "info") {
    setLogs((current) => [createLogEntry(message, tone), ...current].slice(0, 9));
  }

  function updateTransferProgress(direction, bytes, totalBytes, startedAt, extra = {}) {
    const elapsedSeconds = Math.max((performance.now() - startedAt) / 1000, 0.1);
    const percent = totalBytes ? Math.min(100, (bytes / totalBytes) * 100) : 0;

    setTransfer((current) => ({
      ...current,
      direction,
      bytes,
      totalBytes,
      percent,
      speed: bytes / elapsedSeconds,
      ...extra
    }));
  }

  function closePeerConnection() {
    if (dataChannelRef.current) {
      dataChannelRef.current.onopen = null;
      dataChannelRef.current.onmessage = null;
      dataChannelRef.current.onclose = null;
      dataChannelRef.current.onerror = null;
      if (dataChannelRef.current.readyState === "open" || dataChannelRef.current.readyState === "connecting") {
        dataChannelRef.current.close();
      }
      dataChannelRef.current = null;
    }

    if (pcRef.current) {
      pcRef.current.onicecandidate = null;
      pcRef.current.onconnectionstatechange = null;
      pcRef.current.oniceconnectionstatechange = null;
      pcRef.current.ondatachannel = null;
      pcRef.current.close();
      pcRef.current = null;
    }

    pendingCandidatesRef.current = [];
    sendingRef.current = false;
  }

  function createPeerConnection(role) {
    closePeerConnection();

    const pc = new RTCPeerConnection(RTC_CONFIGURATION);
    pcRef.current = pc;

    pc.onicecandidate = (event) => {
      if (event.candidate) {
        emitSignal("candidate", event.candidate);
      }
    };

    pc.onconnectionstatechange = () => {
      const state = pc.connectionState;
      if (state === "connected") {
        setPeerStatus("Direct WebRTC connection established");
      } else if (["failed", "disconnected", "closed"].includes(state)) {
        setPeerStatus(`Peer connection ${state}`);
      } else {
        setPeerStatus(`Peer connection ${state}`);
      }
    };

    pc.oniceconnectionstatechange = () => {
      if (pc.iceConnectionState === "failed") {
        setError("WebRTC ICE negotiation failed. Try both browsers on the same network or add a TURN server.");
      }
    };

    if (role === "receiver") {
      pc.ondatachannel = (event) => {
        setupDataChannel(event.channel, "receiver");
      };
    }

    return pc;
  }

  function setupDataChannel(channel, side) {
    channel.binaryType = "arraybuffer";
    channel.bufferedAmountLowThreshold = LOW_BUFFERED_AMOUNT;
    dataChannelRef.current = channel;

    channel.onopen = () => {
      setPeerStatus("Data channel open");
      addLog("Encrypted WebRTC data channel opened.");

      if (side === "sender") {
        sendSelectedFile();
      }
    };

    channel.onmessage = (event) => {
      messageQueueRef.current = messageQueueRef.current
        .then(() => handleDataChannelMessage(event.data))
        .catch((messageError) => {
          setError(messageError.message || "Failed to process peer message.");
        });
    };

    channel.onclose = () => {
      setPeerStatus("Data channel closed");
      addLog("Data channel closed.", "warning");
    };

    channel.onerror = () => {
      setPeerStatus("Data channel error");
      setError("The WebRTC data channel reported an error.");
    };
  }

  function emitSignal(type, payload) {
    const activeRoomId = roomIdRef.current;
    if (!activeRoomId || !socketRef.current?.connected) {
      return;
    }

    socketRef.current.emit("signal", {
      roomId: activeRoomId,
      type,
      payload
    });
  }

  async function handleSignal(message) {
    try {
      if (message.type === "offer") {
        const pc = pcRef.current || createPeerConnection("receiver");
        await pc.setRemoteDescription(new RTCSessionDescription(message.payload));
        await flushPendingCandidates(pc);
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        emitSignal("answer", answer);
        setPeerStatus("Offer accepted. Waiting for data channel.");
        addLog("WebRTC offer accepted.");
        return;
      }

      if (message.type === "answer") {
        const pc = pcRef.current;
        if (!pc) {
          return;
        }
        await pc.setRemoteDescription(new RTCSessionDescription(message.payload));
        await flushPendingCandidates(pc);
        setPeerStatus("Answer received. ICE is connecting.");
        addLog("WebRTC answer received.");
        return;
      }

      if (message.type === "candidate") {
        const candidate = new RTCIceCandidate(message.payload);
        const pc = pcRef.current;
        if (!pc || !pc.remoteDescription) {
          pendingCandidatesRef.current.push(candidate);
          return;
        }
        await pc.addIceCandidate(candidate);
      }
    } catch (signalError) {
      setError(signalError.message || "Failed to process WebRTC signal.");
    }
  }

  async function flushPendingCandidates(pc) {
    const candidates = pendingCandidatesRef.current.splice(0);
    for (const candidate of candidates) {
      await pc.addIceCandidate(candidate);
    }
  }

  async function startSenderPeer() {
    if (!fileRef.current) {
      setError("Select a file before a receiver joins.");
      return;
    }

    try {
      const pc = createPeerConnection("sender");
      const channel = pc.createDataChannel("file-transfer", {
        ordered: true
      });

      setupDataChannel(channel, "sender");

      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      emitSignal("offer", offer);
      setPeerStatus("Offer sent. Waiting for receiver answer.");
    } catch (peerError) {
      setError(peerError.message || "Could not start WebRTC peer.");
    }
  }

  function sendJson(channel, payload) {
    if (channel?.readyState === "open") {
      channel.send(JSON.stringify(payload));
    }
  }

  function waitForBuffer(channel) {
    if (channel.bufferedAmount <= MAX_BUFFERED_AMOUNT) {
      return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
      const cleanup = () => {
        channel.removeEventListener("bufferedamountlow", onLow);
        channel.removeEventListener("close", onClose);
      };
      const onLow = () => {
        cleanup();
        resolve();
      };
      const onClose = () => {
        cleanup();
        reject(new Error("Data channel closed during transfer."));
      };

      channel.addEventListener("bufferedamountlow", onLow);
      channel.addEventListener("close", onClose, { once: true });
    });
  }

  async function sendSelectedFile() {
    const file = fileRef.current;
    const channel = dataChannelRef.current;

    if (!file || !channel || channel.readyState !== "open" || sendingRef.current) {
      return;
    }

    sendingRef.current = true;
    sendStartRef.current = performance.now();
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    let sentBytes = 0;

    setTransfer({
      ...emptyTransfer(),
      direction: "send",
      status: "Transferring",
      fileName: file.name,
      fileSize: file.size,
      totalBytes: file.size,
      totalChunks
    });

    sendJson(channel, {
      kind: "metadata",
      name: file.name,
      type: file.type || "application/octet-stream",
      size: file.size,
      totalChunks,
      chunkSize: CHUNK_SIZE,
      lastModified: file.lastModified
    });

    try {
      for (let index = 0; index < totalChunks; index += 1) {
        if (channel.readyState !== "open") {
          throw new Error("Data channel closed before the transfer completed.");
        }

        const start = index * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, file.size);
        const buffer = await file.slice(start, end).arrayBuffer();
        const hash = await sha256(buffer);

        sendJson(channel, {
          kind: "chunk-info",
          index,
          size: buffer.byteLength,
          hash
        });

        await waitForBuffer(channel);
        channel.send(buffer);
        sentBytes += buffer.byteLength;

        updateTransferProgress("send", sentBytes, file.size, sendStartRef.current, {
          status: "Transferring",
          chunksVerified: index + 1,
          totalChunks
        });
      }

      await waitForBuffer(channel);
      sendJson(channel, {
        kind: "complete",
        bytes: file.size,
        totalChunks
      });
      setTransfer((current) => ({
        ...current,
        status: "Sent. Waiting for receiver verification.",
        percent: 100
      }));
      addLog("All chunks sent to receiver.");
    } catch (transferError) {
      sendJson(channel, {
        kind: "transfer-error",
        message: transferError.message || "Sender transfer failed."
      });
      setError(transferError.message || "Sender transfer failed.");
      setTransfer((current) => ({
        ...current,
        status: "Failed"
      }));
    } finally {
      sendingRef.current = false;
    }
  }

  async function handleDataChannelMessage(payload) {
    if (typeof payload === "string") {
      const message = JSON.parse(payload);
      await handleControlMessage(message);
      return;
    }

    if (isBinaryPayload(payload)) {
      const buffer = payload instanceof ArrayBuffer ? payload : payload.buffer.slice(payload.byteOffset, payload.byteOffset + payload.byteLength);
      await handleIncomingChunk(buffer);
    }
  }

  async function handleControlMessage(message) {
    if (message.kind === "metadata") {
      incomingMetadataRef.current = message;
      pendingChunkRef.current = null;
      receivedChunksRef.current = new Array(message.totalChunks);
      receivedBytesRef.current = 0;
      verifiedChunksRef.current = 0;
      receiveStartRef.current = performance.now();
      setExpectedFile(message);
      setDownloadedFile(null);
      setTransfer({
        ...emptyTransfer(),
        direction: "receive",
        status: "Receiving",
        fileName: message.name,
        fileSize: message.size,
        totalBytes: message.size,
        totalChunks: message.totalChunks
      });
      addLog(`Receiving ${message.name}.`);
      return;
    }

    if (message.kind === "chunk-info") {
      pendingChunkRef.current = message;
      return;
    }

    if (message.kind === "complete") {
      finalizeDownload();
      return;
    }

    if (message.kind === "receiver-progress") {
      setTransfer((current) => ({
        ...current,
        peerBytes: message.bytesReceived,
        status: current.status === "Sent. Waiting for receiver verification." ? "Receiver verifying" : current.status
      }));
      return;
    }

    if (message.kind === "receiver-complete") {
      setTransfer((current) => ({
        ...current,
        status: "Receiver verified and downloaded",
        peerBytes: message.bytesReceived || current.peerBytes
      }));
      addLog("Receiver verified every chunk and started the download.", "success");
      return;
    }

    if (message.kind === "transfer-error") {
      setError(message.message || "The peer reported a transfer error.");
      setTransfer((current) => ({
        ...current,
        status: "Failed"
      }));
    }
  }

  async function handleIncomingChunk(buffer) {
    const info = pendingChunkRef.current;
    const metadata = incomingMetadataRef.current;
    const channel = dataChannelRef.current;

    if (!info || !metadata) {
      setError("Received file bytes before chunk metadata.");
      return;
    }

    const actualHash = await sha256(buffer);
    if (actualHash !== info.hash) {
      const message = `Chunk ${info.index + 1} failed SHA-256 verification.`;
      setError(message);
      sendJson(channel, {
        kind: "transfer-error",
        message
      });
      setTransfer((current) => ({
        ...current,
        status: "Failed verification"
      }));
      return;
    }

    receivedChunksRef.current[info.index] = buffer;
    receivedBytesRef.current += buffer.byteLength;
    verifiedChunksRef.current += 1;
    pendingChunkRef.current = null;

    updateTransferProgress("receive", receivedBytesRef.current, metadata.size, receiveStartRef.current, {
      status: "Receiving",
      chunksVerified: verifiedChunksRef.current,
      totalChunks: metadata.totalChunks
    });

    sendJson(channel, {
      kind: "receiver-progress",
      bytesReceived: receivedBytesRef.current,
      chunksVerified: verifiedChunksRef.current,
      totalChunks: metadata.totalChunks
    });
  }

  function finalizeDownload() {
    const metadata = incomingMetadataRef.current;
    const channel = dataChannelRef.current;

    if (!metadata) {
      setError("Missing file metadata at completion.");
      return;
    }

    if (verifiedChunksRef.current !== metadata.totalChunks || receivedBytesRef.current !== metadata.size) {
      const message = "Transfer ended before every chunk was verified.";
      setError(message);
      sendJson(channel, {
        kind: "transfer-error",
        message
      });
      return;
    }

    const blob = new Blob(receivedChunksRef.current, {
      type: metadata.type || "application/octet-stream"
    });
    const objectUrl = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = objectUrl;
    anchor.download = metadata.name || "download";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    window.setTimeout(() => URL.revokeObjectURL(objectUrl), 30_000);

    setDownloadedFile({
      name: metadata.name,
      size: metadata.size
    });
    setTransfer((current) => ({
      ...current,
      status: "Complete. Download started.",
      percent: 100
    }));
    sendJson(channel, {
      kind: "receiver-complete",
      fileName: metadata.name,
      bytesReceived: receivedBytesRef.current,
      chunksVerified: verifiedChunksRef.current
    });
    addLog("File verified and download started.", "success");
  }

  function handleFileSelection(fileList) {
    const nextFile = fileList?.[0];
    setError("");

    if (!nextFile) {
      return;
    }

    if (nextFile.size > MAX_FILE_SIZE) {
      setSelectedFile(null);
      setError(`The MVP limit is ${formatBytes(MAX_FILE_SIZE)}. Choose a smaller file for this build.`);
      return;
    }

    setSelectedFile(nextFile);
    setRoomId("");
    roomIdRef.current = "";
    setRoomLink("");
    setPeerStatus("File ready");
    setTransfer(emptyTransfer());
    addLog(`Selected ${nextFile.name}.`);
  }

  function createRoom() {
    if (!selectedFile) {
      setError("Choose a file first.");
      return;
    }

    if (!socketRef.current?.connected) {
      setError("Signaling server is not connected.");
      return;
    }

    closePeerConnection();
    setError("");
    setPeerStatus("Creating room");

    socketRef.current.timeout(7000).emit(
      "create-room",
      {
        metadata: {
          name: selectedFile.name,
          size: selectedFile.size,
          type: selectedFile.type,
          lastModified: selectedFile.lastModified
        }
      },
      (ackError, response) => {
        if (ackError || !response?.ok) {
          setPeerStatus("Room creation failed");
          setError(response?.error || "Room creation timed out.");
          return;
        }

        const nextLink = `${window.location.origin}/room/${response.roomId}`;
        setMode("send");
        setRoomId(response.roomId);
        roomIdRef.current = response.roomId;
        setJoinInput(response.roomId);
        setRoomLink(nextLink);
        setPeerStatus("Room live. Waiting for receiver.");
        setCopyLabel("Copy");
        addLog(`Room ${response.roomId} created.`);
      }
    );
  }

  function joinRoom(targetRoomId = joinInput) {
    const nextRoomId = String(targetRoomId || "").trim().toUpperCase();
    if (!nextRoomId) {
      setError("Enter a room ID.");
      return;
    }

    if (!socketRef.current?.connected) {
      setError("Signaling server is not connected.");
      return;
    }

    closePeerConnection();
    setError("");
    setMode("receive");
    setRoomId(nextRoomId);
    roomIdRef.current = nextRoomId;
    setJoinInput(nextRoomId);
    setRoomLink(`${window.location.origin}/room/${nextRoomId}`);
    setPeerStatus("Joining room");
    window.history.replaceState(null, "", `/room/${nextRoomId}`);

    socketRef.current.timeout(7000).emit("join-room", { roomId: nextRoomId }, (ackError, response) => {
      if (ackError || !response?.ok) {
        setPeerStatus("Join failed");
        setError(response?.error || "Room join timed out.");
        return;
      }

      setExpectedFile(response.metadata || null);
      setPeerStatus("Joined. Waiting for sender offer.");
      addLog(`Joined room ${response.roomId}.`);
    });
  }

  async function copyInviteLink() {
    if (!roomLink) {
      return;
    }

    await navigator.clipboard.writeText(roomLink);
    setCopyLabel("Copied");
    window.setTimeout(() => setCopyLabel("Copy"), 1400);
  }

  function switchToSendMode() {
    setMode("send");
    setError("");
    setExpectedFile(null);
    setDownloadedFile(null);
    setTransfer(emptyTransfer());
    window.history.replaceState(null, "", "/");
  }

  const connectionTone = socketStatus === "Connected" ? "good" : socketStatus === "Connecting" ? "neutral" : "bad";
  const transferLabel = transfer.direction === "send" ? "Sending" : transfer.direction === "receive" ? "Receiving" : "Transfer";

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">MARS Open Projects 2026</p>
          <h1>P2P Web Share</h1>
        </div>
        <div className="status-strip" aria-label="Connection status">
          <StatusPill icon={<Wifi size={16} />} label={socketStatus} tone={connectionTone} />
          <StatusPill icon={<Plug size={16} />} label={peerStatus} tone={peerStatus.includes("failed") ? "bad" : "neutral"} />
        </div>
      </header>

      <section className="workspace">
        <div className="primary-panel">
          <div className="mode-tabs" role="tablist" aria-label="Transfer mode">
            <button className={mode === "send" ? "active" : ""} type="button" onClick={switchToSendMode}>
              <Upload size={17} />
              Send
            </button>
            <button className={mode === "receive" ? "active" : ""} type="button" onClick={() => setMode("receive")}>
              <Download size={17} />
              Receive
            </button>
          </div>

          {mode === "send" ? (
            <SenderPanel
              selectedFile={selectedFile}
              roomId={roomId}
              roomLink={roomLink}
              copyLabel={copyLabel}
              onFiles={handleFileSelection}
              onCreateRoom={createRoom}
              onCopy={copyInviteLink}
            />
          ) : (
            <ReceiverPanel
              joinInput={joinInput}
              setJoinInput={setJoinInput}
              roomId={roomId}
              expectedFile={expectedFile}
              downloadedFile={downloadedFile}
              onJoin={joinRoom}
            />
          )}

          {error ? (
            <div className="alert" role="alert">
              <AlertTriangle size={18} />
              <span>{error}</span>
            </div>
          ) : null}
        </div>

        <aside className="side-panel">
          <section className="metric-card">
            <div className="section-heading">
              <Activity size={18} />
              <h2>{transferLabel}</h2>
            </div>
            <ProgressMeter transfer={transfer} />
          </section>

          <section className="metric-card">
            <div className="section-heading">
              <ShieldCheck size={18} />
              <h2>Verification</h2>
            </div>
            <div className="verification-grid">
              <span>Chunk hash</span>
              <strong>SHA-256</strong>
              <span>Verified</span>
              <strong>
                {transfer.chunksVerified}/{transfer.totalChunks || 0}
              </strong>
              <span>Server file bytes</span>
              <strong>0 B</strong>
            </div>
          </section>

          <section className="metric-card">
            <div className="section-heading">
              <CheckCircle2 size={18} />
              <h2>Activity</h2>
            </div>
            <div className="log-list">
              {logs.map((log) => (
                <div className={`log-row ${log.tone}`} key={log.id}>
                  <time>{log.time}</time>
                  <span>{log.message}</span>
                </div>
              ))}
            </div>
          </section>
        </aside>
      </section>
    </main>
  );
}

function StatusPill({ icon, label, tone }) {
  return (
    <span className={`status-pill ${tone}`}>
      {icon}
      {label}
    </span>
  );
}

function SenderPanel({ selectedFile, roomId, roomLink, copyLabel, onFiles, onCreateRoom, onCopy }) {
  return (
    <div className="transfer-panel">
      <label
        className="dropzone"
        onDragOver={(event) => event.preventDefault()}
        onDrop={(event) => {
          event.preventDefault();
          onFiles(event.dataTransfer.files);
        }}
      >
        <input type="file" onChange={(event) => onFiles(event.target.files)} />
        <Upload size={34} />
        <strong>{selectedFile ? selectedFile.name : "Drop a file"}</strong>
        <span>{selectedFile ? `${formatBytes(selectedFile.size)} selected` : "or browse from this browser"}</span>
      </label>

      <div className="action-row">
        <button className="primary-button" type="button" onClick={onCreateRoom} disabled={!selectedFile}>
          <Send size={18} />
          Create room
        </button>
        <span className="limit-note">MVP file cap: {formatBytes(MAX_FILE_SIZE)}</span>
      </div>

      {roomLink ? (
        <div className="invite-box">
          <div>
            <p className="eyebrow">Room {roomId}</p>
            <strong>{roomLink}</strong>
          </div>
          <button className="icon-button" type="button" onClick={onCopy} aria-label="Copy invite link">
            <Copy size={18} />
            {copyLabel}
          </button>
        </div>
      ) : null}
    </div>
  );
}

function ReceiverPanel({ joinInput, setJoinInput, roomId, expectedFile, downloadedFile, onJoin }) {
  return (
    <div className="transfer-panel">
      <form
        className="join-form"
        onSubmit={(event) => {
          event.preventDefault();
          onJoin(joinInput);
        }}
      >
        <label htmlFor="roomId">Room ID</label>
        <div className="join-row">
          <input
            id="roomId"
            value={joinInput}
            onChange={(event) => setJoinInput(event.target.value.toUpperCase())}
            placeholder="ABC123"
            autoComplete="off"
          />
          <button className="primary-button" type="submit">
            <Link size={18} />
            Join
          </button>
        </div>
      </form>

      <div className="receiver-state">
        <Download size={28} />
        <div>
          <p className="eyebrow">{roomId ? `Room ${roomId}` : "No room joined"}</p>
          <strong>{expectedFile?.name || downloadedFile?.name || "Waiting for file metadata"}</strong>
          <span>
            {downloadedFile
              ? `${formatBytes(downloadedFile.size)} downloaded`
              : expectedFile?.size
                ? `${formatBytes(expectedFile.size)} expected`
                : "The download starts automatically after verification"}
          </span>
        </div>
      </div>
    </div>
  );
}

function ProgressMeter({ transfer }) {
  const percentLabel = `${transfer.percent.toFixed(transfer.percent >= 10 ? 0 : 1)}%`;

  return (
    <div className="progress-card">
      <div className="progress-header">
        <strong>{transfer.fileName || "No active file"}</strong>
        <span>{transfer.status}</span>
      </div>
      <div className="progress-track" aria-label="Transfer progress">
        <div className="progress-fill" style={{ width: `${transfer.percent}%` }} />
      </div>
      <div className="stats-grid">
        <span>Progress</span>
        <strong>{percentLabel}</strong>
        <span>Transferred</span>
        <strong>
          {formatBytes(transfer.bytes)} / {formatBytes(transfer.totalBytes)}
        </strong>
        <span>Speed</span>
        <strong>{formatSpeed(transfer.speed)}</strong>
        <span>Peer received</span>
        <strong>{formatBytes(transfer.peerBytes)}</strong>
      </div>
      {transfer.status === "Failed" || transfer.status === "Failed verification" ? (
        <p className="failure-line">
          <XCircle size={15} />
          Transfer stopped
        </p>
      ) : null}
    </div>
  );
}
