const socket = io("http://localhost:3000");
const localVideo = document.getElementById("localVideo");
let localStream;
let peerConnection;

socket.on("connect", () => {
  console.log("Connected to Socket.Io server with ID: ", socket.id);
  document.title = socket.id.slice(-4);
  loadLocalStream();
});

async function loadLocalStream() {
  try {
    localStream = await navigator.mediaDevices.getUserMedia({
      video: true,
      audio: true,
    });
    localVideo.srcObject = localStream;
    await createPeerConnection();
    if (socket.connected) {
      await createOffer();
    } else {
      console.log("Socket not connected");
    }

    console.log("Local stream loaded successfully");
  } catch (error) {
    console.log("Error loading local stream: ", error);
  }
}
async function createOffer() {
  if (peerConnection.signalingState !== "stable") {
    console.warn(
      "Cannot create offer. Signaling state: ",
      peerConnection.signlingState
    );
    return;
  }

  const offer = await peerConnection.createOffer();

  await peerConnection.setLocalDescription(offer);
  console.log("offer sdp: ", offer);

  socket.emit("offer", { offer });
}
const configuration = {
  iceServers: [
    {
      urls: "stun:stun.l.google.com:19302",
    },
  ],
};
async function createPeerConnection() {
  peerConnection = new RTCPeerConnection(configuration);

  peerConnection.ontrack = (event) => {
    const stream = event.streams[0];
    const streamId = stream.id;
    const remoteVideoId = `video-${streamId}`;

    console.log(
      `Remote stream received: Stream ID = ${streamId} track kind=${event.track.kind}`
    );

    if (
      !stream ||
      stream.getTracks().length === 0 ||
      !stream.getVideoTracks().some((track) => track.enabled)
    ) {
      console.warn(`Stream ${streamId} has no valid video tracks. Skipping.`);
      return;
    }

    let remoteVideo = document.getElementById(remoteVideoId);

    if (!remoteVideo) {
      remoteVideo = document.createElement("video");
      remoteVideo.id = remoteVideoId;
      remoteVideo.srcObject = stream;
      remoteVideo.autoplay = true;
      remoteVideo.playsInline = true;

      document.getElementById("remoteVideos").appendChild(remoteVideo);
    } else {
      console.warn(`Video element already exists for stream id: ${streamId}`);
    }
  };
  peerConnection.onicecandidate = (event) => {
    if (event.candidate) {
      console.log("New ICE Candidate gathered: ", event.candidate);
      socket.emit("ice-candidate", event.candidate);
    }
  };
  localStream.getTracks().forEach((track) => {
    peerConnection.addTrack(track, localStream);
    console.log("Adding local Stream track: ", track);
  });
}

socket.on("answer", async (data) => {
  if (peerConnection.signalingState === "have-local-offer") {
    try {
      await peerConnection.setRemoteDescription(
        new RTCSessionDescription(data.answer)
      );
      console.log("Answer applied successfully.");
    } catch (error) {
      console.warn("Failed to apply remote description");
    }
  } else {
    console.warn("Unexpected signaling state: ", peerConnection.signalingState);
  }
});

socket.on("ice-candidate", async (data) => {
  console.log("ICE candidate received from server: ", data);
  try {
    const candidate = new RTCIceCandidate({
      candidate: data.candidate,
      sdpMid: data.sdpMid,
      sdpMLineIndex: data.sdpMLineIndex,
      usernameFragment: data.usernameFragment,
    });

    await peerConnection.addIceCandidate(candidate);
    console.log("ICE candidate added successfully");
  } catch (error) {
    console.error("Error adding ICE Candidate");
  }
});

socket.on("renegotiation-offer", async (data) => {
  if (peerConnection.signalingState === "stable") {
    try {
      await peerConnection.setRemoteDescription(
        new RTCSessionDescription(data.offer)
      );
      const answer = await peerConnection.createAnswer();
      await peerConnection.setLocalDescription(answer);

      socket.emit("renegotiation-answer", { answer });
      console.log("Renegotiation completed successfully");
    } catch (error) {
      console.error("Error handling renegotiation: ", error);
    }
  }
});

socket.on("user-disconnected", (data) => {
  const streamId = data.streamId;
  console.log("Disconnected user stream id: ", streamId);

  const remoteVideo = document.getElementById(`video-${streamId}`);

  if (remoteVideo) {
    remoteVideo.srcObject = null;
    remoteVideo.remove();
    console.log("Removed video elemenent for stream ID: ", streamId);
  } else {
    console.warn("No video element found for Stream ID: ", streamId);
  }
});
