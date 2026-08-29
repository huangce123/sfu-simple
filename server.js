const { trace } = require("console");
const express = require("express");

const http = require("http");
const app = express();
const { Server } = require("socket.io");
const server = http.createServer(app);
const { RTCPeerConnection, MediaStream } = require("wrtc");
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

const PORT = 3000;
const clients = {};

async function handlePeerConnection(socketId, offer) {
  const peerConnection = new RTCPeerConnection();

  peerConnection.ontrack = (event) => {
    console.log("ontrack event for socket id: ", socketId);

    const [stream] = event.streams;
    if (
      stream &&
      stream
        .getTracks()
        .some((track) => track.kind === "video" && track.readyState === "live")
    ) {
      stream.getTracks().forEach((track) => {
        forwardTrckToOthers(socketId, track, stream);
      });
    } else {
      console.warn(
        `Stream ${
          stream?.id || "undefined"
        } does not have a valid track. Skipping`
      );
    }
  };
  peerConnection.onnegotiationneeded = async () => {
    if (!clients[socketId].isRenegotiating) {
      clients[socketId].isRenegotiating = true;

      try {
        const offer = await peerConnection.createOffer();
        await peerConnection.setLocalDescription(offer);
        clients[socketId].socket.emit("renegotiation-offer", { offer });
      } catch (error) {
        console.error("Error during renegotiation: ", error);
      } finally {
        clients[socketId].isRenegotiating = false;
      }
    }
  };
  peerConnection.onicecandidate = (event) => {
    if (event.candidate) {
      clients[socketId].socket.emit("ice-candidate", {
        candidate: event.candidate.candidate,
        sdpMid: event.candidate.sdpMid,
        sdpMLineIndex: event.candidate.sdpMLineIndex,
        usernameFragment: event.candidate.usernameFragment,
      });
      console.log("ICE candidate sent to client : ", socketId);
    }
  };
  await peerConnection.setRemoteDescription(offer);

  const answer = await peerConnection.createAnswer();
  await peerConnection.setLocalDescription(answer);
  return { peerConnection, answer };
}

async function forwardTrckToOthers(sourcSocketId, track, stream) {
  if (track.readyState !== "live") {
    console.warn(
      `Track ${track.id} from stream ID ${stream.id} is not live ${track.readyState}.Skipping`
    );
    return;
  }

  const hasValidVideoTrack = stream
    .getVideoTracks()
    .some((track) => track.enabled);
  if (
    hasValidVideoTrack &&
    !clients[sourcSocketId].localStream.includes(stream.id)
  ) {
    clients[sourcSocketId].localStream.push(stream.id);
  }

  Object.keys(clients).forEach((destinationSocketId) => {
    if (
      destinationSocketId !== sourcSocketId &&
      clients[destinationSocketId].peerConnection
    ) {
      const receiverConnection = clients[destinationSocketId].peerConnection;
      if (!clients[destinationSocketId].forwardedTracks) {
        clients[destinationSocketId].forwardedTracks = new Map();
      }
      const forwardedTracks = clients[destinationSocketId].forwardedTracks;

      const trackKey = `${track.id}|${stream.id}|${sourcSocketId}|${destinationSocketId}`;

      if (forwardedTracks.has(trackKey)) {
        console.warn(
          `Track ${track.id} already forwarded to receiver ${destinationSocketId}.,Skipped`
        );
        return;
      }

      const existingForwardedTrack = Array.from(forwardedTracks.keys()).find(
        (key) => {
          const [trackId, streamId, sourceId, destId] = key.split("|");
          return (
            sourceId === sourcSocketId &&
            destId === destinationSocketId &&
            forwardedTracks.get(key).kind === track.kind
          );
        }
      );
      if (existingForwardedTrack) {
        console.warn(
          `Track ${track.id}, Stream ID: ${stream.id} already forwarded from ${sourcSocketId} to ${destinationSocketId}`
        );
        return;
      }
      if (!hasValidVideoTrack) {
        console.warn(`Stream ${stream.id} has no valid video tracks. Skipping`);
        return;
      }

      try {
        if (!stream) {
          console.warn(`Stream is missing for track ${track.id}. Skipping`);
          return;
        }

        console.log(
          `Forwarding track ${track.id} ${track.kind} stream Id: ${stream.id} from ${sourcSocketId} to ${destinationSocketId}`
        );

        const existingSender = receiverConnection
          .getSenders()
          .find((sender) => sender.track && sender.track.id === track.id);
        if (existingSender) {
          console.warn(
            `Track ${track.id} is already exists in receiver connection. Skipping.`
          );
          return;
        }

        receiverConnection.addTrack(track, stream);
        forwardedTracks.set(trackKey, { kind: track.kind });
      } catch (error) {
        console.error(`Error forwarding track: `, error);
      }
    }
  });
}
io.on("connection", (socket) => {
  clients[socket.id] = {
    socket,
    peerConnection: null,
    isRenegotiating: false,
    localStream: [],
  };
  console.log("A user connected, ", clients);

  socket.on("offer", async (data) => {
    try {
      const { peerConnection, answer } = await handlePeerConnection(
        socket.id,
        data.offer
      );

      clients[socket.id].peerConnection = peerConnection;
      // console.log("answer: ", answer);
      socket.emit("answer", { answer });
      console.log("clients after adding offer sdP: ", clients);

      Object.keys(clients).forEach((existingSocketId) => {
        if (existingSocketId !== socket.id) {
          const existingClient = clients[existingSocketId];
          if (existingClient.peerConnection) {
            existingClient.peerConnection.getSenders().forEach((sender) => {
              const track = sender.track;
              if (track) {
                forwardTrckToOthers(
                  existingSocketId,
                  track,
                  new MediaStream([track])
                );
              }
            });
          }
        }
      });
    } catch (error) {
      console.error(`Failed to process offer for client ${socket.id}`);
    }
  });
  socket.on("ice-candidate", async (data) => {
    const client = clients[socket.id];
    if (client && client.peerConnection) {
      try {
        await client.peerConnection.addIceCandidate(data);
        console.log(`ICE candidate added for client: ${socket.id}`);
      } catch (error) {
        console.error(`Failed to add ICE candidate for client: ${socket.id}`);
      }
    }
  });

  socket.on("renegotiation-answer", async (data) => {
    console.log("Renegotiation answer for user: ", socket.id);
    const client = clients[socket.id];
    if (client && client.peerConnection) {
      try {
        await client.peerConnection.setRemoteDescription(data.answer);
      } catch (error) {
        console.error(
          "Failed to apply renegotiation answer for client: ",
          socket.id
        );
      }
    }
  });
  socket.on("disconnect", () => {
    console.log("A user disconected, ", socket.id);
    const client = clients[socket.id];
    if (client) {
      if (Array.isArray(client.localStream)) {
        client.localStream.forEach((streamId) => {
          socket.broadcast.emit("user-disconnected", [streamId]);
          console.log(
            `Disconnected stream Id: ${streamId}, socketId: ${socket.id}`
          );
        });
      }
    } else {
      console.error(
        "Expected client.localStream to be an array but got: ",
        client.localStream
      );
    }
    if (client.peerConnection) {
      client.peerConnection.getSenders().forEach((sender) => {
        if (sender.track) {
          sender.track.stop();
        }
      });
      client.peerConnection.close();
    }
    delete clients[socket.id];
  });
});

server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
