// https://bsky.bad-example.com/consuming-the-firehose-cheaply/
const jetstreams = [
  "jetstream1.us-east.bsky.network",
  "jetstream2.us-east.bsky.network",
  "jetstream1.us-west.bsky.network",
  "jetstream2.us-west.bsky.network",
];

const button = document.querySelector('.feed-me');
const theWord = document.querySelector('.the-word');

let ws;

button.addEventListener('click', toggleConnect);

function toggleConnect() {
  if (!ws) connect();
  else if (ws.readyState === ws.CONNECTING) disconnect();
  else if (ws.readyState === ws.OPEN) disconnect();
  else if (ws.readyState === ws.CLOSING) {} // do nothing
  else if (ws.readyState === ws.CLOSED) connect();
  else console.error('wat');
}

function connect() {
  const stream = jetstreams[Math.floor(Math.random() * jetstreams.length)];
  const wsUrl = 'wss://' + stream + '/subscribe?wantedCollections=app.bsky.feed.post';
  ws = new WebSocket(wsUrl);
  ws.onopen = connected;
  ws.onclose = disconnected;
  ws.onerror = e => { ws.close(); console.error(e) };
  ws.onmessage = handleMessage;

  theWord.textContent = 'connecting...';
  setTimeout(disconnect, 15000);
}

function disconnect() {
  button.textContent = 'Disconnecting...';
  ws.close();
}

function connected() {
  button.textContent = 'Disconnect';
}

function disconnected() {
  button.textContent = 'Connect firehose';
  theWord.textContent = '';
}

function handleMessage(event) {
  if (!event.data) return;
  let data;
  try {
    data = JSON.parse(event.data);
  } catch (e) { return; }
  if (!(
    data && data.kind === "commit" &&
    data.commit.operation === "create" &&
    data.commit.record && data.commit.record.text
  )) return;
  const words = data.commit.record.text.split(' ');
  const word = words[Math.floor(Math.random() * words.length)];
  theWord.textContent = word.slice(0, 21);
}
