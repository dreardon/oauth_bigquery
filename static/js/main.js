document.addEventListener("DOMContentLoaded", function () {
  messageInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      sendButton.click();
    }
  });
});

const messagesContainer = document.getElementById("chat-history");
const messageInput = document.getElementById("message-input");
const sendButton = document.getElementById("send-button");

sendButton.addEventListener("click", () => {
  const userMessage = messageInput.value;
  displayMessage(userMessage, "user");
  messageInput.value = "";

  fetch("/chat", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ message: userMessage }),
  })
    .then((response) => response.text())
    .then((response) => {
      displayMessage(response, "bot");
      console.log("response:" + response);
    });
});

function parseChatResponseFormatSQL(text) {
  const sqlRegex = /```sql([\s\S]*?)```/g;
  const sql = "test"
  var htmlFormatted = text.replace(
    sqlRegex,
    '<div class="sql-response">$1</div><button class="sql-execute-btn" onclick="executeSQL(event)"><i class="fas fa-play"></i> Execute SQL</button>'
  );
  return `<div class="chat-response">${htmlFormatted}</div>`;
}

function executeSQL(event) {
  var sql = event.target.previousElementSibling.textContent
  fetch("/results", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ command: sql }),
  })
    .then((response) => response.text())
    .then((response) => {
        document.getElementById('results').innerHTML = response;
      });
  }

function displayMessage(message, type) {
  const messageDiv = document.createElement("div");
  messageDiv.classList.add("message", type);

  messageDiv.innerHTML += parseChatResponseFormatSQL(message);
  messagesContainer.appendChild(messageDiv);
  messagesContainer.scrollTop = messagesContainer.scrollHeight;
}
