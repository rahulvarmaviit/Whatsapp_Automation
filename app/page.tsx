"use client";

import { KeyboardEvent, useEffect, useRef, useState } from "react";
import styles from "./page.module.css";

type PendingTransaction = {
  txnCode: string;
  kind: string;
  amount: number;
  vendorName: string;
  createdBy: string;
};

type ChatMessage = {
  role: "user" | "assistant";
  text: string;
  timestamp: string;
  read?: boolean;
  pendingTransactions?: PendingTransaction[];
};

type ChatApiResponse = {
  reply?: string;
  error?: string;
  requiresConfirmation?: boolean;
  pendingTransactions?: PendingTransaction[];
};

function createSessionId() {
  return `session_${Math.random().toString(36).slice(2)}_${Date.now()}`;
}

function getCurrentTimeLabel(): string {
  return new Date().toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  });
}

export default function Home() {
  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      role: "assistant",
      text: "Welcome! 👋 I'm your Accounts Automation Agent. Let me help you manage transactions.",
      timestamp: "11:47 PM",
      read: true,
    },
    {
      role: "assistant",
      text: "Start by logging in with your credentials.",
      timestamp: "11:48 PM",
      read: true,
    },
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [showConfirmationActions, setShowConfirmationActions] = useState(false);
  const [sessionId] = useState(() => createSessionId());
  const chatFeedRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!chatFeedRef.current) {
      return;
    }

    const frame = window.requestAnimationFrame(() => {
      chatFeedRef.current?.scrollTo({
        top: chatFeedRef.current.scrollHeight,
        behavior: "smooth",
      });
    });

    return () => window.cancelAnimationFrame(frame);
  }, [messages]);

  function getReadReceipt(message: ChatMessage): string {
    if (message.role === "assistant") {
      return "";
    }

    return "✓✓";
  }

  async function sendMessage(text: string) {
    const trimmed = text.trim();
    if (!trimmed) {
      return;
    }

    const userMsg: ChatMessage = {
      role: "user",
      text: trimmed,
      timestamp: getCurrentTimeLabel(),
      read: false,
    };

    setMessages((prev) => [...prev, userMsg]);
    setInput("");
    setLoading(true);

    try {
      const response = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: trimmed, sessionId }),
      });

      let json: ChatApiResponse = {};

      try {
        json = (await response.json()) as ChatApiResponse;
      } catch {
        json = {};
      }

      const reply =
        json.reply ||
        json.error ||
        (response.ok ? "Done." : `Request failed (${response.status}).`);

      const assistantMsg: ChatMessage = {
        role: "assistant",
        text: reply,
        timestamp: getCurrentTimeLabel(),
        read: true,
        pendingTransactions: Array.isArray(json.pendingTransactions)
          ? json.pendingTransactions
          : undefined,
      };

      setMessages((prev) => [...prev, assistantMsg]);
      setShowConfirmationActions(Boolean(json.requiresConfirmation));
    } catch {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          text: "Server error. Please try again.",
          timestamp: getCurrentTimeLabel(),
          read: true,
        },
      ]);
      setShowConfirmationActions(false);
    } finally {
      setLoading(false);
    }
  }

  async function handleSendClick() {
    await sendMessage(input);
  }

  async function onInputKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key !== "Enter") {
      return;
    }

    event.preventDefault();
    await sendMessage(input);
  }

  return (
    <div className={styles.container}>
      <header className={styles.whatsappHeader}>
        <div className={styles.headerLeft}>
          <button className={styles.backButton}>←</button>
          <div className={styles.profileMetaContainer}>
            <h2 className={styles.contactName}>Accounts Agent</h2>
            <p className={styles.statusText}>Online</p>
          </div>
        </div>
        <div className={styles.headerRight}>
          <button className={styles.headerIcon}>📷</button>
          <button className={styles.headerIcon}>☎️</button>
          <button className={styles.headerIcon}>⋯</button>
        </div>
      </header>

      <div className={styles.chatContainer}>
        <div className={styles.messagesWrapper} ref={chatFeedRef}>
          {messages.map((message, index) => (
            <div
              key={`${message.role}-${index}-${message.timestamp}`}
              className={`${styles.messageRow} ${
                message.role === "user" ? styles.userRow : styles.assistantRow
              }`}
            >
              <div
                className={`${styles.messageBubble} ${
                  message.role === "user" ? styles.sentBubble : styles.receivedBubble
                }`}
              >
                <p className={styles.messageText}>{message.text}</p>

                {message.role === "assistant" &&
                  Array.isArray(message.pendingTransactions) &&
                  message.pendingTransactions.length > 0 && (
                    <div className={styles.pendingTransactionList}>
                      {message.pendingTransactions.map((transaction) => (
                        <div
                          key={transaction.txnCode}
                          className={styles.pendingTransactionCard}
                        >
                          <p className={styles.pendingTransactionTitle}>
                            ID: {transaction.txnCode}
                          </p>
                          <p className={styles.pendingTransactionMeta}>
                            {transaction.kind} | Rs.{transaction.amount} | {transaction.vendorName || "N/A"}
                          </p>
                          <p className={styles.pendingTransactionMeta}>
                            Created by: {transaction.createdBy || "N/A"}
                          </p>
                          <div className={styles.inlineActionRow}>
                            <button
                              type="button"
                              disabled={loading}
                              onClick={() => sendMessage(`approve ${transaction.txnCode}`)}
                              className={`${styles.inlineActionButton} ${styles.inlineApproveButton}`}
                            >
                              Approve
                            </button>
                            <button
                              type="button"
                              disabled={loading}
                              onClick={() => sendMessage(`reject ${transaction.txnCode}`)}
                              className={`${styles.inlineActionButton} ${styles.inlineRejectButton}`}
                            >
                              Decline
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}

                <div className={styles.messageFooter}>
                  <span className={styles.timestamp}>{message.timestamp}</span>
                  {message.role === "user" && (
                    <span className={styles.readReceipt}>{getReadReceipt(message)}</span>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>

        <div className={styles.inputBar}>
          <button type="button" className={styles.iconButton} title="Emoji">
            😊
          </button>
          <button type="button" className={styles.iconButton} title="Attachment">
            📎
          </button>
          <button type="button" className={styles.iconButton} title="Camera">
            📷
          </button>
          <input
            type="text"
            className={styles.messageInput}
            placeholder="Message"
            value={input}
            onChange={(event) => setInput(event.target.value)}
            onKeyDown={onInputKeyDown}
            disabled={loading}
          />
          <button
            type="button"
            className={styles.sendButton}
            disabled={loading}
            title="Send"
            onClick={handleSendClick}
          >
            <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
              <path d="M2.01 21L23 12L2.01 3L2 10L17 12L2 14L2.01 21Z" />
            </svg>
          </button>
        </div>

        {showConfirmationActions && (
          <div className={styles.actionButtons}>
            <button
              type="button"
              onClick={() => sendMessage("CONFIRM")}
              disabled={loading}
              className={`${styles.actionBtn} ${styles.confirmBtn}`}
            >
              ✓ Confirm
            </button>
            <button
              type="button"
              onClick={() => sendMessage("CANCEL")}
              disabled={loading}
              className={`${styles.actionBtn} ${styles.cancelBtn}`}
            >
              ✕ Cancel
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
