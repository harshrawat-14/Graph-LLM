import { useState, useRef, useEffect } from 'react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const EXAMPLE_QUERIES = [
  'Which products appear in the most invoices?',
  'Show all incomplete order flows',
  'Trace the flow of order 740506',
  'How many orders does each customer have?',
  'Show all unpaid invoices',
];

function ConfidenceBadge({ confidence }) {
  const badges = {
    data_backed: { icon: '✓', label: 'Data-backed', cls: 'data_backed' },
    no_data: { icon: '⚠', label: 'No data found', cls: 'no_data' },
    rejected: { icon: '✗', label: 'Off-domain', cls: 'rejected' },
    error: { icon: '✗', label: 'Error', cls: 'error' },
  };
  const b = badges[confidence] || badges.error;
  return (
    <span className={`confidence-badge ${b.cls}`}>
      {b.icon} {b.label}
    </span>
  );
}

function Collapsible({ title, children }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="collapsible">
      <button className="collapsible-toggle" onClick={() => setOpen(!open)}>
        <span style={{ fontSize: 10, transition: 'transform 0.2s', transform: open ? 'rotate(90deg)' : 'none' }}>▶</span>
        {title}
      </button>
      {open && <div className="collapsible-content">{children}</div>}
    </div>
  );
}

function DataTable({ data }) {
  if (!data || data.length === 0) return null;
  const cols = Object.keys(data[0]);
  return (
    <table className="data-table">
      <thead>
        <tr>{cols.map(c => <th key={c}>{c}</th>)}</tr>
      </thead>
      <tbody>
        {data.slice(0, 20).map((row, i) => (
          <tr key={i}>
            {cols.map(c => <td key={c} title={String(row[c] ?? '')}>{String(row[c] ?? '')}</td>)}
          </tr>
        ))}
      </tbody>
      {data.length > 20 && (
        <tfoot>
          <tr><td colSpan={cols.length} style={{ textAlign: 'center', color: '#6b7280', fontSize: 11 }}>
            …and {data.length - 20} more rows
          </td></tr>
        </tfoot>
      )}
    </table>
  );
}

export default function ChatPanel({ onHighlight }) {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef(null);
  const inputRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const sendQuery = async (question) => {
    if (!question.trim()) return;

    const userMsg = { role: 'user', content: question };
    setMessages(prev => [...prev, userMsg]);
    setInput('');
    setLoading(true);

    try {
      const res = await fetch(`${API_URL}/api/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question, session_id: 'default' }),
      });

      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const data = await res.json();

      const assistantMsg = {
        role: 'assistant',
        content: data.answer,
        sql: data.sql_used,
        data: data.data,
        confidence: data.confidence,
        intent: data.intent,
        highlighted: data.highlighted_node_ids,
      };

      setMessages(prev => [...prev, assistantMsg]);

      // Highlight nodes in graph
      if (data.highlighted_node_ids?.length) {
        onHighlight(data.highlighted_node_ids);
      }
    } catch (err) {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: `Connection error: ${err.message}. Make sure the backend is running at ${API_URL}.`,
        confidence: 'error',
      }]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendQuery(input);
    }
  };

  return (
    <div className="chat-panel">
      <div className="chat-header">
        <span>💬</span> Query Engine
      </div>

      <div className="example-chips">
        {EXAMPLE_QUERIES.map((q, i) => (
          <button
            key={i}
            className="example-chip"
            onClick={() => sendQuery(q)}
            disabled={loading}
          >
            {q.length > 38 ? q.slice(0, 36) + '…' : q}
          </button>
        ))}
      </div>

      <div className="chat-messages">
        {messages.length === 0 && (
          <div style={{ textAlign: 'center', color: '#6b7280', marginTop: 60, fontSize: 14, lineHeight: 2 }}>
            <div style={{ fontSize: 40, marginBottom: 12 }}>🔍</div>
            Ask a question about your SAP Order-to-Cash data.<br />
            Try clicking one of the example queries above.
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} className={`message ${msg.role}`}>
            <div className="message-bubble">
              {msg.content}
            </div>

            {msg.role === 'assistant' && (
              <div className="message-meta">
                {msg.confidence && <ConfidenceBadge confidence={msg.confidence} />}
                {msg.intent && (
                  <span style={{ fontSize: 11, color: '#6b7280' }}>
                    {msg.intent}
                  </span>
                )}
              </div>
            )}

            {msg.sql && (
              <Collapsible title="SQL Query">
                <div className="sql-display">{msg.sql}</div>
              </Collapsible>
            )}

            {msg.data?.length > 0 && (
              <Collapsible title={`Data (${msg.data.length} rows)`}>
                <DataTable data={msg.data} />
              </Collapsible>
            )}
          </div>
        ))}

        {loading && (
          <div className="message assistant">
            <div className="message-bubble">
              <div className="typing-indicator">
                <div className="dot" />
                <div className="dot" />
                <div className="dot" />
              </div>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      <div className="chat-input-area">
        <div className="chat-input-wrapper">
          <textarea
            ref={inputRef}
            className="chat-input"
            rows={1}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about orders, invoices, payments…"
            disabled={loading}
          />
          <button
            className="send-btn"
            onClick={() => sendQuery(input)}
            disabled={loading || !input.trim()}
          >
            ↑
          </button>
        </div>
      </div>
    </div>
  );
}
