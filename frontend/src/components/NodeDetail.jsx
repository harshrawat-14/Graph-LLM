const NODE_COLORS = {
  Customer: '#3b82f6',
  Order: '#22c55e',
  OrderItem: '#a855f7',
  Product: '#8b5cf6',
  Delivery: '#f97316',
  Invoice: '#eab308',
  Payment: '#14b8a6',
  JournalEntry: '#6366f1',
  BrokenFlow: '#ef4444',
};

export default function NodeDetail({ node, onClose }) {
  if (!node) return null;

  const color = NODE_COLORS[node.type] || '#9ca3af';
  const meta = node.metadata || {};

  // Filter out internal fields
  const displayFields = Object.entries(meta).filter(
    ([key]) => !['raw_source'].includes(key) && meta[key] !== null && meta[key] !== ''
  );

  return (
    <div className="node-detail-overlay">
      <div className="node-detail-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span
            className="node-type-badge"
            style={{ background: color + '25', color: color }}
          >
            {node.type}
          </span>
          <span style={{ fontSize: 14, fontWeight: 600 }}>
            {node.label}
          </span>
        </div>
        <button className="close-btn" onClick={onClose}>×</button>
      </div>

      <div className="node-detail-body">
        <div className="field">
          <span className="field-key">Node ID</span>
          <span className="field-value">{node.id}</span>
        </div>

        {node.broken && (
          <div className="field" style={{ color: '#ef4444' }}>
            <span className="field-key">⚠ Status</span>
            <span className="field-value" style={{ color: '#ef4444' }}>Broken Flow</span>
          </div>
        )}

        {displayFields.map(([key, value]) => (
          <div className="field" key={key}>
            <span className="field-key">{key}</span>
            <span className="field-value" title={String(value)}>
              {typeof value === 'object' ? JSON.stringify(value) : String(value)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
