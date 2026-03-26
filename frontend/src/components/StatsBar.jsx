export default function StatsBar({ stats }) {
  if (!stats) return null;

  return (
    <div className="stats-bar">
      <span className="app-title">⊛ SAP O2C GRAPH</span>

      <div className="stat-chip">
        <span>👤</span>
        <span className="stat-value">{stats.customers || 0}</span> Customers
      </div>

      <div className="stat-chip">
        <span>📋</span>
        <span className="stat-value">{stats.orders || 0}</span> Orders
      </div>

      <div className="stat-chip">
        <span>🚚</span>
        <span className="stat-value">{stats.deliveries || 0}</span> Deliveries
      </div>

      <div className="stat-chip">
        <span>🧾</span>
        <span className="stat-value">{stats.invoices || 0}</span> Invoices
      </div>

      <div className="stat-chip">
        <span>💰</span>
        <span className="stat-value">{stats.payments || 0}</span> Payments
      </div>

      <div className="stat-chip">
        <span>🏷️</span>
        <span className="stat-value">{stats.products || 0}</span> Products
      </div>

      <div className="stat-chip broken">
        <span>⚠️</span>
        <span className="stat-value">{stats.broken_flow_nodes || 0}</span> Broken Flows
      </div>

      <div className="stat-chip">
        <span>🔗</span>
        <span className="stat-value">{stats.graph_nodes || 0}</span> Nodes
        <span className="stat-value">{stats.graph_links || 0}</span> Edges
      </div>
    </div>
  );
}
