import React, { useState, useEffect, useCallback, useRef } from 'react';
import GraphCanvas from './components/GraphCanvas';
import ChatPanel from './components/ChatPanel';
import NodeDetail from './components/NodeDetail';
import StatsBar from './components/StatsBar';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
const MAX_INITIAL_NODES = 150; // more aggressive limit

function limitGraph(data, maxNodes) {
  if (!data || !data.nodes) return data;
  if (data.nodes.length <= maxNodes) return data;
  const limitedNodes = data.nodes.slice(0, maxNodes);
  const idSet = new Set(limitedNodes.map(n => n.id));
  const limitedLinks = data.links.filter(l => {
    const src = typeof l.source === 'object' ? l.source.id : l.source;
    const tgt = typeof l.target === 'object' ? l.target.id : l.target;
    return idSet.has(src) && idSet.has(tgt);
  });
  return { nodes: limitedNodes, links: limitedLinks };
}

// 🔑 Error boundary catches crashes in any child component
class ErrorBoundary extends React.Component {
  state = { error: null };
  static getDerivedStateFromError(e) { return { error: e }; }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 16, color: 'red', fontSize: 12 }}>
          <strong>Component crashed:</strong>
          <pre>{this.state.error.message}</pre>
        </div>
      );
    }
    return this.props.children;
  }
}

export default function App() {
  const [graphData, setGraphData] = useState(null);
  const [stats, setStats] = useState(null);
  const [selectedNode, setSelectedNode] = useState(null);
  const [highlightedNodeIds, setHighlightedNodeIds] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const fullGraphRef = useRef(null);

  useEffect(() => {
    const loadData = async () => {
      try {
        const [graphRes, statsRes] = await Promise.all([
          fetch(`${API_URL}/api/graph`),
          fetch(`${API_URL}/api/stats`),
        ]);

        if (!graphRes.ok) throw new Error(`Graph fetch failed: ${graphRes.status}`);

        const data = await graphRes.json();
        console.log('Graph loaded:', data.nodes?.length, 'nodes', data.links?.length, 'links');
        fullGraphRef.current = data;

        // Defer to let React paint the loading state first
        setTimeout(() => {
          setGraphData(limitGraph(data, MAX_INITIAL_NODES));
        }, 0);

        if (statsRes.ok) {
          setStats(await statsRes.json());
        }
      } catch (err) {
        console.error('Failed to load data:', err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };
    loadData();
  }, []);

  const handleNodeClick = useCallback((node) => {
    setSelectedNode(prev => prev?.id === node.id ? null : node);
  }, []);

  const handleHighlight = useCallback((nodeIds) => {
    setHighlightedNodeIds(nodeIds);
  }, []);

  // 🔑 These guards ensure SOMETHING renders at every state
  if (loading) return <div style={{ padding: 32, color: '#666' }}>Loading graph data...</div>;
  if (error) return <div style={{ padding: 32, color: 'red' }}>Error: {error}</div>;

  return (
    <div className="app-container">
      <ErrorBoundary>
        <StatsBar stats={stats} />
      </ErrorBoundary>

      <div className="main-content">
        <ErrorBoundary>
          {/* 🔑 Only mount GraphCanvas once graphData is ready */}
          {graphData
            ? <GraphCanvas
              graphData={graphData}
              highlightedNodeIds={highlightedNodeIds}
              onNodeClick={handleNodeClick}
              selectedNode={selectedNode}
            />
            : <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              Preparing graph...
            </div>
          }
        </ErrorBoundary>

        <ErrorBoundary>
          <NodeDetail node={selectedNode} onClose={() => setSelectedNode(null)} />
        </ErrorBoundary>

        <ErrorBoundary>
          <ChatPanel onHighlight={handleHighlight} />
        </ErrorBoundary>
      </div>
    </div>
  );
}