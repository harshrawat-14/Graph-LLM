import { useRef, useEffect, useCallback, useMemo } from 'react';
import ForceGraph2D from 'react-force-graph-2d';

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

const NODE_ICONS = {
  Customer: '👤',
  Order: '📋',
  OrderItem: '📦',
  Product: '🏷️',
  Delivery: '🚚',
  Invoice: '🧾',
  Payment: '💰',
  JournalEntry: '📒',
  BrokenFlow: '⚠️',
};

const EDGE_COLORS = {
  PLACED: '#3b82f6',
  CONTAINS: '#22c55e',
  FOR_PRODUCT: '#8b5cf6',
  FULFILLED_BY: '#f97316',
  BILLED_AS: '#eab308',
  PAID_BY: '#14b8a6',
  JOURNAL_ENTRY: '#6366f1',
};


export default function GraphCanvas({
  graphData,
  highlightedNodeIds,
  onNodeClick,
  selectedNode,
}) {
  const fgRef = useRef();
  const highlightSet = useMemo(
    () => new Set(highlightedNodeIds || []),
    [highlightedNodeIds]
  );

  // Auto-zoom to highlighted nodes
  useEffect(() => {
    if (!fgRef.current || !highlightedNodeIds?.length || !graphData?.nodes?.length) return;

    const timeout = setTimeout(() => {
      const highlighted = graphData.nodes.filter(n => highlightSet.has(n.id));
      if (highlighted.length === 0) return;

      if (highlighted.length === 1) {
        fgRef.current.centerAt(highlighted[0].x, highlighted[0].y, 300);
        fgRef.current.zoom(4, 300);
      } else {
        fgRef.current.zoomToFit(300, 60, n => highlightSet.has(n.id));
      }
    }, 200);

    return () => clearTimeout(timeout);
  }, [highlightedNodeIds, graphData, highlightSet]);

  const nodeCanvasObject = useCallback((node, ctx, globalScale) => {
    const isHighlighted = highlightSet.has(node.id);
    const isSelected = selectedNode?.id === node.id;
    const isBroken = node.broken || node.type === 'BrokenFlow';

    const baseSize = node.type === 'Customer' ? 7 :
                     node.type === 'Order' ? 6 :
                     node.type === 'BrokenFlow' ? 5 : 4.5;

    const size = isHighlighted ? baseSize * 1.4 : baseSize;
    const color = NODE_COLORS[node.type] || '#9ca3af';

    // Glow effect for highlighted nodes
    if (isHighlighted) {
      ctx.beginPath();
      ctx.arc(node.x, node.y, size + 4, 0, 2 * Math.PI);
      ctx.fillStyle = color + '30';
      ctx.fill();

      ctx.beginPath();
      ctx.arc(node.x, node.y, size + 2, 0, 2 * Math.PI);
      ctx.fillStyle = color + '50';
      ctx.fill();
    }

    // Selection ring
    if (isSelected) {
      ctx.beginPath();
      ctx.arc(node.x, node.y, size + 3, 0, 2 * Math.PI);
      ctx.strokeStyle = '#ffffff';
      ctx.lineWidth = 2;
      ctx.stroke();
    }

    // Main circle
    ctx.beginPath();
    ctx.arc(node.x, node.y, size, 0, 2 * Math.PI);
    ctx.fillStyle = color;
    ctx.fill();

    // Broken flow dashed border
    if (isBroken) {
      ctx.setLineDash([2, 2]);
      ctx.strokeStyle = '#ef4444';
      ctx.lineWidth = 1.5;
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // Label (only show at reasonable zoom)
    if (globalScale > 1.2) {
      const label = node.label?.length > 20 ? node.label.slice(0, 18) + '…' : node.label;
      const fontSize = Math.max(10 / globalScale, 2.5);
      ctx.font = `${fontSize}px Inter, sans-serif`;
      ctx.fillStyle = isHighlighted ? '#ffffff' : '#c0c4d0';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';
      ctx.fillText(label || '', node.x, node.y + size + 2);
    }
  }, [highlightSet, selectedNode]);

  const linkCanvasObject = useCallback((link, ctx) => {
    const color = link.broken ? '#ef444480' :
                  EDGE_COLORS[link.relation] || '#4b5563';

    ctx.beginPath();
    ctx.moveTo(link.source.x, link.source.y);
    ctx.lineTo(link.target.x, link.target.y);
    ctx.strokeStyle = color;
    ctx.lineWidth = link.broken ? 0.5 : 0.8;

    if (link.broken) {
      ctx.setLineDash([3, 3]);
    }
    ctx.stroke();
    ctx.setLineDash([]);
  }, []);

  if (!graphData || !graphData.nodes?.length) {
    return (
      <div className="graph-panel">
        <div className="graph-loading">
          <div className="spinner" />
          <span>Loading graph…</span>
        </div>
      </div>
    );
  }

  return (
    <div className="graph-panel">
      <ForceGraph2D
        ref={fgRef}
        graphData={graphData}
        nodeCanvasObject={nodeCanvasObject}
        linkCanvasObjectMode={() => 'replace'}
        linkCanvasObject={linkCanvasObject}
        onNodeClick={onNodeClick}
        nodeId="id"
        linkSource="source"
        linkTarget="target"
        backgroundColor="#0f1117"
        warmupTicks={50}
        cooldownTicks={60}
        d3AlphaDecay={0.05}
        d3VelocityDecay={0.4}
        enableNodeDrag={true}
        enableZoomInteraction={true}
        minZoom={0.5}
        maxZoom={20}
      />

      <div className="graph-legend">
        {Object.entries(NODE_COLORS).map(([type, color]) => (
          <div key={type} className="legend-item">
            <div className="legend-dot" style={{ backgroundColor: color }} />
            <span>{type}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
