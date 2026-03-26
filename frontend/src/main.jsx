import React from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }
  componentDidCatch(error, info) {
    console.error('APP CRASHED:', error)
    console.error('INFO:', info)
    this.setState({ error: error.toString() })
  }
  render() {
    if (this.state.error) {
      return (
        <div style={{ color: 'red', padding: '40px', fontSize: '16px', background: 'white' }}>
          <h2>App crashed — error:</h2>
          <pre style={{ whiteSpace: 'pre-wrap' }}>{this.state.error}</pre>
        </div>
      )
    }
    return this.props.children
  }
}

createRoot(document.getElementById('root')).render(
  <ErrorBoundary>
    <App />
  </ErrorBoundary>
)