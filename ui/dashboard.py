#!/usr/bin/env python3
"""
Ray Job Monitor Dashboard

A Streamlit dashboard for monitoring Ray jobs on Kubernetes, with features for:
- Submitting tokenization, embedding, and pipeline jobs
- Monitoring job progress and worker status
- Viewing live logs from head and worker nodes
- Demonstrating fault tolerance by killing worker pods

Usage:
    streamlit run ui/dashboard.py

Or use the launcher script:
    ./scripts/run_dashboard.sh
"""

import os
import time
from datetime import datetime
from typing import Dict, List

import streamlit as st

from ui.k8s_client import get_k8s_client

# Page configuration
st.set_page_config(
    page_title="Ray Job Monitor",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for better styling
st.markdown("""
<style>
    /* Main theme colors - dark purple/blue aesthetic */
    :root {
        --primary-color: #7c3aed;
        --secondary-color: #4f46e5;
        --success-color: #10b981;
        --warning-color: #f59e0b;
        --danger-color: #ef4444;
        --background-dark: #0f0f1a;
        --card-bg: #1a1a2e;
        --text-primary: #e2e8f0;
        --text-secondary: #94a3b8;
    }
    
    /* Status badges */
    .status-badge {
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        display: inline-block;
    }
    .status-running { background: linear-gradient(135deg, #3b82f6, #1d4ed8); color: white; }
    .status-succeeded { background: linear-gradient(135deg, #10b981, #059669); color: white; }
    .status-failed { background: linear-gradient(135deg, #ef4444, #dc2626); color: white; }
    .status-pending { background: linear-gradient(135deg, #f59e0b, #d97706); color: white; }
    .status-unknown { background: linear-gradient(135deg, #6b7280, #4b5563); color: white; }
    
    /* Worker cards */
    .worker-card {
        background: linear-gradient(145deg, #1e1e2e, #2a2a3e);
        border: 1px solid #3f3f5a;
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }
    .worker-card:hover {
        border-color: #7c3aed;
        box-shadow: 0 6px 12px rgba(124, 58, 237, 0.2);
    }
    
    /* Metrics cards */
    .metric-card {
        background: linear-gradient(145deg, #1a1a2e, #252538);
        border-radius: 16px;
        padding: 20px;
        text-align: center;
        border: 1px solid #2d2d44;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #7c3aed, #4f46e5);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-label {
        color: #94a3b8;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    /* Event log */
    .event-item {
        padding: 8px 12px;
        border-left: 3px solid #7c3aed;
        margin: 4px 0;
        background: rgba(124, 58, 237, 0.1);
        border-radius: 0 8px 8px 0;
        font-family: 'JetBrains Mono', monospace;
        font-size: 13px;
    }
    .event-time {
        color: #6b7280;
        font-size: 11px;
    }
    
    /* Log viewer */
    .log-container {
        background: #0d1117;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 12px;
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        font-size: 12px;
        line-height: 1.5;
        max-height: 400px;
        overflow-y: auto;
        color: #c9d1d9;
    }
    
    /* Hide Streamlit branding and top-right icons */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    [data-testid="stToolbar"] {visibility: hidden;}
    [data-testid="stDecoration"] {visibility: hidden;}
    
    /* Improve sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0f1a 0%, #1a1a2e 100%);
    }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Session State Initialization
# -----------------------------------------------------------------------------

def init_session_state():
    """Initialize session state variables."""
    if "k8s_client" not in st.session_state:
        kubeconfig = os.environ.get("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        st.session_state.k8s_client = get_k8s_client(kubeconfig=kubeconfig)
    
    if "events" not in st.session_state:
        st.session_state.events = []
    
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = time.time()
    
    if "selected_log_pod" not in st.session_state:
        st.session_state.selected_log_pod = None


def add_event(message: str, event_type: str = "info"):
    """Add an event to the event log."""
    st.session_state.events.insert(0, {
        "time": datetime.now().strftime("%H:%M:%S"),
        "message": message,
        "type": event_type,
    })
    # Keep only last 50 events
    st.session_state.events = st.session_state.events[:50]


# -----------------------------------------------------------------------------
# UI Components
# -----------------------------------------------------------------------------

def render_status_badge(status: str) -> str:
    """Render a status badge HTML."""
    status_lower = status.lower()
    if "running" in status_lower or "initializing" in status_lower:
        css_class = "status-running"
    elif "succeeded" in status_lower or "complete" in status_lower:
        css_class = "status-succeeded"
    elif "failed" in status_lower or "error" in status_lower:
        css_class = "status-failed"
    elif "pending" in status_lower:
        css_class = "status-pending"
    else:
        css_class = "status-unknown"
    
    return f'<span class="status-badge {css_class}">{status}</span>'


def render_header():
    """Render the main header."""
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown("# 🔮 Ray Job Monitor")
        st.markdown("*Wikipedia Tokenization & Embedding Pipeline*")
    
    with col2:
        k8s_connected = st.session_state.k8s_client.is_connected()
        if k8s_connected:
            context = st.session_state.k8s_client.get_context()
            st.markdown(f"**K8s:** ✅ `{context}`" if context else "**K8s:** ✅ Connected")
        else:
            st.markdown("**K8s:** ❌ Disconnected")


def render_sidebar():
    """Render the sidebar with job launcher."""
    with st.sidebar:
        st.markdown("## 🚀 Job Launcher")
        
        job_type = st.selectbox(
            "Job Type",
            options=["pipeline", "tokenize", "embed"],
            format_func=lambda x: {
                "pipeline": "🔄 Full Pipeline (Tokenize → Embed)",
                "tokenize": "📝 Tokenize Only (CPU)",
                "embed": "🧠 Embed Only (GPU)",
            }.get(x, x),
        )
        
        st.markdown("### Parameters")
        
        max_pages = st.number_input(
            "Max Pages",
            min_value=1,
            max_value=1000,
            value=20,
            help="Maximum number of Wikipedia pages to process",
        )
        
        concurrency = st.slider(
            "Concurrency",
            min_value=1,
            max_value=8,
            value=4,
            help="Number of concurrent tasks",
        )
        
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🚀 Submit", type="primary", use_container_width=True):
                # Clear dashboard events for fresh start
                st.session_state.events = []
                
                # Clear output files FIRST (while head pod still exists)
                with st.spinner("Clearing previous results..."):
                    clear_result = st.session_state.k8s_client.clear_output()
                    if clear_result.get("success"):
                        add_event("Cleared tokens & embeddings", "info")
                
                # Delete ALL existing jobs
                existing_jobs = st.session_state.k8s_client.list_rayjobs()
                if existing_jobs:
                    with st.spinner("Deleting existing jobs..."):
                        for job in existing_jobs:
                            st.session_state.k8s_client.delete_rayjob(job['name'])
                            add_event(f"Deleted job: {job['name']}", "info")
                
                # Wait for all Ray pods to terminate
                with st.spinner("Waiting for pods to terminate..."):
                    max_wait = 30  # seconds
                    waited = 0
                    while waited < max_wait:
                        pods = st.session_state.k8s_client.get_all_ray_pods()
                        all_pods = pods.get("head", []) + pods.get("workers", [])
                        if not all_pods:
                            break
                        time.sleep(2)
                        waited += 2
                    
                    if waited >= max_wait:
                        add_event("Warning: Some pods still terminating", "warning")
                    else:
                        add_event("All previous pods terminated", "info")
                
                # Submit new job
                with st.spinner("Submitting new job..."):
                    result = st.session_state.k8s_client.submit_rayjob(
                        job_type=job_type,
                        params={
                            "max_pages": max_pages,
                            "concurrency": concurrency,
                        },
                    )
                    if result["success"]:
                        st.success(f"✅ {result['message']}")
                        add_event(f"Submitted job: {result['job_name']}", "success")
                        st.rerun()
                    else:
                        st.error(f"❌ {result['error']}")
                        add_event(f"Failed to submit job: {result['error']}", "error")
        
        with col2:
            if st.button("🧹 Clear", use_container_width=True):
                with st.spinner("Clearing checkpoints & results..."):
                    clear_result = st.session_state.k8s_client.clear_output()
                    if clear_result.get("success"):
                        st.success("✅ Cleared!")
                        add_event("Cleared all checkpoints & results", "success")
                    else:
                        st.error(f"❌ {clear_result.get('error', 'Failed')}")
                        add_event(f"Clear failed: {clear_result.get('error')}", "error")
                st.rerun()
        
        with col3:
            if st.button("🔄 Refresh", use_container_width=True):
                st.session_state.last_refresh = time.time()
                st.rerun()
        
        st.markdown("---")
        st.markdown("## 📋 Active Jobs")
        
        jobs = st.session_state.k8s_client.list_rayjobs()
        if jobs:
            for job in jobs:
                with st.expander(f"**{job['name']}**", expanded=True):
                    st.markdown(render_status_badge(job['status']), unsafe_allow_html=True)
                    st.caption(f"Created: {job['created']}")
                    if st.button(f"🗑️ Delete", key=f"del_{job['name']}", use_container_width=True):
                        if st.session_state.k8s_client.delete_rayjob(job['name']):
                            st.success("Deleted!")
                            add_event(f"Deleted job: {job['name']}", "info")
                            st.rerun()
        else:
            st.info("No active jobs")
        
        st.markdown("---")
        st.markdown("### ⚙️ Settings")
        
        auto_refresh = st.checkbox("Auto-refresh (5s)", value=True)
        if auto_refresh:
            st.caption("Dashboard refreshes automatically")


def render_job_progress():
    """Render job progress section."""
    st.markdown("## 📊 Job Progress")
    
    jobs = st.session_state.k8s_client.list_rayjobs()
    
    if not jobs:
        st.info("No jobs found. Submit a job from the sidebar to get started.")
        return
    
    # Prioritize showing a running/pending job over completed ones
    running_jobs = [j for j in jobs if j['status'] not in ['SUCCEEDED', 'FAILED']]
    latest_job = running_jobs[0] if running_jobs else jobs[0]
    
    if latest_job:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Job</div>
                <div style="font-size: 1.2rem; font-weight: 600; color: #e2e8f0;">{latest_job['name'][:20]}...</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Status</div>
                <div style="margin-top: 8px;">{render_status_badge(latest_job['status'])}</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Try to get checkpoint data for progress
        checkpoint = st.session_state.k8s_client.read_checkpoint()
        
        with col3:
            completed = checkpoint.get("count", 0) if checkpoint else 0
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Completed</div>
                <div class="metric-value">{completed}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            # Count active pods
            pods = st.session_state.k8s_client.get_all_ray_pods()
            active_pods = len([p for p in pods.get("workers", []) if p['status'] == 'Running'])
            active_pods += len([p for p in pods.get("head", []) if p['status'] == 'Running'])
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Active Pods</div>
                <div class="metric-value">{active_pods}</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Progress bar
        if checkpoint and checkpoint.get("count", 0) > 0:
            # Estimate total (this is approximate)
            total_estimate = 20  # Default sample size
            progress = min(completed / total_estimate, 1.0)
            st.progress(progress, text=f"Progress: {completed} pages completed")


def render_worker_grid():
    """Render the worker pod grid with logs."""
    st.markdown("## 👷 Cluster Pods & Logs")
    
    # Show current job status at the top for context
    jobs = st.session_state.k8s_client.list_rayjobs()
    if jobs:
        # Prioritize running jobs
        running_jobs = [j for j in jobs if j['status'] not in ['SUCCEEDED', 'FAILED']]
        current_job = running_jobs[0] if running_jobs else jobs[0]
        
        job_status = current_job['status']
        st.markdown(f"**Current Job:** `{current_job['name']}` — {render_status_badge(job_status)}", unsafe_allow_html=True)
        
        # Show warning if job failed but pods still running
        if job_status == 'FAILED':
            st.warning("⚠️ Job has FAILED. Pods may still be running during cleanup.")
    
    pods = st.session_state.k8s_client.get_all_ray_pods()
    head_pods = pods.get("head", [])
    worker_pods = pods.get("workers", [])
    submitter_pods = pods.get("submitter", [])
    all_pods = submitter_pods + head_pods + worker_pods
    
    if not all_pods:
        st.info("No Ray pods found. Submit a job to start the cluster.")
        return
    
    # Create two columns: pod list and log viewer
    col_pods, col_logs = st.columns([1, 2])
    
    with col_pods:
        # Submitter pod (has the actual job output!)
        if submitter_pods:
            st.markdown("### 📋 Job Runner")
            for sub in submitter_pods:
                st.markdown(f"""
                <div class="worker-card" style="border-color: #7c3aed;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <strong style="color: #7c3aed;">JOB OUTPUT</strong>
                        {render_status_badge(sub['status'])}
                    </div>
                    <div style="margin-top: 8px; color: #94a3b8; font-size: 12px;">
                        <div>📍 {sub['node'][:25] if sub['node'] else 'Pending'}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                if st.button("📜 View Job Logs", key=f"logs_sub_{sub['name']}", use_container_width=True, type="primary"):
                    st.session_state.selected_log_pod = sub['name']
                    st.rerun()
        
        # Head pod
        if head_pods:
            st.markdown("### 🎯 Head Node")
            head = head_pods[0]
            
            st.markdown(f"""
            <div class="worker-card">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <strong style="color: #e2e8f0;">HEAD</strong>
                    {render_status_badge(head['status'])}
                </div>
                <div style="margin-top: 8px; color: #94a3b8; font-size: 12px;">
                    <div>📍 {head['node'][:25]}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("📜 View Logs", key=f"logs_head_{head['name']}", use_container_width=True):
                st.session_state.selected_log_pod = head['name']
                st.rerun()
        
        # Worker pods
        if worker_pods:
            st.markdown("### 🔧 Workers")
            
            for idx, worker in enumerate(worker_pods):
                resources = worker.get('resources', {})
                
                st.markdown(f"""
                <div class="worker-card">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <strong style="color: #e2e8f0;">Worker {idx + 1}</strong>
                        {render_status_badge(worker['status'])}
                    </div>
                    <div style="margin-top: 8px; color: #94a3b8; font-size: 12px;">
                        <div>📍 {worker['node'][:20]}</div>
                        <div>💾 {resources.get('memory_request', 'N/A')}</div>
                        <div>🔄 Restarts: {worker['restarts']}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                btn_col1, btn_col2 = st.columns(2)
                with btn_col1:
                    if st.button("📜 Logs", key=f"logs_{worker['name']}", use_container_width=True):
                        st.session_state.selected_log_pod = worker['name']
                        st.rerun()
                
                with btn_col2:
                    if worker['status'] == "Running":
                        if st.button("💀 Kill", key=f"kill_{worker['name']}", use_container_width=True):
                            with st.spinner("Killing..."):
                                result = st.session_state.k8s_client.delete_pod(worker['name'])
                                if result['success']:
                                    add_event(f"KILLED: {worker['name']}", "warning")
                                    time.sleep(1)
                                    st.rerun()
        else:
            st.info("No workers running")
    
    with col_logs:
        st.markdown("### 📜 Live Logs")
        
        # Pod selector dropdown
        pod_names = [p['name'] for p in all_pods]
        
        if st.session_state.selected_log_pod and st.session_state.selected_log_pod in pod_names:
            default_idx = pod_names.index(st.session_state.selected_log_pod)
        else:
            default_idx = 0
            if pod_names:
                st.session_state.selected_log_pod = pod_names[0]
        
        def format_pod_name(x):
            if any(s['name'] == x for s in submitter_pods):
                return f"📋 JOB OUTPUT: {x}"
            elif any(h['name'] == x for h in head_pods):
                return f"🎯 HEAD: {x}"
            else:
                return f"🔧 WORKER: {x}"
        
        selected_pod = st.selectbox(
            "Select Pod",
            options=pod_names,
            index=default_idx,
            format_func=format_pod_name,
        )
        
        if selected_pod:
            st.session_state.selected_log_pod = selected_pod
            
            # Log controls
            col_lines, col_refresh = st.columns([2, 1])
            with col_lines:
                tail_lines = st.select_slider(
                    "Log lines",
                    options=[50, 100, 200, 500],
                    value=100,
                )
            with col_refresh:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("🔄 Refresh Logs", use_container_width=True):
                    st.rerun()
            
            # Fetch and display logs (use timestamp to prevent caching)
            # Adding a unique key based on time ensures fresh fetch
            logs = st.session_state.k8s_client.get_pod_logs(
                selected_pod,
                tail=tail_lines,
            )
            
            if logs:
                # Show when logs were fetched
                st.caption(f"📅 Fetched at {datetime.now().strftime('%H:%M:%S')}")
                st.code(logs, language="log", line_numbers=False)
            else:
                st.caption("No logs available yet")
    
    # Fault tolerance info
    st.markdown("---")
    with st.expander("🛡️ Fault Tolerance Demo", expanded=False):
        st.markdown("""
        **To demonstrate Ray's fault tolerance:**
        1. **Start a job** using the sidebar
        2. **Wait for workers** to begin processing tasks  
        3. **Click "💀 Kill"** on an active worker
        4. **Watch the logs** - Ray detects failure and reschedules tasks
        5. **Job completes** successfully despite the worker failure
        
        ✨ *This demonstrates Ray's automatic task retry!*
        """)


def render_event_log():
    """Render the event log."""
    st.markdown("## 📜 Event Log")
    
    # Get K8s events
    k8s_events = st.session_state.k8s_client.get_events(limit=10)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Dashboard Events")
        if st.session_state.events:
            for event in st.session_state.events[:10]:
                icon = {
                    "success": "✅",
                    "error": "❌",
                    "warning": "⚠️",
                    "info": "ℹ️",
                }.get(event['type'], "📌")
                
                st.markdown(f"""
                <div class="event-item">
                    <span class="event-time">{event['time']}</span>
                    {icon} {event['message']}
                </div>
                """, unsafe_allow_html=True)
        else:
            st.caption("No events yet")
    
    with col2:
        st.markdown("### Kubernetes Events")
        if k8s_events:
            for event in k8s_events[-10:]:
                icon = "⚠️" if event['type'] == "Warning" else "ℹ️"
                st.markdown(f"""
                <div class="event-item">
                    <span class="event-time">{event['timestamp'][-8:] if event['timestamp'] else ''}</span>
                    {icon} <strong>{event['reason']}</strong>: {event['message'][:60]}...
                </div>
                """, unsafe_allow_html=True)
        else:
            st.caption("No K8s events")


def render_results_viewer():
    """Render the results viewer section."""
    st.markdown("## 📁 Output Files")
    
    head_pods = st.session_state.k8s_client.list_head_pods()
    if not head_pods:
        st.info("No head pod available to browse results")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Token Files")
        result = st.session_state.k8s_client.exec_command(
            head_pods[0]['name'],
            ["ls", "-la", "/output/tokens"],
        )
        if result['returncode'] == 0 and result['stdout']:
            lines = result['stdout'].strip().split('\n')
            st.code('\n'.join(lines[:15]))
            if len(lines) > 15:
                st.caption(f"... and {len(lines) - 15} more files")
        else:
            st.caption("No token files yet")
    
    with col2:
        st.markdown("### Embedding Files")
        result = st.session_state.k8s_client.exec_command(
            head_pods[0]['name'],
            ["ls", "-la", "/output/embeddings"],
        )
        if result['returncode'] == 0 and result['stdout']:
            lines = result['stdout'].strip().split('\n')
            st.code('\n'.join(lines[:15]))
            if len(lines) > 15:
                st.caption(f"... and {len(lines) - 15} more files")
        else:
            st.caption("No embedding files yet")


# -----------------------------------------------------------------------------
# Main App
# -----------------------------------------------------------------------------

def main():
    """Main application entry point."""
    init_session_state()
    
    # Header
    render_header()
    
    # Sidebar
    render_sidebar()
    
    # Main content tabs
    tab1, tab2, tab3 = st.tabs([
        "📊 Progress",
        "👷 Workers & Logs",
        "📁 Results",
    ])
    
    with tab1:
        render_job_progress()
        st.markdown("---")
        render_event_log()
    
    with tab2:
        render_worker_grid()
    
    with tab3:
        render_results_viewer()
    
    # Auto-refresh
    if st.session_state.get("auto_refresh", True):
        time.sleep(5)
        st.rerun()


if __name__ == "__main__":
    main()
