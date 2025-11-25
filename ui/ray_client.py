"""Ray Dashboard API client for monitoring job and cluster status."""

import logging
from typing import Any, Dict, List, Optional

import requests

log = logging.getLogger(__name__)


class RayClient:
    """Client for Ray Dashboard REST API."""

    def __init__(self, dashboard_url: str = "http://localhost:8265"):
        """
        Initialize Ray Dashboard client.

        Args:
            dashboard_url: Base URL of Ray Dashboard (default: localhost:8265)
        """
        self.base_url = dashboard_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """Make GET request to Ray Dashboard API."""
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.ConnectionError:
            log.warning(f"Cannot connect to Ray Dashboard at {self.base_url}")
            return None
        except requests.exceptions.Timeout:
            log.warning(f"Timeout connecting to Ray Dashboard")
            return None
        except Exception as e:
            log.error(f"Error fetching {endpoint}: {e}")
            return None

    def is_connected(self) -> bool:
        """Check if Ray Dashboard is reachable."""
        try:
            resp = self.session.get(f"{self.base_url}/api/version", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

    def get_version(self) -> Optional[str]:
        """Get Ray version from dashboard."""
        data = self._get("/api/version")
        if data:
            return data.get("ray_version") or data.get("version")
        return None

    def get_jobs(self) -> List[Dict]:
        """
        Get list of all Ray jobs.

        Returns:
            List of job dictionaries with keys like:
            - job_id, submission_id, status, entrypoint, start_time, end_time, etc.
        """
        # Try the Jobs API endpoint
        data = self._get("/api/jobs/")
        if data is None:
            return []

        # Handle different response formats
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # Some versions return {"jobs": [...]}
            return data.get("jobs", [])
        return []

    def get_job_info(self, job_id: str) -> Optional[Dict]:
        """
        Get detailed info for a specific job.

        Args:
            job_id: Ray job ID or submission ID

        Returns:
            Job info dict or None if not found
        """
        data = self._get(f"/api/jobs/{job_id}")
        return data

    def get_nodes(self) -> List[Dict]:
        """
        Get list of Ray cluster nodes.

        Returns:
            List of node dictionaries with keys like:
            - node_id, node_ip, state, resources_total, resources_available
        """
        data = self._get("/api/cluster_status")
        if data is None:
            return []

        # Extract nodes from cluster status
        nodes = []
        
        # Try different response formats
        if isinstance(data, dict):
            # Format: {"result": true, "data": {"clusterStatus": {"nodes": [...]}}}
            cluster_data = data.get("data", {})
            if isinstance(cluster_data, dict):
                cluster_status = cluster_data.get("clusterStatus", {})
                if isinstance(cluster_status, dict):
                    nodes = cluster_status.get("nodes", [])
                    if nodes:
                        return nodes
            
            # Alternative format: direct nodes list
            if "nodes" in data:
                return data["nodes"]

        # Try the nodes API endpoint as fallback
        nodes_data = self._get("/api/v0/nodes")
        if nodes_data and isinstance(nodes_data, dict):
            return nodes_data.get("data", {}).get("nodes", [])

        return nodes

    def get_node_summary(self) -> Dict[str, Any]:
        """
        Get summary of cluster nodes.

        Returns:
            Dict with total_nodes, alive_nodes, dead_nodes, resources
        """
        nodes = self.get_nodes()
        
        alive = [n for n in nodes if n.get("state", "").upper() == "ALIVE"]
        dead = [n for n in nodes if n.get("state", "").upper() == "DEAD"]
        
        # Aggregate resources
        total_cpu = sum(n.get("resources_total", {}).get("CPU", 0) for n in alive)
        avail_cpu = sum(n.get("resources_available", {}).get("CPU", 0) for n in alive)
        total_gpu = sum(n.get("resources_total", {}).get("GPU", 0) for n in alive)
        avail_gpu = sum(n.get("resources_available", {}).get("GPU", 0) for n in alive)
        
        return {
            "total_nodes": len(nodes),
            "alive_nodes": len(alive),
            "dead_nodes": len(dead),
            "total_cpu": total_cpu,
            "available_cpu": avail_cpu,
            "total_gpu": total_gpu,
            "available_gpu": avail_gpu,
        }

    def get_actors(self) -> List[Dict]:
        """Get list of Ray actors."""
        data = self._get("/api/v0/actors")
        if data and isinstance(data, dict):
            return data.get("data", {}).get("actors", [])
        return []

    def get_tasks(self) -> List[Dict]:
        """
        Get list of Ray tasks (if available in this Ray version).
        Note: Task-level API may not be available in all Ray versions.
        """
        data = self._get("/api/v0/tasks")
        if data and isinstance(data, dict):
            return data.get("data", {}).get("tasks", [])
        return []

    def get_cluster_status(self) -> Dict[str, Any]:
        """
        Get overall cluster status.

        Returns:
            Dict with cluster health info
        """
        data = self._get("/api/cluster_status")
        if data is None:
            return {"status": "disconnected"}

        if isinstance(data, dict):
            return {
                "status": "connected",
                "result": data.get("result", False),
                "data": data.get("data", {}),
            }
        return {"status": "unknown", "raw": data}


def get_ray_client(dashboard_url: str = "http://localhost:8265") -> RayClient:
    """Factory function to create RayClient instance."""
    return RayClient(dashboard_url)


