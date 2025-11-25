"""Kubernetes client for managing Ray jobs and pods."""

import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# Default namespace for wiki-tokenizer workloads
DEFAULT_NAMESPACE = "wiki-tokenizer"

# Path to k8s manifests
K8S_DIR = Path(__file__).parent.parent / "k8s"


class K8sClient:
    """Client for Kubernetes operations via kubectl."""

    def __init__(
        self,
        kubeconfig: Optional[str] = None,
        namespace: str = DEFAULT_NAMESPACE,
    ):
        """
        Initialize Kubernetes client.

        Args:
            kubeconfig: Path to kubeconfig file (default: use KUBECONFIG env or ~/.kube/config)
            namespace: Default namespace for operations
        """
        self.kubeconfig = kubeconfig or os.environ.get("KUBECONFIG")
        self.namespace = namespace

    def _kubectl(
        self,
        args: List[str],
        namespace: Optional[str] = None,
        capture_output: bool = True,
        input_data: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        """Run kubectl command."""
        cmd = ["kubectl"]
        if self.kubeconfig:
            cmd.extend(["--kubeconfig", self.kubeconfig])
        if namespace or self.namespace:
            cmd.extend(["-n", namespace or self.namespace])
        cmd.extend(args)

        log.debug(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=capture_output,
                text=True,
                input=input_data,
                timeout=60,
            )
            return result
        except subprocess.TimeoutExpired:
            log.error(f"kubectl command timed out: {' '.join(args)}")
            raise
        except Exception as e:
            log.error(f"kubectl error: {e}")
            raise

    def is_connected(self) -> bool:
        """Check if kubectl can connect to cluster."""
        try:
            result = self._kubectl(["cluster-info"], namespace=None, capture_output=True)
            return result.returncode == 0
        except Exception:
            return False

    def get_context(self) -> Optional[str]:
        """Get current kubectl context."""
        try:
            result = self._kubectl(
                ["config", "current-context"],
                namespace=None,
                capture_output=True,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return None

    # -------------------------------------------------------------------------
    # RayJob Operations
    # -------------------------------------------------------------------------

    def list_rayjobs(self, namespace: Optional[str] = None) -> List[Dict]:
        """
        List all RayJobs in namespace, sorted by creation time (newest first).

        Returns:
            List of RayJob dicts with name, status, etc.
        """
        result = self._kubectl(
            ["get", "rayjob", "-o", "json"],
            namespace=namespace,
        )
        if result.returncode != 0:
            log.error(f"Failed to list rayjobs: {result.stderr}")
            return []

        try:
            data = json.loads(result.stdout)
            jobs = []
            for item in data.get("items", []):
                metadata = item.get("metadata", {})
                status = item.get("status", {})
                spec = item.get("spec", {})
                jobs.append({
                    "name": metadata.get("name"),
                    "namespace": metadata.get("namespace"),
                    "created": metadata.get("creationTimestamp"),
                    "status": status.get("jobStatus", "Unknown"),
                    "deployment_status": status.get("jobDeploymentStatus", "Unknown"),
                    "start_time": status.get("startTime"),
                    "end_time": status.get("endTime"),
                    "entrypoint": spec.get("entrypoint", ""),
                    "ray_cluster_name": status.get("rayClusterName"),
                })
            # Sort by creation time, newest first
            jobs.sort(key=lambda x: x.get("created", ""), reverse=True)
            return jobs
        except json.JSONDecodeError as e:
            log.error(f"Failed to parse rayjob list: {e}")
            return []

    def get_rayjob(self, name: str, namespace: Optional[str] = None) -> Optional[Dict]:
        """Get a specific RayJob by name."""
        result = self._kubectl(
            ["get", "rayjob", name, "-o", "json"],
            namespace=namespace,
        )
        if result.returncode != 0:
            return None

        try:
            item = json.loads(result.stdout)
            metadata = item.get("metadata", {})
            status = item.get("status", {})
            spec = item.get("spec", {})
            return {
                "name": metadata.get("name"),
                "namespace": metadata.get("namespace"),
                "created": metadata.get("creationTimestamp"),
                "status": status.get("jobStatus", "Unknown"),
                "deployment_status": status.get("jobDeploymentStatus", "Unknown"),
                "start_time": status.get("startTime"),
                "end_time": status.get("endTime"),
                "entrypoint": spec.get("entrypoint", ""),
                "ray_cluster_name": status.get("rayClusterName"),
                "message": status.get("message", ""),
            }
        except json.JSONDecodeError:
            return None

    def submit_rayjob(
        self,
        job_type: str,
        params: Optional[Dict[str, Any]] = None,
        namespace: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Submit a RayJob to the cluster.

        Args:
            job_type: One of "tokenize", "embed", "pipeline"
            params: Optional parameters to customize the job
            namespace: Kubernetes namespace

        Returns:
            Dict with success status and job name or error
        """
        params = params or {}
        
        # Map job type to YAML file
        yaml_files = {
            "tokenize": "rayjob-toy.yaml",
            "embed": "rayjob-embed.yaml",
            "pipeline": "rayjob-pipeline.yaml",
        }
        
        if job_type not in yaml_files:
            return {"success": False, "error": f"Unknown job type: {job_type}"}

        yaml_path = K8S_DIR / yaml_files[job_type]
        if not yaml_path.exists():
            return {"success": False, "error": f"YAML file not found: {yaml_path}"}

        # Read and optionally modify the YAML
        yaml_content = yaml_path.read_text()
        
        # Generate unique job name to allow multiple submissions
        import time
        timestamp = int(time.time())
        
        # Replace job name with unique name
        if job_type == "tokenize":
            original_name = "wiki-rayjob-sample"
            new_name = f"wiki-tokenize-{timestamp}"
        elif job_type == "embed":
            original_name = "wiki-rayjob-embed"
            new_name = f"wiki-embed-{timestamp}"
        else:  # pipeline
            original_name = "wiki-pipeline"
            new_name = f"wiki-pipeline-{timestamp}"
        
        yaml_content = yaml_content.replace(f"name: {original_name}", f"name: {new_name}")
        
        # Apply custom parameters if provided
        if params.get("max_pages"):
            # Add --max-pages to entrypoint
            yaml_content = yaml_content.replace(
                "--concurrency",
                f"--max-pages {params['max_pages']}\n    --concurrency",
            )
        
        if params.get("concurrency"):
            yaml_content = yaml_content.replace(
                "--concurrency 4",
                f"--concurrency {params['concurrency']}",
            )

        # Write to temp file and apply
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            result = self._kubectl(
                ["apply", "-f", temp_path],
                namespace=namespace,
            )
            if result.returncode != 0:
                return {"success": False, "error": result.stderr}

            return {
                "success": True,
                "job_name": new_name,
                "message": f"Submitted {job_type} job: {new_name}",
            }
        finally:
            os.unlink(temp_path)

    def delete_rayjob(self, name: str, namespace: Optional[str] = None) -> bool:
        """Delete a RayJob."""
        result = self._kubectl(
            ["delete", "rayjob", name, "--ignore-not-found"],
            namespace=namespace,
        )
        return result.returncode == 0

    # -------------------------------------------------------------------------
    # Pod Operations
    # -------------------------------------------------------------------------

    def list_pods(
        self,
        labels: Optional[Dict[str, str]] = None,
        namespace: Optional[str] = None,
    ) -> List[Dict]:
        """
        List pods in namespace.

        Args:
            labels: Optional label selector dict
            namespace: Kubernetes namespace

        Returns:
            List of pod dicts
        """
        args = ["get", "pods", "-o", "json"]
        if labels:
            selector = ",".join(f"{k}={v}" for k, v in labels.items())
            args.extend(["-l", selector])

        result = self._kubectl(args, namespace=namespace)
        if result.returncode != 0:
            log.error(f"Failed to list pods: {result.stderr}")
            return []

        try:
            data = json.loads(result.stdout)
            pods = []
            for item in data.get("items", []):
                metadata = item.get("metadata", {})
                spec = item.get("spec", {})
                status = item.get("status", {})
                
                # Get container statuses
                container_statuses = status.get("containerStatuses", [])
                ready_containers = sum(1 for c in container_statuses if c.get("ready"))
                total_containers = len(container_statuses)
                
                # Get resource requests/limits from first container
                containers = spec.get("containers", [])
                resources = {}
                if containers:
                    res = containers[0].get("resources", {})
                    requests = res.get("requests", {})
                    limits = res.get("limits", {})
                    resources = {
                        "cpu_request": requests.get("cpu", ""),
                        "memory_request": requests.get("memory", ""),
                        "gpu_request": requests.get("nvidia.com/gpu", "0"),
                        "cpu_limit": limits.get("cpu", ""),
                        "memory_limit": limits.get("memory", ""),
                    }
                
                pods.append({
                    "name": metadata.get("name"),
                    "namespace": metadata.get("namespace"),
                    "node": spec.get("nodeName", ""),
                    "status": status.get("phase", "Unknown"),
                    "ready": f"{ready_containers}/{total_containers}",
                    "restarts": sum(c.get("restartCount", 0) for c in container_statuses),
                    "created": metadata.get("creationTimestamp"),
                    "labels": metadata.get("labels", {}),
                    "resources": resources,
                    "pod_ip": status.get("podIP", ""),
                    "host_ip": status.get("hostIP", ""),
                })
            return pods
        except json.JSONDecodeError as e:
            log.error(f"Failed to parse pod list: {e}")
            return []

    def list_worker_pods(self, namespace: Optional[str] = None) -> List[Dict]:
        """List Ray worker pods."""
        return self.list_pods(
            labels={"ray.io/node-type": "worker"},
            namespace=namespace,
        )

    def list_head_pods(self, namespace: Optional[str] = None) -> List[Dict]:
        """List Ray head pods."""
        return self.list_pods(
            labels={"ray.io/node-type": "head"},
            namespace=namespace,
        )

    def list_job_pods(self, namespace: Optional[str] = None) -> List[Dict]:
        """List RayJob submitter pods (these run the actual job script)."""
        # RayJob submitter pods have the job-name label but not node-type
        all_pods = self.list_pods(namespace=namespace)
        job_pods = []
        for pod in all_pods:
            labels = pod.get("labels", {})
            # Submitter pods have ray.io/job-name but NOT ray.io/node-type
            if "batch.kubernetes.io/job-name" in labels or (
                any("rayjob" in labels.get(k, "").lower() for k in labels) and 
                "ray.io/node-type" not in labels
            ):
                job_pods.append(pod)
        return job_pods

    def get_all_ray_pods(self, namespace: Optional[str] = None) -> Dict[str, List[Dict]]:
        """Get all Ray pods grouped by type."""
        return {
            "head": self.list_head_pods(namespace),
            "workers": self.list_worker_pods(namespace),
            "submitter": self.list_job_pods(namespace),
        }

    def delete_pod(self, name: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Delete a pod (for fault tolerance demo).

        Args:
            name: Pod name
            namespace: Kubernetes namespace

        Returns:
            Dict with success status
        """
        result = self._kubectl(
            ["delete", "pod", name, "--grace-period=0", "--force"],
            namespace=namespace,
        )
        if result.returncode == 0:
            return {"success": True, "message": f"Deleted pod {name}"}
        else:
            return {"success": False, "error": result.stderr}

    def get_pod_logs(
        self,
        name: str,
        namespace: Optional[str] = None,
        tail: int = 100,
        since: Optional[str] = None,
        all_containers: bool = True,
    ) -> str:
        """
        Get logs from a pod.
        
        Args:
            name: Pod name
            namespace: Kubernetes namespace
            tail: Number of lines to fetch from the end
            since: Only return logs newer than this duration (e.g., "60s", "5m")
            all_containers: If True, get logs from all containers in the pod
        """
        args = ["logs", name, f"--tail={tail}", "--timestamps"]
        if all_containers:
            args.append("--all-containers=true")
        if since:
            args.append(f"--since={since}")

        result = self._kubectl(args, namespace=namespace)
        if result.returncode != 0:
            return f"Error getting logs: {result.stderr}"
        return result.stdout

    def exec_command(
        self,
        pod_name: str,
        command: List[str],
        namespace: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a command in a pod.

        Args:
            pod_name: Name of the pod
            command: Command to execute
            namespace: Kubernetes namespace

        Returns:
            Dict with stdout, stderr, and return code
        """
        args = ["exec", pod_name, "--"] + command
        result = self._kubectl(args, namespace=namespace)
        return {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }

    def read_checkpoint(
        self,
        checkpoint_path: str = "/output/checkpoint.json",
        namespace: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Read checkpoint file from head pod.

        Args:
            checkpoint_path: Path to checkpoint file in pod
            namespace: Kubernetes namespace

        Returns:
            Checkpoint data dict or None
        """
        head_pods = self.list_head_pods(namespace)
        if not head_pods:
            return None

        head_pod = head_pods[0]["name"]
        result = self.exec_command(
            head_pod,
            ["cat", checkpoint_path],
            namespace=namespace,
        )
        
        if result["returncode"] != 0:
            return None

        try:
            return json.loads(result["stdout"])
        except json.JSONDecodeError:
            return None

    def list_output_files(
        self,
        output_dir: str = "/output",
        namespace: Optional[str] = None,
    ) -> List[str]:
        """List files in output directory."""
        head_pods = self.list_head_pods(namespace)
        if not head_pods:
            return []

        head_pod = head_pods[0]["name"]
        result = self.exec_command(
            head_pod,
            ["ls", "-la", output_dir],
            namespace=namespace,
        )
        
        if result["returncode"] != 0:
            return []

        return result["stdout"].strip().split("\n")

    # -------------------------------------------------------------------------
    # Namespace Operations
    # -------------------------------------------------------------------------

    def ensure_namespace(self, namespace: Optional[str] = None) -> bool:
        """Ensure namespace exists."""
        ns = namespace or self.namespace
        result = self._kubectl(
            ["create", "namespace", ns, "--dry-run=client", "-o", "yaml"],
            namespace=None,
        )
        if result.returncode != 0:
            return False

        result = self._kubectl(
            ["apply", "-f", "-"],
            namespace=None,
            input_data=result.stdout,
        )
        return result.returncode == 0

    def get_events(
        self,
        namespace: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict]:
        """Get recent events in namespace."""
        result = self._kubectl(
            ["get", "events", "--sort-by=.lastTimestamp", "-o", "json"],
            namespace=namespace,
        )
        if result.returncode != 0:
            return []

        try:
            data = json.loads(result.stdout)
            events = []
            for item in data.get("items", [])[-limit:]:
                events.append({
                    "type": item.get("type", ""),
                    "reason": item.get("reason", ""),
                    "message": item.get("message", ""),
                    "object": item.get("involvedObject", {}).get("name", ""),
                    "timestamp": item.get("lastTimestamp", ""),
                })
            return events
        except json.JSONDecodeError:
            return []

    def clear_output(self, namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Clear output directory contents before starting a new job.
        
        This removes tokens, embeddings, and checkpoint files from /output.
        """
        head_pods = self.list_head_pods(namespace)
        if not head_pods:
            return {"success": False, "error": "No head pod available"}

        head_pod = head_pods[0]["name"]
        
        # Clear output directories
        commands = [
            ["rm", "-rf", "/output/tokens"],
            ["rm", "-rf", "/output/embeddings"],
            ["rm", "-f", "/output/checkpoint.json"],
            ["rm", "-f", "/output/tokenize_checkpoint.json"],
            ["rm", "-f", "/output/embed_checkpoint.json"],
            ["rm", "-f", "/output/token_counts.jsonl"],
        ]
        
        errors = []
        for cmd in commands:
            result = self.exec_command(head_pod, cmd, namespace=namespace)
            if result["returncode"] != 0 and result["stderr"]:
                errors.append(result["stderr"])
        
        if errors:
            return {"success": False, "error": "; ".join(errors)}
        
        return {"success": True, "message": "Output cleared"}


def get_k8s_client(
    kubeconfig: Optional[str] = None,
    namespace: str = DEFAULT_NAMESPACE,
) -> K8sClient:
    """Factory function to create K8sClient instance."""
    return K8sClient(kubeconfig=kubeconfig, namespace=namespace)


