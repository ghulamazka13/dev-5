from typing import Any, Dict

from dag_factory import build_datasource_to_dwh_dag


class DatasourceToDwhGenerator:
    """Generate datasource-to-dwh DAGs from metadata configs."""

    def generate_dag(self, dag_cfg: Dict[str, Any]):
        return build_datasource_to_dwh_dag(dag_cfg)
