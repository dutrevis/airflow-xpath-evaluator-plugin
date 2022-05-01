from airflow.models.baseoperator import BaseOperatorLink
from airflow.models import TaskInstance


class EvaluatedUrlLink(BaseOperatorLink):
    name = 'Evaluated URL'

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        evaluated_url = ti.xcom_pull(
            task_ids=operator.task_id,
            key="evaluated_url"
        )
        return evaluated_url
