import airflow
import json
from airflow.plugins_manager import AirflowPlugin
from airflow.operators.http_operator import SimpleHttpOperator

class JsonHttpOperator(SimpleHttpOperator):

    def execute(self, context):
        text = super(JsonHttpOperator, self).execute(context)
        return json.loads(text)

