from airflow.plugins_manager import AirflowPlugin
from xpath_evaluation.links.evaluated_url import EvaluatedUrlLink
from xpath_evaluation.operators.xpath_evaluation_operator import XPathEvaluationOperator


class AirflowXPathEvaluationOperatorPlugin(AirflowPlugin):
    name = "xpath_evaluation_operator"
    operators = [XPathEvaluationOperator]
    sensors = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = [EvaluatedUrlLink(), ]
