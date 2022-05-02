#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime
from dateutil import parser
from abc import abstractmethod

import requests
from lxml import html
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator, SkipMixin

from xpath_evaluation.links.evaluated_url import EvaluatedUrlLink


class BaseXPathEvaluationOperator(PythonOperator, SkipMixin):
    """
    :param xpath: The XPath string used to retrieve HTML elements from the evaluated URL.
    :type xpath: str
    :param evaluated_url: The URL of the website where the XPath will be applied to retrieve
        an element that will be evaluated.
    :type evaluated_url: str
    :param expected_value: value to be used as the expected value for evaluation.
    :type expected_value: any
    :param fail_on_not_found: defines wether the operator should raise an error when XPath
        returns no elements. Default is ``True``. Prevails over ``soft_fail``.
    :type fail_on_not_found: boolean
    :param soft_fail: defines wether the operator should skip downstream tasks when extraction
        or evaluation fails. If ``False``, the operator will fail. Default is ``True``.
    :type soft_fail: boolean
    """

    template_fields = ['xpath', 'evaluated_url', 'expected_value']

    ui_color = '#19647e'

    operator_extra_links = (
        EvaluatedUrlLink(),
    )

    def __init__(self,
                 xpath,
                 evaluated_url,
                 expected_value=None,
                 fail_on_not_found: bool = True,
                 soft_fail: bool = True,
                 *args, **kwargs):
        self.xpath = xpath
        self.evaluated_url = evaluated_url
        self.expected_value = expected_value
        self.fail_on_not_found = fail_on_not_found
        self.soft_fail = soft_fail
        self.provide_context = True
        super().__init__(*args, **kwargs)

    @property
    def expected_value(self):
        """
        Returns expected value
        """
        return self._expected_value

    @abstractmethod
    def _evaluate_xpath(self):
        """
        Abstract method that expects an evaluation of the XPath element extracted,
        according to the provided expected value. Must be implemented.
        Raises 'NotImplementedError'.
        """
        raise NotImplementedError()

    @staticmethod
    def _get_first_element_from_html_list(html_elements):
        """
        Returns the first element from a provided list of HtmlElement or string elements.
        :param html_elements: A list with either HtmlElement elements or strings.
        :type html_elements: list
        """
        extracted_element = html_elements[0]
        first_element = (extracted_element.text
                         if type(extracted_element) == html.HtmlElement
                         else extracted_element)

        return first_element

    @staticmethod
    def _get_xpath_elements_from_url(url, xpath):
        """
        Uses a XPath string to retrieve a list of HtmlElement or string elements,
        extracted from the HTML tree of an URL response.
        :param url: The URL of the website where the XPath will be applied.
        :type url: str
        :param xpath: The XPath string used to retrieve HTML elements from the URL.
        :type xpath: str
        """
        response = requests.get(url)
        tree = html.fromstring(response.content)
        html_elements = tree.xpath(xpath)

        return html_elements

    def _extract_element_from_url(self):
        """
        Returns a string element extracted from the HTML of the URL using the XPath.
        """
        self.log.info(
            f"Extracting HTML elements from '{self.evaluated_url}' "
            "using the provided XPath..."
        )
        html_elements = self._get_xpath_elements_from_url(
            self.evaluated_url, self.xpath)

        if not html_elements:
            not_found_log = (
                f"No elements were found using the "
                f"provided XPath within '{self.evaluated_url}'"
            )

            if self.fail_on_not_found:
                raise AirflowFailException(not_found_log)

            self.log.warning(not_found_log)
            return None

        self.log.info(f"Found {len(html_elements)} HTML elements")

        extracted_value = self._get_first_element_from_html_list(html_elements)

        return extracted_value

    def execute(self, context):
        context["ti"].xcom_push(
            key="evaluated_url",
            value=self.evaluated_url
        )
        raised_exc = AirflowFailException(
            f"Failing task as 'soft_fail' is {self.soft_fail}")
        try:
            condition = super().execute(context)
        except Exception as ex:
            if isinstance(ex, AirflowFailException):
                raise ex
            self.log.error(str(ex))
            raised_exc = ex
            condition = False

        self.log.info(f"Condition result is {condition}")

        if condition:
            self.log.info('Proceeding with downstream tasks...')
            return

        if not self.soft_fail:
            raise raised_exc

        self.log.warning(
            f"Skipping downstream tasks as 'soft_fail' is {self.soft_fail}...")

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'],
                      context['ti'].execution_date, downstream_tasks)

        self.log.info("Done.")


class XPathDatetimeEvaluationOperator(BaseXPathEvaluationOperator):
    """
    :param xpath: The XPath string used to retrieve HTML elements from the evaluated URL.
    :type xpath: str
    :param evaluated_url: The URL of the website where the XPath will be applied to retrieve
        an element that will be evaluated.
    :type evaluated_url: str
    :param expected_value: Datetime value to be used as the expected value for
        evaluation. When no value is provided, falls back to ``datetime.now()``.
    :type expected_value: datetime
    :param fail_on_not_found: defines wether the operator should raise an error when XPath
        returns no elements. Default is ``True``. Prevails over ``soft_fail``.
    :type fail_on_not_found: boolean
    :param soft_fail: defines wether the operator should skip downstream tasks when extraction
        or evaluation fails. If ``False``, the operator will fail. Default is ``True``.
    :type soft_fail: boolean
    :param max_datetime_diff_sec: maximum value in seconds that a datetime difference
        evaluation may reach before failing the task. Default is ``0``.
    :type max_datetime_diff_sec: int
    """

    def __init__(self, max_datetime_diff_sec: int = 0, *args, **kwargs):
        self.max_datetime_diff_sec = max_datetime_diff_sec
        super().__init__(
            python_callable=self._evaluate_xpath,
            *args, **kwargs)

    @BaseXPathEvaluationOperator.expected_value.setter
    def expected_value(self, expected_value):
        """
        Assigns expected value, asserting required type.
        """
        if not isinstance(expected_value, (datetime, type(None))):
            raise TypeError(
                "Expected value must be a datetime or 'None'. Received '{}'".format(
                    expected_value.__class__.__name__)
            )
        self._expected_value = expected_value or datetime.now()

    def _evaluate_xpath(self):
        '''
        Main method to evaluate XPath element according to the expected value.
        '''
        extracted_value = self._extract_element_from_url()

        self.log.info(
            f"Parsing extracted value '{extracted_value}' as datetime...")

        parsed_datetime = parser.parse(extracted_value)
        datetime_diff_sec = (
            parsed_datetime - self.expected_value).total_seconds()

        self.log.info(
            f"Evaluating '{parsed_datetime}' to '{self.expected_value}' "
            f"with max difference of {self.max_datetime_diff_sec} seconds..."
        )

        return datetime_diff_sec > self.max_datetime_diff_sec


class XPathStrEvaluationOperator(BaseXPathEvaluationOperator):
    """
    :param xpath: The XPath string used to retrieve HTML elements from the evaluated URL.
    :type xpath: str
    :param evaluated_url: The URL of the website where the XPath will be applied to retrieve
        an element that will be evaluated.
    :type evaluated_url: str
    :param expected_value: String value expected to be retrieved and evaluated.
    :type expected_value: str
    :param fail_on_not_found: defines wether the operator should raise an error when XPath
        returns no elements. Default is ``True``. Prevails over ``soft_fail``.
    :type fail_on_not_found: boolean
    :param soft_fail: defines wether the operator should skip downstream tasks when extraction
        or evaluation fails. If ``False``, the operator will fail. Default is ``True``.
    :type soft_fail: boolean
    :type fail_on_not_found: boolean
    """

    def __init__(self, *args, **kwargs):
        super().__init__(
            python_callable=self._evaluate_xpath,
            *args, **kwargs)

    @BaseXPathEvaluationOperator.expected_value.setter
    def expected_value(self, expected_value):
        """
        Assigns expected value, asserting required type.
        """
        if not isinstance(expected_value, (str)):
            raise TypeError(
                "Expected value must be a string. Received '{}'".format(
                    expected_value.__class__.__name__)
            )
        self._expected_value = expected_value

    def _evaluate_xpath(self):
        '''
        Main method to evaluate XPath element according to the expected value.
        '''
        extracted_value = self._extract_element_from_url()

        self.log.info(
            f"Comparing extracted value '{extracted_value}' "
            f"to expected value '{self.expected_value}'..."
        )
        return extracted_value == self.expected_value
