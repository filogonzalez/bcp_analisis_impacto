# Databricks notebook source
# MAGIC %pip install -U -qqqq mlflow databricks-langchain databricks-agents uv langgraph==0.3.4

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log the agent as an MLflow model

# COMMAND ----------

import mlflow
from mlflow.types.agent import ChatAgentMessage
from mlflow.models.resources import DatabricksServingEndpoint
from pkg_resources import get_distribution
from databricks_langchain import ChatDatabricks
import yaml

from agent import chat_agent, LLM_ENDPOINT_NAME

# Load configuration from chain_config.yaml
with open("chain_config.yaml", "r") as f:
    chain_config = yaml.safe_load(f)

# Define resources needed for the agent
resources = [
    # Add the LLM endpoint as a resource
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME)
]

# Log the agent as an MLflow model
with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model="agent.py",
        extra_pip_requirements=[
            f"databricks-connect=={get_distribution('databricks-connect').version}",
            "databricks-sdk",
            "langchain-core",
            "langgraph"
        ],
        resources=resources,
        model_config=chain_config  # Pass the configuration directly
    )
    mlflow.log_dict(chain_config, "agent_chain_config.json")

print(f"MLflow Run: {logged_agent_info.run_id}")
print(f"Model URI: {logged_agent_info.model_uri}")

# Verify the model has been logged correctly by loading it
model = mlflow.pyfunc.load_model(logged_agent_info.model_uri)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Test local

# COMMAND ----------

print("\nTesting the logged model...")

# Example conversation with notebook path in the query
messages = [
    ChatAgentMessage(
        role="user", 
        content="notebook_path: /Workspace/Users/theodore.kop@databricks.com/FakeAirbnbData ¿Cuál es el linaje de datos de la columna 'property_type'?"
    )
]
#

# Get the response
response = chat_agent.predict(messages=messages)
print("Agent Response:", response.messages[-1].content)

# Example of a follow-up question (no need to include notebook path again)
follow_up_messages = messages + [
    ChatAgentMessage(role="assistant", content=response.messages[-1].content),
    ChatAgentMessage(role="user", content="¿Qué transformaciones se aplican a esta columna?")
]

# Get the follow-up response
follow_up_response = chat_agent.predict(messages=follow_up_messages)
print("\nFollow-up Response:", follow_up_response.messages[-1].content)

# Example of switching to a different notebook
new_notebook_messages = [
    ChatAgentMessage(
        role="user", 
        content="notebook_path: /Workspace/Users/theodore.kop@databricks.com/BCP/System_Tables_Helper ¿Cuál es el linaje de datos de la columna 'task_key' en la tabla 'system.lakeflow.job_task_run_timeline'?"
    )
]

# Get the response for the new notebook
new_response = chat_agent.predict(messages=new_notebook_messages)
print("\nNew Notebook Response:", new_response.messages[-1].content) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

catalog = "theodore_kop_personal"
schema = "bcp"
model_name = "bcp_analisis_impacto_demo_V2"

UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointStateReady, EndpointStateConfigUpdate

w = WorkspaceClient()

deployment_info = agents.deploy(model_name=UC_MODEL_NAME, model_version=uc_registered_model_info.version)

browser_url = mlflow.utils.databricks_utils.get_browser_hostname()
print(f"\n\nView deployment status: https://{browser_url}/ml/endpoints/{deployment_info.endpoint_name}")

print("\nWaiting for endpoint to deploy.  This can take 15 - 20 minutes.", end="")
while w.serving_endpoints.get(deployment_info.endpoint_name).state.ready == EndpointStateReady.NOT_READY or w.serving_endpoints.get(deployment_info.endpoint_name).state.config_update == EndpointStateConfigUpdate.IN_PROGRESS:
    print(".", end="")
    time.sleep(30)


# COMMAND ----------


    #View status: https://e2-dogfood.staging.cloud.databricks.com/ml/endpoints/agents_theodore_kop_personal-bcp-bcp_analisis_impacto_demo
    #Review App: https://e2-dogfood.staging.cloud.databricks.com/ml/review-v2/b995f4de074f48c5878049cb689396f7/chat
    #Monitor: https://e2-dogfood.staging.cloud.databricks.com/ml/experiments/402310512162917/evaluation-monitoring