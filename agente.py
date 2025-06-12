# Databricks notebook source
# MAGIC %pip install -U -qqqq mlflow databricks-langchain databricks-agents uv langgraph==0.3.4

# COMMAND ----------

# MAGIC %%writefile agent.py
# MAGIC from typing import Any, Generator, Optional, Sequence, Union
# MAGIC
# MAGIC import mlflow
# MAGIC from databricks_langchain import ChatDatabricks
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service import workspace
# MAGIC import base64
# MAGIC import re
# MAGIC from langchain_core.language_models import LanguageModelLike
# MAGIC from langchain_core.runnables import RunnableConfig, RunnableLambda
# MAGIC from langchain_core.tools import BaseTool
# MAGIC from langgraph.graph import END, StateGraph
# MAGIC from langgraph.graph.graph import CompiledGraph
# MAGIC from langgraph.graph.state import CompiledStateGraph
# MAGIC from langgraph.prebuilt.tool_node import ToolNode
# MAGIC from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
# MAGIC from mlflow.pyfunc import ChatAgent
# MAGIC from mlflow.types.agent import (
# MAGIC     ChatAgentChunk,
# MAGIC     ChatAgentMessage,
# MAGIC     ChatAgentResponse,
# MAGIC     ChatContext,
# MAGIC )
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Define tools for your agent
# MAGIC ###############################################################################
# MAGIC tools = []
# MAGIC
# MAGIC # Create a notebook retriever tool
# MAGIC class NotebookRetrieverTool(BaseTool):
# MAGIC     name: str = "notebook_retriever"
# MAGIC     description: str = "Retrieves the content of a Databricks notebook for analysis. The query should include the notebook path in the format 'notebook_path: /path/to/notebook'"
# MAGIC     notebook_path: str = ""
# MAGIC     
# MAGIC     def __init__(self):
# MAGIC         super().__init__()
# MAGIC         self._workspace_client = WorkspaceClient()
# MAGIC         self._cached_content = None
# MAGIC         self._cached_path = None  # Track which path is cached
# MAGIC     
# MAGIC     def _extract_notebook_path(self, query: str) -> Optional[str]:
# MAGIC         """Extract notebook path from the query."""
# MAGIC         # Look for notebook_path: pattern in the query
# MAGIC         match = re.search(r'notebook_path:\s*(/[^\s]+)', query)
# MAGIC         if match:
# MAGIC             return match.group(1)
# MAGIC         return None
# MAGIC     
# MAGIC     def _run(self, query: str) -> str:
# MAGIC         """Retrieve notebook content."""
# MAGIC         # Extract notebook path from query
# MAGIC         notebook_path = self._extract_notebook_path(query)
# MAGIC         if not notebook_path:
# MAGIC             return "Error: No notebook path found in query. Please include 'notebook_path: /path/to/notebook' in your query."
# MAGIC         
# MAGIC         # Update the notebook path
# MAGIC         self.notebook_path = notebook_path
# MAGIC         
# MAGIC         # Clear cache if the path has changed
# MAGIC         if self._cached_path != notebook_path:
# MAGIC             self._cached_content = None
# MAGIC             self._cached_path = None
# MAGIC         
# MAGIC         if self._cached_content is None:
# MAGIC             try:
# MAGIC                 # Export the notebook in source format
# MAGIC                 export = self._workspace_client.workspace.export(
# MAGIC                     self.notebook_path, 
# MAGIC                     format=workspace.ExportFormat.SOURCE
# MAGIC                 )
# MAGIC                 
# MAGIC                 # Decode the base64 content and update cache
# MAGIC                 self._cached_content = base64.b64decode(export.content).decode('utf-8')
# MAGIC                 self._cached_path = notebook_path  # Update cached path
# MAGIC             except Exception as e:
# MAGIC                 print(f"Failed to export {self.notebook_path}: {e}")
# MAGIC                 return f"Error: Failed to export notebook {self.notebook_path}: {str(e)}"
# MAGIC         
# MAGIC         return self._cached_content
# MAGIC     
# MAGIC     def clear_cache(self):
# MAGIC         """Clear the cached notebook content."""
# MAGIC         self._cached_content = None
# MAGIC         self._cached_path = None
# MAGIC
# MAGIC # Add the notebook retriever tool
# MAGIC tools.append(NotebookRetrieverTool())  # No need to pass notebook_path anymore
# MAGIC
# MAGIC #####################
# MAGIC ## Define agent logic
# MAGIC #####################
# MAGIC def create_tool_calling_agent(
# MAGIC     model: LanguageModelLike,
# MAGIC     tools: Union[ToolNode, Sequence[BaseTool]],
# MAGIC     system_prompt: Optional[str] = None,
# MAGIC ) -> CompiledGraph:
# MAGIC     model = model.bind_tools(tools)
# MAGIC
# MAGIC     # Define the function that determines which node to go to
# MAGIC     def should_continue(state: ChatAgentState):
# MAGIC         messages = state["messages"]
# MAGIC         last_message = messages[-1]
# MAGIC         # If there are function calls, continue. else, end
# MAGIC         if last_message.get("tool_calls"):
# MAGIC             return "continue"
# MAGIC         else:
# MAGIC             return "end"
# MAGIC
# MAGIC     if system_prompt:
# MAGIC         preprocessor = RunnableLambda(
# MAGIC             lambda state: [{"role": "system", "content": system_prompt}]
# MAGIC             + state["messages"]
# MAGIC         )
# MAGIC     else:
# MAGIC         preprocessor = RunnableLambda(lambda state: state["messages"])
# MAGIC     model_runnable = preprocessor | model
# MAGIC
# MAGIC     def call_model(
# MAGIC         state: ChatAgentState,
# MAGIC         config: RunnableConfig,
# MAGIC     ):
# MAGIC         response = model_runnable.invoke(state, config)
# MAGIC         return {"messages": [response]}
# MAGIC
# MAGIC     workflow = StateGraph(ChatAgentState)
# MAGIC
# MAGIC     workflow.add_node("agent", RunnableLambda(call_model))
# MAGIC     workflow.add_node("tools", ChatAgentToolNode(tools))
# MAGIC
# MAGIC     workflow.set_entry_point("agent")
# MAGIC     workflow.add_conditional_edges(
# MAGIC         "agent",
# MAGIC         should_continue,
# MAGIC         {
# MAGIC             "continue": "tools",
# MAGIC             "end": END,
# MAGIC         },
# MAGIC     )
# MAGIC     workflow.add_edge("tools", "agent")
# MAGIC
# MAGIC     return workflow.compile()
# MAGIC
# MAGIC class LangGraphChatAgent(ChatAgent):
# MAGIC     def __init__(self, agent: CompiledStateGraph):
# MAGIC         self.agent = agent
# MAGIC
# MAGIC     def predict(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> ChatAgentResponse:
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC
# MAGIC         messages = []
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 messages.extend(
# MAGIC                     ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
# MAGIC                 )
# MAGIC         return ChatAgentResponse(messages=messages)
# MAGIC
# MAGIC     def predict_stream(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> Generator[ChatAgentChunk, None, None]:
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 yield from (
# MAGIC                     ChatAgentChunk(**{"delta": msg}) for msg in node_data["messages"]
# MAGIC                 )
# MAGIC
# MAGIC # Create the agent object, and specify it as the agent object to use when
# MAGIC # loading the agent back for inference via mlflow.models.set_model()
# MAGIC
# MAGIC # Create the notebook retriever tool
# MAGIC # notebook_tool = NotebookRetrieverTool()
# MAGIC
# MAGIC # Load configuration
# MAGIC config = mlflow.models.ModelConfig(development_config="chain_config.yaml")
# MAGIC llm_config = config.get("llm_config")
# MAGIC databricks_resources = config.get("databricks_resources")
# MAGIC
# MAGIC # Create the LLM model
# MAGIC LLM_ENDPOINT_NAME = databricks_resources.get("llm_endpoint_name")
# MAGIC
# MAGIC llm = ChatDatabricks(
# MAGIC     endpoint=databricks_resources.get("llm_endpoint_name"),
# MAGIC     extra_params=llm_config.get("llm_parameters")
# MAGIC )
# MAGIC
# MAGIC # Create the agent with the tool
# MAGIC agent = create_tool_calling_agent(
# MAGIC     model=llm,
# MAGIC     tools=tools,
# MAGIC     system_prompt=llm_config.get("llm_system_prompt_template")
# MAGIC )
# MAGIC mlflow.langchain.autolog()
# MAGIC
# MAGIC chat_agent = LangGraphChatAgent(agent)
# MAGIC mlflow.models.set_model(chat_agent) 