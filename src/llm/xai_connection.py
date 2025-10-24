from xai_sdk import Client
from xai_sdk.chat import  user
from config import get_llm_settings

grok = get_llm_settings("grok")

# Crea un client sincrono
client = Client(
    api_key = grok["api_key"],
    api_host = grok["api_host"]
)

chat = client.chat.create(model=str(grok["config"]["model"]))
chat.append(user("What is the meaning of life?"))


completion = chat.sample()

