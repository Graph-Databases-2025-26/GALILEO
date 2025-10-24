from ibm_watsonx_ai import Credentials, Model


# Substitute with your API key and URL
my_key = "YOUR_IBM_WATSONX_AI_API_KEY"
my_url = "https://us-south.ml.cloud.ibm.com"

creds = Credentials(url=my_url, api_key=my_key)

model = Model(
    model_id="ibm/granite-13b-chat-v2",
    credentials=creds,
    project_id="IL_TUO_PROJECT_ID"   #Substitute with your project ID
)

response = model.generate(prompt="Hello, Watsonx!")
print(response)
