from openai import OpenAI
from pydantic import BaseModel

local_base_url = 'http://192.168.0.166:8000/v1'
model_name='Qwen3-235B'

client: OpenAI = OpenAI(
    api_key="EMPTY",
    base_url=local_base_url,
)

class People(BaseModel):
    name: str
    age: int

def guided_json_completion(client: OpenAI, model: str=model_name,input_prompt: str=None):
    json_schema = People.model_json_schema()
    system_prompt = "Please provide the name and age of one random person."
    completion = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": input_prompt,
            }
        ],
        extra_body={"guided_json": json_schema},
    )
    return completion.choices[0].message.reasoning_content,completion.choices[0].message.content


print("\nGuided JSON Completion (People):")
data = guided_json_completion(client, model_name)
people = People.model_validate_json(data)
print(people) 