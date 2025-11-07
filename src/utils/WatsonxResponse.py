#Defining the form of LLM response from LLM using Pydantic
from pydantic import BaseModel, Field
from typing import List, Dict, Union, Any


class WatsonxResponse(BaseModel):
   
    result_set: List[Dict[str, Union[str, int, float, Any]]] = Field( 
        default_factory=list,
        description="List of result records, each as a {column_name: value} dict where values can be mixed types."
    )
   
    time: float = Field(0.0, description="Tempo di esecuzione (placeholder).")
    tokens: int = Field(0, description="Numero di token usati (placeholder).")