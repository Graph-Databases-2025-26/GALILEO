#Defining the form of LLM response from LLM using Pydantic
from pydantic import BaseModel, Field
from typing import List, Dict

class WatsonxResponse(BaseModel):
    result_set: List[Dict[str, str]] = Field(
        ..., 
        description="Contiene la risposta dell'LLM. Deve contenere un solo elemento: un dizionario con chiave 'Risposta' e valore il testo della risposta in linguaggio naturale."
    )
    time: float = Field(0.0, description="Tempo di esecuzione (placeholder).")
    tokens: int = Field(0, description="Numero di token usati (placeholder).")