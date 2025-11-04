from pydantic import BaseModel, Field
from typing import List, Dict, Optional

class WatsonxResponse(BaseModel):
    """Schema of the expected Watsonx structured JSON response."""
    result_set: List[Dict[str, str]] = Field(
        default_factory=list,
        description="List of result records, each as a {column_name: value} dict"
    )
    time: Optional[float] = Field(
        default=None,
        description="Estimated execution time in seconds"
    )
    tokens: Optional[int] = Field(
        default=None,
        description="Estimated number of tokens used"
    )
