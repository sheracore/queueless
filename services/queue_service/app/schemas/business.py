from pydantic import BaseModel


class BusinessCreate(BaseModel):
    name: str


class BusinessResponse(BaseModel):
    id: int
    name: str

    model_config = {
        "from_attributes": True,
    }
