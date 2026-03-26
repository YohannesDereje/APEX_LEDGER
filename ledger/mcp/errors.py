from pydantic import BaseModel


class ErrorDetail(BaseModel):
    code: str
    detail: str