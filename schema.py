from typing import Annotated
from pydantic import BaseModel, AfterValidator
import re


def validate_phone(v):
    if not re.fullmatch(r'0\d{10}', v):
        raise ValueError('Phone must start with 0 and have 11 digits.')
    return v


def validate_national_code(v):
    if not re.fullmatch(r'\d{10}', v):
        raise ValueError("National code must have 10 digit")
    return v

class VoteInput(BaseModel):
    phone: Annotated[str, AfterValidator(validate_phone)]
    national_code: Annotated[str, AfterValidator(validate_national_code)]
    candidate_id: str


class UserInput(BaseModel):
    user_name: str
    national_code: Annotated[str, AfterValidator(validate_national_code)]
    phone: Annotated[str, AfterValidator(validate_phone)]
