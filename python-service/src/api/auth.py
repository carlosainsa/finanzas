from enum import Enum
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.config import settings


bearer_scheme = HTTPBearer(auto_error=False)


class OperatorRole(str, Enum):
    READ = "read"
    CONTROL = "control"


def configured_tokens(role: OperatorRole) -> set[str]:
    tokens: set[str] = set()
    if settings.operator_api_token:
        tokens.add(settings.operator_api_token)
    if role == OperatorRole.READ and settings.operator_read_token:
        tokens.add(settings.operator_read_token)
    if role == OperatorRole.CONTROL and settings.operator_control_token:
        tokens.add(settings.operator_control_token)
    if role == OperatorRole.READ and settings.operator_control_token:
        tokens.add(settings.operator_control_token)
    return tokens


def validate_operator_token(
    credentials: HTTPAuthorizationCredentials | None, role: OperatorRole
) -> None:
    tokens = configured_tokens(role)
    if not tokens:
        return
    if credentials is None or credentials.credentials not in tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"valid operator {role.value} bearer token is required",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def require_read_auth(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
) -> None:
    validate_operator_token(credentials, OperatorRole.READ)


async def require_control_auth(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
) -> None:
    validate_operator_token(credentials, OperatorRole.CONTROL)
