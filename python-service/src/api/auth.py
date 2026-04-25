from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.config import settings


bearer_scheme = HTTPBearer(auto_error=False)


async def require_operator_auth(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
) -> None:
    if settings.operator_api_token is None:
        return
    if credentials is None or credentials.credentials != settings.operator_api_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="valid operator bearer token is required",
            headers={"WWW-Authenticate": "Bearer"},
        )
