from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.business import Business
from app.schemas.business import BusinessCreate, BusinessResponse

router = APIRouter(
    prefix="/businesses",
    tags=["businesses"],
)

@router.post(
    "",
    response_model=BusinessResponse,
)
def create_business(
        data: BusinessCreate,
        db: Session = Depends(get_db),
):
    business = Business(
        name=data.name,
        created_at=datetime.now(timezone.utc),
    )
    db.add(business)
    db.commit()
    db.refresh(business)

    return business