import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./vivreici.db")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with async_session() as session:
        yield session

async def init_db():
    async with engine.begin() as conn:
        from backend.models import Commune, Score  # noqa
        await conn.run_sync(Base.metadata.create_all)
        # Migration : ajout de colonnes si absentes (SQLite ne supporte pas ALTER TABLE IF NOT EXISTS)
        for sql in [
            "ALTER TABLE scores ADD COLUMN equipements_detail TEXT",
            "ALTER TABLE iris_scores ADD COLUMN equipements_detail TEXT",
            "ALTER TABLE iris_zones ADD COLUMN geometry TEXT",
            "ALTER TABLE communes ADD COLUMN geometry TEXT",
            "ALTER TABLE scores ADD COLUMN nb_arrets_tc INTEGER DEFAULT 0",
        ]:
            try:
                await conn.execute(text(sql))
            except Exception:
                pass  # Colonne déjà existante
