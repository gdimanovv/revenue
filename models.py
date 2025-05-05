from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, Text
)
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.types import DateTime

Base = declarative_base()

class Product(Base):
    __tablename__ = 'product'

    sku_id = Column(Integer, primary_key=True)
    sku_description = Column(Text)
    price = Column(Float)
    insert_timestamp_utc = Column(DateTime)

class Sales(Base):
    __tablename__ = 'sales'

    sku_id = Column(Integer)
    order_id = Column(String, primary_key=True)
    sales = Column(Integer)
    orderdate_utc = Column(DateTime)  
    insert_timestamp_utc = Column(String)

class Revenue(Base):
    __tablename__ = 'revenue'

    sku_id = Column(String, primary_key=True)
    date_id = Column(Date, primary_key=True)
    price = Column(Float)
    sales = Column(Integer)
    revenue = Column(Float)

def init_db(in_memory = False, db_path='product_sales.db'):
    if in_memory:
        engine = create_engine('sqlite:///:memory:')
    else:
        engine = create_engine(f'sqlite:///{db_path}', echo=False)
    Base.metadata.create_all(engine)
    return engine

# upsert to enable data correction over time
def upsert_df_revenue_records(engine, revenue_df):
    with Session(engine) as session:
        for _, row in revenue_df.iterrows():
            stmt = insert(Revenue).values(
                sku_id=row['sku_id'],
                date_id=row['date_id'],
                price=row['price'],
                sales=row['sales'],
                revenue=row['revenue'],
            ).on_conflict_do_update(
                index_elements=['sku_id', 'date_id'],
                set_={
                    'price': row['price'],
                    'sales': row['sales'],
                    'revenue': row['revenue'],
                }
            )
            session.execute(stmt)
        session.commit()