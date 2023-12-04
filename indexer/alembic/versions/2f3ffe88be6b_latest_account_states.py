"""account_state_ts

Revision ID: 2f3ffe88be6b
Revises: d758b692c861
Create Date: 2023-11-10 19:42:47.950043

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column, select
from sqlalchemy.dialects import postgresql
from sqlalchemy import select, func


# revision identifiers, used by Alembic.
revision = '2f3ffe88be6b'
down_revision = '0880b4133aa6'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('latest_account_states',
    sa.Column('hash', sa.String(), nullable=False),
    sa.Column('account', sa.String(), nullable=False),
    sa.Column('last_trans_lt', sa.BigInteger(), nullable=False),
    sa.Column('timestamp', sa.BigInteger(), nullable=False),
    sa.Column('balance', sa.BigInteger(), nullable=False),
    sa.Column('account_status', postgresql.ENUM(name='account_status_type', create_type=False), nullable=False),
    sa.Column('frozen_hash', sa.String(), nullable=True),
    sa.Column('code_hash', sa.String(), nullable=True),
    sa.Column('data_hash', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('account')
    )

        # Data migration logic
    # Establish connection to the database
    bind = op.get_bind()
    metadata = sa.MetaData(bind=bind)

    # Define the tables
    transactions = sa.Table('transactions', metadata, autoload_with=bind)
    account_states = sa.Table('account_states', metadata, autoload_with=bind)
    latest_account_states = sa.Table('latest_account_states', metadata, autoload_with=bind)

    # Find the latest transaction for each account
    latest_transactions = select([
        transactions.c.account,
        func.max(transactions.c.lt).label('last_trans_lt')
    ]).group_by(transactions.c.account).alias('latest_transactions')

    # Join with transactions to get the account_state_hash_after and timestamp
    join_query = select([
        latest_transactions.c.account,
        latest_transactions.c.last_trans_lt,
        transactions.c.now.label('timestamp'),
        transactions.c.account_state_hash_after
    ]).select_from(
        latest_transactions.join(transactions, sa.and_(
            latest_transactions.c.account == transactions.c.account,
            latest_transactions.c.last_trans_lt == transactions.c.lt
        ))
    )

    # Execute the join query
    join_results = bind.execute(join_query)

    # For each result, find the corresponding account state and insert into latest_account_states
    for row in join_results:
        account_state = bind.execute(select([
            account_states
        ]).where(
            account_states.c.hash == row.account_state_hash_after
        )).fetchone()

        if account_state:
            insert_query = latest_account_states.insert().values(
                hash=row.account_state_hash_after,
                account=row.account,
                last_trans_lt=row.last_trans_lt,
                timestamp=row.timestamp,
                balance=account_state.balance,
                account_status=account_state.account_status,
                frozen_hash=account_state.frozen_hash,
                code_hash=account_state.code_hash,
                data_hash=account_state.data_hash
            )
            bind.execute(insert_query)

    op.create_index('latest_account_states_balance', 'latest_account_states', ['balance'], unique=False, postgresql_using='btree', postgresql_concurrently=False)


def downgrade():
    op.drop_table('latest_account_states')
