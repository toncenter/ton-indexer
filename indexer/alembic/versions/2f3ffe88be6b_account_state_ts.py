"""account_state_ts

Revision ID: 2f3ffe88be6b
Revises: d758b692c861
Create Date: 2023-11-10 19:42:47.950043

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column, select


# revision identifiers, used by Alembic.
revision = '2f3ffe88be6b'
down_revision = 'd758b692c861'
branch_labels = None
depends_on = None


def upgrade():
    # Step 1: Add the new column 'timestamp' to 'account_states'
    op.add_column('account_states', sa.Column('timestamp', sa.Integer, nullable=True))

    # Step 2: Data Migration
    # Define the tables
    account_states = table('account_states',
                           column('hash', sa.String),
                           column('timestamp', sa.Integer))
    transactions = table('transactions',
                         column('account_state_hash_after', sa.String),
                         column('now', sa.Integer))

    # Perform a subquery to find the corresponding 'now' value for each account state
    subquery = select([transactions.c.now]).where(transactions.c.account_state_hash_after == account_states.c.hash).limit(1).as_scalar()

    # Update the 'timestamp' in 'account_states'
    op.execute(
        account_states.update().values(timestamp=subquery)
    )

    # Step 3: Finalize the Column
    # If required, make 'timestamp' non-nullable
    op.alter_column('account_states', 'timestamp', nullable=False)


def downgrade():
    op.drop_column('account_states', 'timestamp')
