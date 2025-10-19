"""POC-2 integration

Revision ID: 002
Revises: 001
Create Date: 2024-01-02 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add columns to strategies table
    op.add_column('strategies', sa.Column('code_content', sa.Text(), nullable=True))
    op.add_column('strategies', sa.Column('strategy_class', sa.String(length=100), nullable=True))
    
    # Add columns to backtests table
    op.add_column('backtests', sa.Column('job_id', sa.String(length=100), nullable=True))
    op.add_column('backtests', sa.Column('strategy_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column('backtests', sa.Column('timeframe', sa.String(length=10), nullable=True))
    
    # Add foreign key constraint for strategy_id
    op.create_foreign_key('fk_backtests_strategy_id', 'backtests', 'strategies', ['strategy_id'], ['id'], ondelete='CASCADE')
    
    # Add unique constraint for job_id
    op.create_unique_constraint('uq_backtests_job_id', 'backtests', ['job_id'])
    
    # Create indexes
    op.create_index('idx_backtests_job_id', 'backtests', ['job_id'], unique=False)
    op.create_index('idx_backtests_strategy_id', 'backtests', ['strategy_id'], unique=False)


def downgrade() -> None:
    # Drop indexes
    op.drop_index('idx_backtests_strategy_id', table_name='backtests')
    op.drop_index('idx_backtests_job_id', table_name='backtests')
    
    # Drop constraints
    op.drop_constraint('uq_backtests_job_id', 'backtests', type_='unique')
    op.drop_constraint('fk_backtests_strategy_id', 'backtests', type_='foreignkey')
    
    # Drop columns from backtests table
    op.drop_column('backtests', 'timeframe')
    op.drop_column('backtests', 'strategy_id')
    op.drop_column('backtests', 'job_id')
    
    # Drop columns from strategies table
    op.drop_column('strategies', 'strategy_class')
    op.drop_column('strategies', 'code_content')
