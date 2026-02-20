"""Refine trade results - drop trade_price_extremes, add MAE/MFE to backtest_trade_results

Revision ID: 004
Revises: 003
Create Date: 2026-02-20 00:00:00.000000

Changes:
- Drop trade_price_extremes (was added without explicit design intent)
- Add mfe_points, mfe_price, mfe_timestamp to backtest_trade_results
- Add mae_points, mae_price, mae_timestamp to backtest_trade_results

MAE/MFE now live on the trade row itself. Timestamps are sufficient to derive
duration-to-extreme; bar indexes are omitted as they are not portable across
backtests with different date ranges or timeframes.

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '004'
down_revision = '003'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # =====================================================
    # STEP 1: DROP trade_price_extremes
    # =====================================================
    # No dependents — nothing FKs into this table.
    # Indexes are dropped automatically with the table.
    op.drop_table('trade_price_extremes')

    # =====================================================
    # STEP 2: ADD MAE/MFE COLUMNS TO backtest_trade_results
    # =====================================================
    op.add_column('backtest_trade_results',
        sa.Column('mfe_points', sa.Numeric(precision=20, scale=8), nullable=True))
    op.add_column('backtest_trade_results',
        sa.Column('mfe_price', sa.Numeric(precision=20, scale=8), nullable=True))
    op.add_column('backtest_trade_results',
        sa.Column('mfe_timestamp', sa.TIMESTAMP(timezone=True), nullable=True))

    op.add_column('backtest_trade_results',
        sa.Column('mae_points', sa.Numeric(precision=20, scale=8), nullable=True))
    op.add_column('backtest_trade_results',
        sa.Column('mae_price', sa.Numeric(precision=20, scale=8), nullable=True))
    op.add_column('backtest_trade_results',
        sa.Column('mae_timestamp', sa.TIMESTAMP(timezone=True), nullable=True))


def downgrade() -> None:
    # Remove MAE/MFE columns from backtest_trade_results
    op.drop_column('backtest_trade_results', 'mae_timestamp')
    op.drop_column('backtest_trade_results', 'mae_price')
    op.drop_column('backtest_trade_results', 'mae_points')
    op.drop_column('backtest_trade_results', 'mfe_timestamp')
    op.drop_column('backtest_trade_results', 'mfe_price')
    op.drop_column('backtest_trade_results', 'mfe_points')

    # Recreate trade_price_extremes (structure only — data cannot be recovered)
    op.create_table(
        'trade_price_extremes',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('backtest_run_id', sa.Integer(), nullable=False),
        sa.Column('trade_id', sa.String(length=50), nullable=False),
        sa.Column('period_type', sa.String(length=30), nullable=False),
        sa.Column('period_start_timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('period_end_timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('period_start_bar_index', sa.Integer(), nullable=True),
        sa.Column('period_end_bar_index', sa.Integer(), nullable=True),
        sa.Column('max_favorable_excursion_points', sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column('max_adverse_excursion_points', sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column('mfe_timestamp', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('mae_timestamp', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('mfe_bar_index', sa.Integer(), nullable=True),
        sa.Column('mae_bar_index', sa.Integer(), nullable=True),
        sa.Column('mfe_price', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('mae_price', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=True,
                  server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(
            ['backtest_run_id', 'trade_id'],
            ['backtest_trade_results.backtest_run_id', 'backtest_trade_results.trade_id'],
            ondelete='CASCADE',
            name='fk_trade_extremes_trade'
        )
    )
    op.create_index('idx_trade_extremes_trade', 'trade_price_extremes', ['backtest_run_id', 'trade_id'])
    op.create_index('idx_trade_extremes_period', 'trade_price_extremes', ['period_type'])
