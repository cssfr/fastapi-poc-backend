"""Run lifecycle v1 - add run_steps + minimal run-level correlation/reproducibility fields

Revision ID: 005
Revises: 004
Create Date: 2026-02-22 00:00:00.000000

Changes:
- Add run_steps table (durable step timeline aligned with canonical step graph)
- Add minimal production fields to backtest_runs:
  - airflow_dag_id, airflow_run_id (orchestration correlation only)
  - engine_id, strategy_ref, deps_hash (reproducibility keys)
  - error_class, error_summary (terminal diagnosis)
- Change backtest_runs.status default from 'running' to 'queued'

Notes:
- This migration intentionally avoids introducing an artifacts table; artifacts can be
  referenced in backtest_runs.metadata until artifacts become a first-class output.
- run_steps is platform audit data (Supabase durable truth), not a mirror of Airflow metadb.

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '005'
down_revision = '004'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # =====================================================
    # STEP 1: CREATE run_steps (durable step timeline)
    # =====================================================
    op.create_table(
        'run_steps',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('run_id', sa.Integer(), nullable=False),
        sa.Column('step_name', sa.String(length=50), nullable=False),
        sa.Column('attempt', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='PENDING'),
        sa.Column('started_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('ended_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('error_class', sa.String(length=30), nullable=True),
        sa.Column('error_summary', sa.Text(), nullable=True),
        sa.Column('airflow_task_id', sa.String(length=250), nullable=True),
        sa.Column('worker_id', sa.String(length=250), nullable=True),
        sa.Column('metadata', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=True,
                  server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=True,
                  server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['run_id'], ['backtest_runs.id'], ondelete='CASCADE', name='fk_run_steps_run'),
        sa.UniqueConstraint('run_id', 'step_name', 'attempt', name='uq_run_steps_attempt')
    )

    op.create_index('idx_run_steps_run', 'run_steps', ['run_id'])
    op.create_index('idx_run_steps_run_status', 'run_steps', ['run_id', 'status'])
    op.create_index('idx_run_steps_run_step', 'run_steps', ['run_id', 'step_name'])

    # Optional: DB-level guardrails for canonical step/status values.
    # Kept as soft constraints for now (enforced in application logic) to avoid
    # churn if you rename/extend steps early.
    #
    # If you want hard constraints, uncomment and adjust as needed.
    # op.create_check_constraint(
    #     'ck_run_steps_status',
    #     'run_steps',
    #     "status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','SKIPPED')"
    # )

    # Add updated_at trigger (function already created in revision 003)
    op.execute("""
        CREATE TRIGGER update_run_steps_updated_at
        BEFORE UPDATE ON run_steps
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)

    # =====================================================
    # STEP 2: ADD MINIMAL PRODUCTION FIELDS TO backtest_runs
    # =====================================================

    # Orchestration correlation (minimal)
    op.add_column('backtest_runs', sa.Column('airflow_dag_id', sa.String(length=250), nullable=True))
    op.add_column('backtest_runs', sa.Column('airflow_run_id', sa.String(length=250), nullable=True))

    # Reproducibility keys (minimal)
    op.add_column('backtest_runs', sa.Column('engine_id', sa.String(length=100), nullable=True))
    op.add_column('backtest_runs', sa.Column('strategy_ref', sa.String(length=250), nullable=True))
    op.add_column('backtest_runs', sa.Column('deps_hash', sa.String(length=128), nullable=True))

    # Terminal diagnosis (minimal)
    op.add_column('backtest_runs', sa.Column('error_class', sa.String(length=30), nullable=True))
    op.add_column('backtest_runs', sa.Column('error_summary', sa.Text(), nullable=True))

    # Useful indexes for filtering/auditing (keep minimal)
    op.create_index('idx_backtest_runs_status_created_at', 'backtest_runs', ['status', 'created_at'])
    op.create_index('idx_backtest_runs_user_created_at', 'backtest_runs', ['user_id', 'created_at'])

    # =====================================================
    # STEP 3: CHANGE status DEFAULT TO 'queued'
    # =====================================================
    # We intentionally do not rewrite existing rows; only the default for new rows.
    op.alter_column('backtest_runs', 'status',
                    existing_type=sa.String(length=50),
                    server_default='queued')


def downgrade() -> None:
    # Revert status default to previous value ('running' in earlier schema)
    op.alter_column('backtest_runs', 'status',
                    existing_type=sa.String(length=50),
                    server_default='running')

    # Drop indexes added to backtest_runs
    op.drop_index('idx_backtest_runs_user_created_at', table_name='backtest_runs')
    op.drop_index('idx_backtest_runs_status_created_at', table_name='backtest_runs')

    # Drop columns added to backtest_runs
    op.drop_column('backtest_runs', 'error_summary')
    op.drop_column('backtest_runs', 'error_class')
    op.drop_column('backtest_runs', 'deps_hash')
    op.drop_column('backtest_runs', 'strategy_ref')
    op.drop_column('backtest_runs', 'engine_id')
    op.drop_column('backtest_runs', 'airflow_run_id')
    op.drop_column('backtest_runs', 'airflow_dag_id')

    # Drop trigger + run_steps table
    op.execute("DROP TRIGGER IF EXISTS update_run_steps_updated_at ON run_steps")
    op.drop_index('idx_run_steps_run_step', table_name='run_steps')
    op.drop_index('idx_run_steps_run_status', table_name='run_steps')
    op.drop_index('idx_run_steps_run', table_name='run_steps')
    op.drop_table('run_steps')
