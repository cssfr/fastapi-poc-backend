"""Schema refactor - Multi-user backtesting architecture

Revision ID: 003
Revises: 002
Create Date: 2025-02-20 00:00:00.000000

Major changes:
- Split strategies into strategy_definitions + strategy_configurations
- Remove analytics columns from backtests (compute on-demand)
- Remove code_content from strategies (code lives in filesystem)
- Remove job_id from backtests (orchestrator concern)
- Rename backtests → backtest_runs
- Replace trades → backtest_trade_results (complete restructure)
- Add trade_price_extremes (MFE/MAE tracking)
- Add trade_events (order ledger)
- Add user_id to all tables for multi-user support

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '003'
down_revision = '002'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # =====================================================
    # STEP 1: DROP OLD TABLES
    # =====================================================
    # Note: Assumes data is not needed. If data preservation required,
    # add data migration logic before dropping.
    
    op.drop_table('trades')
    op.drop_table('backtests')
    op.drop_table('strategies')
    
    # =====================================================
    # STEP 2: CREATE STRATEGY MANAGEMENT TABLES
    # =====================================================
    
    # strategy_definitions
    op.create_table(
        'strategy_definitions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('strategy_id', sa.String(length=100), nullable=False),
        sa.Column('strategy_class', sa.String(length=100), nullable=False),
        sa.Column('display_name', sa.String(length=255), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('schema_version', sa.String(length=20), nullable=False, server_default='1.0.0'),
        sa.Column('parameter_schema', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('execution_schema', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('enabled', sa.Boolean(), nullable=True, server_default='true'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('created_by', sa.String(length=100), nullable=True),
        sa.Column('updated_by', sa.String(length=100), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('strategy_id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE')
    )
    op.create_index('idx_strategy_definitions_user', 'strategy_definitions', ['user_id'])
    op.create_index('idx_strategy_definitions_class', 'strategy_definitions', ['strategy_class'])
    op.create_index('idx_strategy_definitions_schema_version', 'strategy_definitions', ['schema_version'])
    op.create_index('idx_strategy_definitions_enabled', 'strategy_definitions', ['enabled'])
    
    # strategy_configurations
    op.create_table(
        'strategy_configurations',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('config_id', sa.String(length=100), nullable=False),
        sa.Column('strategy_definition_id', sa.Integer(), nullable=True),
        sa.Column('instance_name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('parameters', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('symbol', sa.String(length=50), nullable=False),
        sa.Column('timeframe', sa.String(length=20), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=True, server_default='true'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('created_by', sa.String(length=100), nullable=True),
        sa.Column('updated_by', sa.String(length=100), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('config_id'),
        sa.UniqueConstraint('strategy_definition_id', 'symbol', 'timeframe', name='strategy_configurations_strategy_definition_id_symbol_timeframe'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['strategy_definition_id'], ['strategy_definitions.id'], ondelete='CASCADE')
    )
    op.create_index('idx_strategy_configurations_user', 'strategy_configurations', ['user_id'])
    op.create_index('idx_strategy_configurations_symbol_timeframe', 'strategy_configurations', ['symbol', 'timeframe'])
    op.create_index('idx_strategy_configurations_enabled', 'strategy_configurations', ['enabled'])
    
    # =====================================================
    # STEP 3: CREATE BACKTEST EXECUTION TABLES
    # =====================================================
    
    # backtest_runs
    op.create_table(
        'backtest_runs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('backtest_id', sa.String(length=50), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('strategy_name', sa.String(length=100), nullable=False),
        sa.Column('strategy_configuration_id', sa.Integer(), nullable=True),
        sa.Column('strategy_parameters', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('symbol', sa.String(length=20), nullable=False),
        sa.Column('timeframe', sa.String(length=10), nullable=False),
        sa.Column('start_date', sa.Date(), nullable=False),
        sa.Column('end_date', sa.Date(), nullable=False),
        sa.Column('data_start_timestamp', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('data_end_timestamp', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('data_source', sa.String(length=50), nullable=True),
        sa.Column('data_points_count', sa.Integer(), nullable=True),
        sa.Column('initial_balance', sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=True, server_default='running'),
        sa.Column('started_at', sa.TIMESTAMP(timezone=True), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('completed_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('execution_time_ms', sa.Integer(), nullable=True),
        sa.Column('total_trades', sa.Integer(), nullable=True, server_default='0'),
        sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('backtest_id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['strategy_configuration_id'], ['strategy_configurations.id'], ondelete='SET NULL')
    )
    op.create_index('idx_backtest_runs_user', 'backtest_runs', ['user_id'])
    op.create_index('idx_backtest_runs_strategy', 'backtest_runs', ['strategy_name'])
    op.create_index('idx_backtest_runs_config', 'backtest_runs', ['strategy_configuration_id'])
    op.create_index('idx_backtest_runs_symbol', 'backtest_runs', ['symbol'])
    op.create_index('idx_backtest_runs_date_range', 'backtest_runs', ['start_date', 'end_date'])
    op.create_index('idx_backtest_runs_status', 'backtest_runs', ['status'])
    op.create_index('idx_backtest_runs_created', 'backtest_runs', ['created_at'])
    
    # backtest_trade_results
    op.create_table(
        'backtest_trade_results',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('backtest_run_id', sa.Integer(), nullable=False),
        sa.Column('trade_id', sa.String(length=50), nullable=False),
        sa.Column('symbol', sa.String(length=20), nullable=False),
        sa.Column('trade_date', sa.Date(), nullable=False),
        sa.Column('side', sa.String(length=10), nullable=False),
        sa.Column('entry_timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('entry_bar_index', sa.Integer(), nullable=False),
        sa.Column('entry_price', sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column('exit_timestamp', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('exit_bar_index', sa.Integer(), nullable=True),
        sa.Column('exit_price', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('exit_reason', sa.String(length=50), nullable=True),
        sa.Column('initial_stop_loss', sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column('initial_take_profit', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('final_stop_loss', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('final_take_profit', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('pnl_points', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('session_high', sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column('session_low', sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column('holding_period_minutes', sa.Integer(), nullable=True),
        sa.Column('holding_period_bars', sa.Integer(), nullable=True),
        sa.Column('entry_slippage_points', sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column('exit_slippage_points', sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column('strategy_context', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default='{}'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('backtest_run_id', 'trade_id', name='backtest_trade_results_backtest_run_id_trade_id_key'),
        sa.ForeignKeyConstraint(['backtest_run_id'], ['backtest_runs.id'], ondelete='CASCADE'),
        sa.CheckConstraint("side IN ('long', 'short')", name='check_side_valid')
    )
    op.create_index('idx_trade_results_backtest', 'backtest_trade_results', ['backtest_run_id'])
    op.create_index('idx_trade_results_date', 'backtest_trade_results', ['trade_date'])
    op.create_index('idx_trade_results_side', 'backtest_trade_results', ['side'])
    op.create_index('idx_trade_results_exit_reason', 'backtest_trade_results', ['exit_reason'])
    op.create_index('idx_trade_results_strategy_context', 'backtest_trade_results', ['strategy_context'], postgresql_using='gin')
    
    # trade_price_extremes
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
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['backtest_run_id', 'trade_id'], ['backtest_trade_results.backtest_run_id', 'backtest_trade_results.trade_id'], ondelete='CASCADE', name='fk_trade_extremes_trade')
    )
    op.create_index('idx_trade_extremes_trade', 'trade_price_extremes', ['backtest_run_id', 'trade_id'])
    op.create_index('idx_trade_extremes_period', 'trade_price_extremes', ['period_type'])
    
    # trade_events
    op.create_table(
        'trade_events',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('backtest_run_id', sa.Integer(), nullable=False),
        sa.Column('trade_id', sa.String(length=50), nullable=False),
        sa.Column('event_timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('event_sequence', sa.Integer(), nullable=False),
        sa.Column('event_type', sa.String(length=30), nullable=False),
        sa.Column('order_type', sa.String(length=20), nullable=True),
        sa.Column('order_side', sa.String(length=10), nullable=True),
        sa.Column('order_quantity', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('order_price', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('fill_price', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('fill_quantity', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('slippage_bps', sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column('execution_delay_ms', sa.Integer(), nullable=True),
        sa.Column('previous_stop_loss', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('new_stop_loss', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('stop_loss_reason', sa.String(length=100), nullable=True),
        sa.Column('previous_take_profit', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('new_take_profit', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('take_profit_reason', sa.String(length=100), nullable=True),
        sa.Column('bar_index', sa.Integer(), nullable=True),
        sa.Column('trigger_price', sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column('tag', sa.String(length=50), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('backtest_run_id', 'trade_id', 'event_sequence', name='uq_trade_events_sequence'),
        sa.ForeignKeyConstraint(['backtest_run_id'], ['backtest_runs.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['backtest_run_id', 'trade_id'], ['backtest_trade_results.backtest_run_id', 'backtest_trade_results.trade_id'], ondelete='CASCADE', name='fk_trade_events_trade')
    )
    op.create_index('idx_trade_events_backtest', 'trade_events', ['backtest_run_id', 'event_timestamp'])
    op.create_index('idx_trade_events_trade', 'trade_events', ['trade_id', 'event_sequence'])
    op.create_index('idx_trade_events_type', 'trade_events', ['event_type'])
    
    # =====================================================
    # STEP 4: CREATE TRIGGERS FOR UPDATED_AT
    # =====================================================
    
    # Create function if it doesn't exist
    op.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    
    # Apply to tables
    op.execute("""
        CREATE TRIGGER update_strategy_definitions_updated_at
        BEFORE UPDATE ON strategy_definitions
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)
    
    op.execute("""
        CREATE TRIGGER update_strategy_configurations_updated_at
        BEFORE UPDATE ON strategy_configurations
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)
    
    op.execute("""
        CREATE TRIGGER update_backtest_runs_updated_at
        BEFORE UPDATE ON backtest_runs
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)
    
    op.execute("""
        CREATE TRIGGER update_backtest_trade_results_updated_at
        BEFORE UPDATE ON backtest_trade_results
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)
    
    # =====================================================
    # STEP 5: CREATE COMPATIBILITY VIEWS
    # =====================================================
    # These views allow old frontend/backend code to keep working
    # while you migrate to new schema. Remove these views once
    # all code is updated.
    
    # strategies view (maps to strategy_configurations)
    op.execute("""
        CREATE VIEW strategies AS
        SELECT 
            ('00000000-0000-0000-0000-' || lpad(sc.id::text, 12, '0'))::uuid as id,
            sc.user_id,
            sc.instance_name as name,
            sc.description,
            sc.parameters,
            false as is_public,
            sc.created_at,
            sc.updated_at,
            NULL::text as code_content,
            sd.strategy_class
        FROM strategy_configurations sc
        LEFT JOIN strategy_definitions sd ON sd.id = sc.strategy_definition_id;
    """)
    
    # backtests view (maps to backtest_runs)
    op.execute("""
        CREATE VIEW backtests AS
        SELECT 
            ('00000000-0000-0000-0000-' || lpad(br.id::text, 12, '0'))::uuid as id,
            br.user_id,
            br.name,
            br.strategy_name as strategy,
            br.symbol,
            br.start_date,
            br.end_date,
            br.initial_balance as initial_capital,
            NULL::numeric as final_value,
            NULL::numeric as total_return,
            NULL::numeric as max_drawdown,
            NULL::numeric as sharpe_ratio,
            NULL::numeric as win_rate,
            br.total_trades,
            br.status,
            br.created_at,
            br.updated_at,
            NULL::varchar(100) as job_id,
            ('00000000-0000-0000-0000-' || lpad(COALESCE(br.strategy_configuration_id, 0)::text, 12, '0'))::uuid as strategy_id,
            br.timeframe
        FROM backtest_runs br;
    """)
    
    # trades view (maps to backtest_trade_results)
    op.execute("""
        CREATE VIEW trades AS
        SELECT 
            ('00000000-0000-0000-0000-' || lpad(btr.id::text, 12, '0'))::uuid as id,
            ('00000000-0000-0000-0000-' || lpad(btr.backtest_run_id::text, 12, '0'))::uuid as backtest_id,
            btr.side as trade_type,
            btr.symbol,
            1.0 as quantity,  -- No quantity in new schema (points-based)
            btr.entry_price as price,
            btr.entry_timestamp as timestamp,
            btr.created_at
        FROM backtest_trade_results btr;
    """)
    
    # =====================================================
    # STEP 6: CREATE INSTEAD OF TRIGGERS FOR WRITABLE VIEWS
    # =====================================================
    # These make the compatibility views accept INSERT/UPDATE/DELETE,
    # routing writes to the new tables with best-effort field mapping.
    # Analytics fields that no longer exist (final_value, etc.) are silently ignored.

    # --- strategies view ---
    op.execute("""
        CREATE OR REPLACE FUNCTION strategies_instead_insert()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO strategy_configurations (
                user_id, config_id, instance_name, description,
                parameters, symbol, timeframe
            ) VALUES (
                NEW.user_id,
                gen_random_uuid()::text,
                NEW.name,
                NEW.description,
                COALESCE(NEW.parameters, '{}'::jsonb),
                'UNKNOWN',
                'D'
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER strategies_insert
        INSTEAD OF INSERT ON strategies
        FOR EACH ROW EXECUTE FUNCTION strategies_instead_insert();
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION strategies_instead_update()
        RETURNS TRIGGER AS $$
        BEGIN
            UPDATE strategy_configurations SET
                instance_name = NEW.name,
                description = NEW.description,
                parameters = COALESCE(NEW.parameters, '{}'::jsonb),
                updated_at = CURRENT_TIMESTAMP
            WHERE ('00000000-0000-0000-0000-' || lpad(id::text, 12, '0'))::uuid = NEW.id
              AND user_id = NEW.user_id;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER strategies_update
        INSTEAD OF UPDATE ON strategies
        FOR EACH ROW EXECUTE FUNCTION strategies_instead_update();
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION strategies_instead_delete()
        RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM strategy_configurations
            WHERE ('00000000-0000-0000-0000-' || lpad(id::text, 12, '0'))::uuid = OLD.id
              AND user_id = OLD.user_id;
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER strategies_delete
        INSTEAD OF DELETE ON strategies
        FOR EACH ROW EXECUTE FUNCTION strategies_instead_delete();
    """)

    # --- backtests view ---
    op.execute("""
        CREATE OR REPLACE FUNCTION backtests_instead_insert()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO backtest_runs (
                user_id, backtest_id, name, strategy_name, symbol, timeframe,
                start_date, end_date, initial_balance, status,
                strategy_parameters, total_trades
            ) VALUES (
                NEW.user_id,
                gen_random_uuid()::text,
                NEW.name,
                COALESCE(NEW.strategy, 'unknown'),
                NEW.symbol,
                COALESCE(NEW.timeframe, 'D'),
                NEW.start_date,
                NEW.end_date,
                COALESCE(NEW.initial_capital, 10000),
                COALESCE(NEW.status, 'pending'),
                '{}'::jsonb,
                COALESCE(NEW.total_trades, 0)
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER backtests_insert
        INSTEAD OF INSERT ON backtests
        FOR EACH ROW EXECUTE FUNCTION backtests_instead_insert();
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION backtests_instead_update()
        RETURNS TRIGGER AS $$
        BEGIN
            UPDATE backtest_runs SET
                name = COALESCE(NEW.name, name),
                status = COALESCE(NEW.status, status),
                total_trades = COALESCE(NEW.total_trades, total_trades),
                updated_at = CURRENT_TIMESTAMP
            WHERE ('00000000-0000-0000-0000-' || lpad(id::text, 12, '0'))::uuid = NEW.id
              AND user_id = NEW.user_id;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER backtests_update
        INSTEAD OF UPDATE ON backtests
        FOR EACH ROW EXECUTE FUNCTION backtests_instead_update();
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION backtests_instead_delete()
        RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM backtest_runs
            WHERE ('00000000-0000-0000-0000-' || lpad(id::text, 12, '0'))::uuid = OLD.id
              AND user_id = OLD.user_id;
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER backtests_delete
        INSTEAD OF DELETE ON backtests
        FOR EACH ROW EXECUTE FUNCTION backtests_instead_delete();
    """)

    # --- trades view ---
    op.execute("""
        CREATE OR REPLACE FUNCTION trades_instead_insert()
        RETURNS TRIGGER AS $$
        DECLARE
            run_id integer;
        BEGIN
            SELECT id INTO run_id
            FROM backtest_runs
            WHERE ('00000000-0000-0000-0000-' || lpad(id::text, 12, '0'))::uuid = NEW.backtest_id
            LIMIT 1;

            IF run_id IS NULL THEN
                RAISE EXCEPTION 'backtest_run not found for backtest_id %', NEW.backtest_id;
            END IF;

            INSERT INTO backtest_trade_results (
                backtest_run_id, trade_id, symbol, trade_date, side,
                entry_timestamp, entry_bar_index, entry_price,
                initial_stop_loss, session_high, session_low,
                strategy_context
            ) VALUES (
                run_id,
                gen_random_uuid()::text,
                NEW.symbol,
                NEW.timestamp::date,
                CASE WHEN NEW.trade_type = 'buy' THEN 'long' ELSE 'short' END,
                NEW.timestamp,
                0,
                NEW.price,
                0,
                NEW.price,
                NEW.price
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trades_insert
        INSTEAD OF INSERT ON trades
        FOR EACH ROW EXECUTE FUNCTION trades_instead_insert();
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION trades_instead_delete()
        RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM backtest_trade_results
            WHERE ('00000000-0000-0000-0000-' || lpad(id::text, 12, '0'))::uuid = OLD.id;
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trades_delete
        INSTEAD OF DELETE ON trades
        FOR EACH ROW EXECUTE FUNCTION trades_instead_delete();
    """)

    op.execute("COMMENT ON VIEW strategies IS 'Compatibility view for old code. Maps strategy_configurations to old strategies table structure. Remove when migration complete.'")
    op.execute("COMMENT ON VIEW backtests IS 'Compatibility view for old code. Maps backtest_runs to old backtests table structure. Analytics columns return NULL. Remove when migration complete.'")
    op.execute("COMMENT ON VIEW trades IS 'Compatibility view for old code. Maps backtest_trade_results to old trades table structure. Limited data mapping. Remove when migration complete.'")


def downgrade() -> None:
    # Drop compatibility views first (CASCADE also drops their INSTEAD OF triggers)
    op.execute("DROP VIEW IF EXISTS trades CASCADE")
    op.execute("DROP VIEW IF EXISTS backtests CASCADE")
    op.execute("DROP VIEW IF EXISTS strategies CASCADE")

    # Drop INSTEAD OF trigger functions (triggers were dropped with their views above)
    op.execute("DROP FUNCTION IF EXISTS trades_instead_delete()")
    op.execute("DROP FUNCTION IF EXISTS trades_instead_insert()")
    op.execute("DROP FUNCTION IF EXISTS backtests_instead_delete()")
    op.execute("DROP FUNCTION IF EXISTS backtests_instead_update()")
    op.execute("DROP FUNCTION IF EXISTS backtests_instead_insert()")
    op.execute("DROP FUNCTION IF EXISTS strategies_instead_delete()")
    op.execute("DROP FUNCTION IF EXISTS strategies_instead_update()")
    op.execute("DROP FUNCTION IF EXISTS strategies_instead_insert()")
    
    # Drop triggers
    op.execute("DROP TRIGGER IF EXISTS update_backtest_trade_results_updated_at ON backtest_trade_results")
    op.execute("DROP TRIGGER IF EXISTS update_backtest_runs_updated_at ON backtest_runs")
    op.execute("DROP TRIGGER IF EXISTS update_strategy_configurations_updated_at ON strategy_configurations")
    op.execute("DROP TRIGGER IF EXISTS update_strategy_definitions_updated_at ON strategy_definitions")
    op.execute("DROP FUNCTION IF EXISTS update_updated_at_column()")
    
    # Drop new tables
    op.drop_table('trade_events')
    op.drop_table('trade_price_extremes')
    op.drop_table('backtest_trade_results')
    op.drop_table('backtest_runs')
    op.drop_table('strategy_configurations')
    op.drop_table('strategy_definitions')
    
    # Recreate old tables (basic structure - data cannot be recovered)
    op.create_table(
        'strategies',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('parameters', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('is_public', sa.Boolean(), nullable=True, server_default='false'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('code_content', sa.Text(), nullable=True),
        sa.Column('strategy_class', sa.String(length=100), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE')
    )
    
    op.create_table(
        'backtests',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('strategy', sa.String(length=255), nullable=False),
        sa.Column('symbol', sa.String(length=20), nullable=False),
        sa.Column('start_date', sa.Date(), nullable=False),
        sa.Column('end_date', sa.Date(), nullable=False),
        sa.Column('initial_capital', sa.Numeric(), nullable=False),
        sa.Column('final_value', sa.Numeric(), nullable=True),
        sa.Column('total_return', sa.Numeric(), nullable=True),
        sa.Column('max_drawdown', sa.Numeric(), nullable=True),
        sa.Column('sharpe_ratio', sa.Numeric(), nullable=True),
        sa.Column('win_rate', sa.Numeric(), nullable=True),
        sa.Column('total_trades', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(length=50), server_default='pending'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('job_id', sa.String(length=100), nullable=True),
        sa.Column('strategy_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('timeframe', sa.String(length=10), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('job_id', name='uq_backtests_job_id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['strategy_id'], ['strategies.id'], ondelete='CASCADE', name='fk_backtests_strategy_id')
    )
    
    op.create_index('idx_backtests_job_id', 'backtests', ['job_id'])
    op.create_index('idx_backtests_strategy_id', 'backtests', ['strategy_id'])
    
    op.create_table(
        'trades',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('backtest_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('trade_type', sa.String(length=10), nullable=False),
        sa.Column('symbol', sa.String(length=20), nullable=False),
        sa.Column('quantity', sa.Numeric(), nullable=False),
        sa.Column('price', sa.Numeric(), nullable=False),
        sa.Column('timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['backtest_id'], ['backtests.id'], ondelete='CASCADE')
    )
