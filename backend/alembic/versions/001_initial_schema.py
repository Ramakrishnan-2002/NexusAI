"""Initial database schema with pgvector and FTS support

Revision ID: 001_initial_schema
Revises: 
Create Date: 2026-09-03 08:45:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

try:
    from pgvector.sqlalchemy import Vector
    has_vector = True
except ImportError:
    has_vector = False

revision: str = '001_initial_schema'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Enable pgvector extension if PostgreSQL
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # 2. Articles table
    op.create_table(
        'articles',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('title', sa.String(length=512), nullable=False),
        sa.Column('wiki', sa.String(length=32), nullable=False),
        sa.Column('namespace', sa.Integer(), nullable=False),
        sa.Column('page_id', sa.Integer(), nullable=True),
        sa.Column('url', sa.String(length=1024), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_articles_title', 'articles', ['title'])
    op.create_index('ix_articles_wiki', 'articles', ['wiki'])
    op.create_index('ix_articles_wiki_title', 'articles', ['wiki', 'title'], unique=True)
    op.create_index('ix_articles_updated_at', 'articles', ['updated_at'])

    # 3. Editors table
    op.create_table(
        'editors',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('username', sa.String(length=256), nullable=False),
        sa.Column('is_bot', sa.Boolean(), nullable=False),
        sa.Column('edit_count', sa.Integer(), nullable=False),
        sa.Column('first_seen', sa.DateTime(timezone=True), nullable=False),
        sa.Column('last_seen', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_editors_username', 'editors', ['username'], unique=True)
    op.create_index('ix_editors_edit_count', 'editors', ['edit_count'])

    # 4. Edits table
    op.create_table(
        'edits',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('event_id', sa.String(length=128), nullable=False),
        sa.Column('article_id', sa.Integer(), nullable=False),
        sa.Column('editor_username', sa.String(length=256), nullable=False),
        sa.Column('is_bot', sa.Boolean(), nullable=False),
        sa.Column('is_minor', sa.Boolean(), nullable=False),
        sa.Column('revision_id', sa.BigInteger(), nullable=True),
        sa.Column('parent_revision_id', sa.BigInteger(), nullable=True),
        sa.Column('change_size', sa.Integer(), nullable=False),
        sa.Column('byte_diff', sa.Integer(), nullable=False),
        sa.Column('comment', sa.Text(), nullable=True),
        sa.Column('occurred_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['article_id'], ['articles.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_edits_event_id', 'edits', ['event_id'], unique=True)
    op.create_index('ix_edits_article_occurred', 'edits', ['article_id', 'occurred_at'])
    op.create_index('ix_edits_occurred_at', 'edits', ['occurred_at'])

    # 5. Activity Snapshots
    op.create_table(
        'activity_snapshots',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('article_id', sa.Integer(), nullable=False),
        sa.Column('window_seconds', sa.Integer(), nullable=False),
        sa.Column('edit_count', sa.Integer(), nullable=False),
        sa.Column('byte_delta', sa.Integer(), nullable=False),
        sa.Column('unique_editors', sa.Integer(), nullable=False),
        sa.Column('bot_ratio', sa.Float(), nullable=False),
        sa.Column('baseline_velocity', sa.Float(), nullable=False),
        sa.Column('spike_multiplier', sa.Float(), nullable=False),
        sa.Column('calculated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['article_id'], ['articles.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_activity_snapshots_article_calc', 'activity_snapshots', ['article_id', 'calculated_at'])

    # 6. Trend Events
    op.create_table(
        'trend_events',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('article_id', sa.Integer(), nullable=False),
        sa.Column('article_title', sa.String(length=512), nullable=False),
        sa.Column('topic', sa.String(length=128), nullable=False),
        sa.Column('activity_score', sa.Float(), nullable=False),
        sa.Column('edits_per_minute', sa.Float(), nullable=False),
        sa.Column('baseline_velocity', sa.Float(), nullable=False),
        sa.Column('spike_multiplier', sa.Float(), nullable=False),
        sa.Column('unique_editors', sa.Integer(), nullable=False),
        sa.Column('total_byte_delta', sa.Integer(), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('first_detected_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('peak_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('ai_summary', sa.Text(), nullable=True),
        sa.Column('ai_importance', sa.String(length=32), nullable=True),
        sa.Column('metadata_json', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['article_id'], ['articles.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_trend_events_status_score', 'trend_events', ['status', 'activity_score'])
    op.create_index('ix_trend_events_detected_at', 'trend_events', ['first_detected_at'])

    # 7. Knowledge Chunks with Vector and FTS support
    vector_col = Vector(384) if has_vector and bind.dialect.name == "postgresql" else sa.JSON()
    op.create_table(
        'knowledge_chunks',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('article_id', sa.Integer(), nullable=False),
        sa.Column('edit_id', sa.Integer(), nullable=True),
        sa.Column('revision_id', sa.Integer(), nullable=True),
        sa.Column('article_title', sa.String(length=512), nullable=False),
        sa.Column('title', sa.String(length=512), nullable=False),
        sa.Column('content', sa.Text(), nullable=False),
        sa.Column('embedding', vector_col, nullable=True),
        sa.Column('chunk_metadata', sa.JSON(), nullable=False),
        sa.Column('occurred_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['article_id'], ['articles.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_knowledge_chunks_article_occurred', 'knowledge_chunks', ['article_id', 'occurred_at'])

    # 8. AI Analyses
    op.create_table(
        'ai_analyses',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('trend_id', sa.Integer(), nullable=True),
        sa.Column('article_id', sa.Integer(), nullable=True),
        sa.Column('title', sa.String(length=512), nullable=False),
        sa.Column('summary', sa.Text(), nullable=False),
        sa.Column('importance', sa.String(length=32), nullable=False),
        sa.Column('detected_topic', sa.String(length=128), nullable=False),
        sa.Column('change_type', sa.String(length=64), nullable=False),
        sa.Column('reasoning', sa.Text(), nullable=False),
        sa.Column('confidence', sa.Float(), nullable=False),
        sa.Column('evidence_points', sa.JSON(), nullable=False),
        sa.Column('citations_json', sa.JSON(), nullable=False),
        sa.Column('provider', sa.String(length=64), nullable=False),
        sa.Column('model', sa.String(length=64), nullable=False),
        sa.Column('was_fallback', sa.Boolean(), nullable=False),
        sa.Column('latency_ms', sa.Float(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['trend_id'], ['trend_events.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_ai_analyses_created_at', 'ai_analyses', ['created_at'])

    # 9. Processing Jobs (Idempotency)
    op.create_table(
        'processing_jobs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('idempotency_key', sa.String(length=256), nullable=False),
        sa.Column('job_type', sa.String(length=64), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('retries', sa.Integer(), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('payload_json', sa.JSON(), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_processing_jobs_idempotency_key', 'processing_jobs', ['idempotency_key'], unique=True)


def downgrade() -> None:
    op.drop_table('processing_jobs')
    op.drop_table('ai_analyses')
    op.drop_table('knowledge_chunks')
    op.drop_table('trend_events')
    op.drop_table('activity_snapshots')
    op.drop_table('edits')
    op.drop_table('editors')
    op.drop_table('articles')
