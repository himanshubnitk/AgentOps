"""week3 observability cost artifacts

Revision ID: 0003_week3
Revises: 0002_week2
Create Date: 2026-07-05
"""

import sqlalchemy as sa
from alembic import op

revision = "0003_week3"
down_revision = "0002_week2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "usage_records",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "run_id",
            sa.String(length=36),
            sa.ForeignKey("runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "step_id",
            sa.String(length=36),
            sa.ForeignKey("run_steps.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("model", sa.String(length=160), nullable=False),
        sa.Column("input_tokens", sa.BigInteger(), nullable=False),
        sa.Column("output_tokens", sa.BigInteger(), nullable=False),
        sa.Column("cached_input_tokens", sa.BigInteger(), nullable=False),
        sa.Column("reasoning_tokens", sa.BigInteger(), nullable=False),
        sa.Column("input_unit_price", sa.Numeric(20, 10), nullable=False),
        sa.Column("output_unit_price", sa.Numeric(20, 10), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("calculated_cost", sa.Numeric(20, 10), nullable=False),
        sa.Column("pricing_version", sa.String(length=120), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_usage_run", "usage_records", ["run_id"])

    op.create_table(
        "artifacts",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "run_id",
            sa.String(length=36),
            sa.ForeignKey("runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "step_id",
            sa.String(length=36),
            sa.ForeignKey("run_steps.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("kind", sa.String(length=80), nullable=False),
        sa.Column("storage_uri", sa.Text(), nullable=False),
        sa.Column("content_type", sa.String(length=160), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_artifacts_run", "artifacts", ["run_id"])


def downgrade() -> None:
    op.drop_index("idx_artifacts_run", table_name="artifacts")
    op.drop_table("artifacts")
    op.drop_index("idx_usage_run", table_name="usage_records")
    op.drop_table("usage_records")
