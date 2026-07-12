"""week4 workflows approvals evaluations sdk ingestion

Revision ID: 0004_week4
Revises: 0003_week3
Create Date: 2026-07-10
"""

import sqlalchemy as sa
from alembic import op

revision = "0004_week4"
down_revision = "0003_week3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workflows",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("organization_id", sa.String(length=36), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("project_id", sa.String(length=36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("slug", sa.String(length=180), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("latest_version_number", sa.Integer(), nullable=False),
        sa.Column("created_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("project_id", "slug", name="uq_workflows_project_slug"),
    )
    op.create_table(
        "workflow_versions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("workflow_id", sa.String(length=36), sa.ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("definition", sa.JSON(), nullable=False),
        sa.Column("definition_hash", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("workflow_id", "version_number", name="uq_workflow_versions_workflow_number"),
    )
    op.create_index("idx_workflows_project_created", "workflows", ["project_id", "created_at"])

    op.create_table(
        "approval_requests",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("organization_id", sa.String(length=36), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("project_id", sa.String(length=36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
        sa.Column("run_id", sa.String(length=36), sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("step_id", sa.String(length=36), sa.ForeignKey("run_steps.id", ondelete="CASCADE"), nullable=False),
        sa.Column("node_key", sa.String(length=200), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("input", sa.JSON(), nullable=False),
        sa.Column("decision", sa.JSON(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by", sa.String(length=36), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("decided_by", sa.String(length=36), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("run_id", "node_key", name="uq_approval_requests_run_node"),
    )
    op.create_index("idx_approvals_project_status", "approval_requests", ["project_id", "status", "created_at"])

    op.create_table(
        "datasets",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("organization_id", sa.String(length=36), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("project_id", sa.String(length=36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("slug", sa.String(length=180), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("project_id", "slug", name="uq_datasets_project_slug"),
    )
    op.create_table(
        "dataset_cases",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("dataset_id", sa.String(length=36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("input", sa.JSON(), nullable=False),
        sa.Column("expected_output", sa.JSON(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_dataset_cases_dataset", "dataset_cases", ["dataset_id", "created_at"])

    op.create_table(
        "experiments",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("organization_id", sa.String(length=36), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("project_id", sa.String(length=36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
        sa.Column("dataset_id", sa.String(length=36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("agent_version_id", sa.String(length=36), sa.ForeignKey("agent_versions.id"), nullable=True),
        sa.Column("workflow_version_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("summary", sa.JSON(), nullable=True),
        sa.Column("created_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "experiment_runs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("experiment_id", sa.String(length=36), sa.ForeignKey("experiments.id", ondelete="CASCADE"), nullable=False),
        sa.Column("dataset_case_id", sa.String(length=36), sa.ForeignKey("dataset_cases.id", ondelete="CASCADE"), nullable=False),
        sa.Column("run_id", sa.String(length=36), sa.ForeignKey("runs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("score", sa.Numeric(10, 4), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("experiment_id", "dataset_case_id", name="uq_experiment_runs_case"),
    )
    op.create_index("idx_experiment_runs_experiment", "experiment_runs", ["experiment_id", "created_at"])

    op.create_table(
        "api_keys",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("organization_id", sa.String(length=36), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("project_id", sa.String(length=36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("prefix", sa.String(length=24), nullable=False),
        sa.Column("secret_hash", sa.String(length=64), nullable=False),
        sa.Column("created_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("prefix", name="uq_api_keys_prefix"),
    )
    op.create_index("idx_api_keys_project", "api_keys", ["project_id", "created_at"])


def downgrade() -> None:
    op.drop_index("idx_api_keys_project", table_name="api_keys")
    op.drop_table("api_keys")
    op.drop_index("idx_experiment_runs_experiment", table_name="experiment_runs")
    op.drop_table("experiment_runs")
    op.drop_table("experiments")
    op.drop_index("idx_dataset_cases_dataset", table_name="dataset_cases")
    op.drop_table("dataset_cases")
    op.drop_table("datasets")
    op.drop_index("idx_approvals_project_status", table_name="approval_requests")
    op.drop_table("approval_requests")
    op.drop_index("idx_workflows_project_created", table_name="workflows")
    op.drop_table("workflow_versions")
    op.drop_table("workflows")
