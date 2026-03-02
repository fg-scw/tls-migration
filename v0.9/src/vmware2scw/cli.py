"""CLI entry point for vmware2scw."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from vmware2scw.config import AppConfig, BatchMigrationPlan, VMMigrationPlan

console = Console()


def load_config(config_path: str | None) -> AppConfig:
    """Load configuration from file or environment."""
    if config_path:
        return AppConfig.from_yaml(config_path)
    try:
        return AppConfig.from_env_and_args()
    except Exception as e:
        console.print(f"[red]Error loading config: {e}[/red]")
        console.print("Provide a --config file or set environment variables.")
        sys.exit(1)


@click.group()
@click.version_option(version="0.1.0", prog_name="vmware2scw")
def main():
    """VMware to Scaleway Instance migration tool.

    Migrate virtual machines from VMware vSphere/vCenter environments
    to Scaleway Instances (KVM/qcow2).
    """
    pass


@main.command()
@click.option("--vcenter", required=True, help="vCenter hostname or IP")
@click.option("--username", required=True, help="vCenter username")
@click.option("--password-file", type=click.Path(exists=True), help="File containing vCenter password")
@click.option("--password", help="vCenter password (prefer --password-file)")
@click.option("--insecure", is_flag=True, default=False, help="Skip SSL verification")
@click.option("--output", "-o", type=click.Path(), help="Output file (JSON)")
@click.option("--format", "fmt", type=click.Choice(["table", "json"]), default="table")
def inventory(vcenter: str, username: str, password_file: str | None, password: str | None,
              insecure: bool, output: str | None, fmt: str):
    """List all VMs in a vCenter environment with their specifications."""
    from vmware2scw.vmware.client import VSphereClient
    from vmware2scw.vmware.inventory import VMInventory

    # Resolve password
    if password_file:
        password = Path(password_file).read_text().strip()
    elif not password:
        password = click.prompt("vCenter password", hide_input=True)

    with console.status("[bold green]Connecting to vCenter..."):
        client = VSphereClient()
        client.connect(vcenter, username, password, insecure=insecure)

    with console.status("[bold green]Collecting VM inventory..."):
        inv = VMInventory(client)
        vms = inv.list_all_vms()

    if fmt == "json":
        data = [vm.model_dump() for vm in vms]
        if output:
            Path(output).write_text(json.dumps(data, indent=2, default=str))
            console.print(f"[green]Inventory saved to {output}[/green]")
        else:
            console.print_json(json.dumps(data, indent=2, default=str))
    else:
        table = Table(title=f"VM Inventory — {vcenter}")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("State", style="green")
        table.add_column("CPU", justify="right")
        table.add_column("RAM (MB)", justify="right")
        table.add_column("Disks", justify="right")
        table.add_column("Total (GB)", justify="right")
        table.add_column("OS", style="magenta")
        table.add_column("Firmware")
        table.add_column("Tools")

        for vm in vms:
            total_gb = sum(d.size_gb for d in vm.disks)
            table.add_row(
                vm.name,
                vm.power_state,
                str(vm.cpu),
                str(vm.memory_mb),
                str(len(vm.disks)),
                f"{total_gb:.1f}",
                vm.guest_os_full or vm.guest_os,
                vm.firmware,
                vm.tools_status,
            )

        console.print(table)
        console.print(f"\n[dim]Total: {len(vms)} VMs[/dim]")

    client.disconnect()


@main.command()
@click.option("--vm", required=True, help="VM name to validate")
@click.option("--target-type", required=True, help="Scaleway instance type (e.g. PRO2-S)")
@click.option("--config", "config_path", type=click.Path(exists=True), help="Configuration file")
def validate(vm: str, target_type: str, config_path: str | None):
    """Validate that a VM can be migrated to a Scaleway instance type."""
    config = load_config(config_path)

    from vmware2scw.pipeline.validator import MigrationValidator
    from vmware2scw.vmware.client import VSphereClient
    from vmware2scw.vmware.inventory import VMInventory

    with console.status("[bold green]Connecting to vCenter..."):
        client = VSphereClient()
        pw = config.vmware.password.get_secret_value() if config.vmware.password else ""
        client.connect(config.vmware.vcenter, config.vmware.username, pw, insecure=config.vmware.insecure)

    with console.status(f"[bold green]Fetching VM info for '{vm}'..."):
        inv = VMInventory(client)
        vm_info = inv.get_vm_info(vm)

    validator = MigrationValidator()
    report = validator.validate(vm_info, target_type)

    if report.passed:
        console.print(f"\n[bold green]✅ Validation passed[/bold green] — '{vm}' is compatible with {target_type}")
    else:
        console.print(f"\n[bold red]❌ Validation failed[/bold red] — '{vm}' has compatibility issues:")

    for check in report.checks:
        icon = "✅" if check.passed else "❌" if check.blocking else "⚠️"
        console.print(f"  {icon} {check.name}: {check.message}")

    client.disconnect()

    if not report.passed:
        sys.exit(1)


@main.command()
@click.option("--vm", required=True, help="VM name to migrate")
@click.option("--target-type", required=True, help="Scaleway instance type")
@click.option("--zone", default="fr-par-1", help="Scaleway availability zone")
@click.option("--config", "config_path", type=click.Path(exists=True), help="Configuration file")
@click.option("--skip-validation", is_flag=True, default=False, help="Skip pre-validation")
@click.option("--dry-run", is_flag=True, default=False, help="Simulate without executing")
def migrate(vm: str, target_type: str, zone: str, config_path: str | None,
            skip_validation: bool, dry_run: bool):
    """Migrate a single VM from VMware to Scaleway."""
    config = load_config(config_path)

    plan = VMMigrationPlan(
        vm_name=vm,
        target_type=target_type,
        zone=zone,
        skip_validation=skip_validation,
    )

    from vmware2scw.pipeline.migration import MigrationPipeline

    pipeline = MigrationPipeline(config)

    if dry_run:
        console.print("[yellow]DRY RUN — No changes will be made[/yellow]")
        pipeline.dry_run(plan)
    else:
        result = pipeline.run(plan)
        if result.success:
            console.print(f"\n[bold green]✅ Migration complete![/bold green]")
            console.print(f"  Scaleway Instance ID: {result.instance_id}")
            console.print(f"  Duration: {result.duration}")
        else:
            console.print(f"\n[bold red]❌ Migration failed at stage '{result.failed_stage}'[/bold red]")
            console.print(f"  Error: {result.error}")
            console.print(f"  Run 'vmware2scw resume --migration-id {result.migration_id}' to retry")
            sys.exit(1)


@main.command("migrate-batch")
@click.option("--plan", "plan_path", required=True, type=click.Path(exists=True), help="Batch migration plan YAML")
@click.option("--config", "config_path", type=click.Path(exists=True), help="Configuration file")
def migrate_batch(plan_path: str, config_path: str | None):
    """Execute a batch migration plan."""
    config = load_config(config_path)
    batch = BatchMigrationPlan.from_yaml(plan_path)

    from vmware2scw.pipeline.batch import BatchMigrationRunner

    runner = BatchMigrationRunner(config)
    runner.run(batch)


@main.command()
@click.option("--migration-id", required=True, help="Migration ID to check")
def status(migration_id: str):
    """Check the status of a migration."""
    from vmware2scw.pipeline.state import MigrationStateStore

    store = MigrationStateStore()
    state = store.load(migration_id)

    if not state:
        console.print(f"[red]Migration '{migration_id}' not found[/red]")
        sys.exit(1)

    console.print(f"\n[bold]Migration: {state.migration_id}[/bold]")
    console.print(f"  VM: {state.vm_name}")
    console.print(f"  Stage: {state.current_stage}")
    console.print(f"  Started: {state.started_at}")
    console.print(f"  Completed stages: {', '.join(state.completed_stages)}")
    if state.error:
        console.print(f"  [red]Error: {state.error}[/red]")


@main.command()
@click.option("--migration-id", required=True, help="Migration ID to resume")
@click.option("--config", "config_path", type=click.Path(exists=True), help="Configuration file")
def resume(migration_id: str, config_path: str | None):
    """Resume a failed migration from the last successful stage."""
    config = load_config(config_path)

    from vmware2scw.pipeline.migration import MigrationPipeline

    pipeline = MigrationPipeline(config)
    result = pipeline.resume(migration_id)

    if result.success:
        console.print(f"\n[bold green]✅ Migration resumed and completed![/bold green]")
    else:
        console.print(f"\n[bold red]❌ Migration still failing at stage '{result.failed_stage}'[/bold red]")
        sys.exit(1)


@main.command()
@click.option("--vm", help="VM name to suggest instance types for")
@click.option("--config", "config_path", type=click.Path(exists=True), help="Configuration file")
def suggest(vm: str, config_path: str | None):
    """Suggest Scaleway instance types for a VMware VM."""
    config = load_config(config_path)

    from vmware2scw.scaleway.mapping import ResourceMapper
    from vmware2scw.vmware.client import VSphereClient
    from vmware2scw.vmware.inventory import VMInventory

    with console.status("[bold green]Connecting to vCenter..."):
        client = VSphereClient()
        pw = config.vmware.password.get_secret_value() if config.vmware.password else ""
        client.connect(config.vmware.vcenter, config.vmware.username, pw, insecure=config.vmware.insecure)

    with console.status(f"[bold green]Fetching VM info for '{vm}'..."):
        inv = VMInventory(client)
        vm_info = inv.get_vm_info(vm)

    mapper = ResourceMapper()
    suggestions = mapper.suggest_instance_type(vm_info)

    table = Table(title=f"Instance Type Suggestions for '{vm}'")
    table.add_column("Type", style="cyan")
    table.add_column("vCPUs", justify="right")
    table.add_column("RAM (GB)", justify="right")
    table.add_column("Storage", justify="right")
    table.add_column("Fit Score", justify="right", style="green")
    table.add_column("Notes")

    for s in suggestions:
        table.add_row(
            s.instance_type,
            str(s.vcpus),
            str(s.ram_gb),
            f"{s.storage_gb}GB",
            f"{s.fit_score:.0%}",
            s.notes,
        )

    console.print(table)
    client.disconnect()


if __name__ == "__main__":
    main()
