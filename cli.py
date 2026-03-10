#!/usr/bin/env python
"""
CLI for ad-hoc Kineis/CLS API requests.

Uses the async kineis_client module directly. Credentials can be passed as
options or read from environment variables (KINEIS_USERNAME, KINEIS_PASSWORD).

Usage:
    python cli.py auth
    python cli.py devices
    python cli.py bulk --from-dt 2026-03-01T00:00:00.000Z --to-dt 2026-03-09T00:00:00.000Z
    python cli.py realtime --checkpoint 0
    python cli.py bulk --device-refs REF1,REF2 --from-dt ... --to-dt ...
"""

import asyncio
import json
import os
import sys

import click

# Ensure the project root is on sys.path so `app` is importable
sys.path.insert(0, os.path.dirname(__file__))


def _run(coro):
    return asyncio.run(coro)


def _print_json(data):
    click.echo(json.dumps(data, indent=2, default=str))


def _parse_list(value):
    """Split a comma-separated string into a list, or return None."""
    if not value:
        return None
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_int_list(value):
    if not value:
        return None
    return [int(v.strip()) for v in value.split(",") if v.strip()]


def _common_options(f):
    """Shared credential options for all subcommands."""
    f = click.option("--client-id", default="api-telemetry", show_default=True, help="OAuth client_id")(f)
    f = click.option("--password", "-p", envvar="KINEIS_PASSWORD", required=True, help="CLS/Kineis API password")(f)
    f = click.option("--username", "-u", envvar="KINEIS_USERNAME", required=True, help="CLS/Kineis API username")(f)
    return f


@click.group()
def cli():
    """Ad-hoc Kineis/CLS API client."""


@cli.command()
@_common_options
def auth(username, password, client_id):
    """Authenticate and print the token response."""
    from app.services.kineis_client import get_access_token

    result = _run(get_access_token(
        username=username,
        password=password,
        client_id=client_id,
    ))
    _print_json(result)


@cli.command()
@_common_options
def devices(username, password, client_id):
    """List accessible devices."""
    from app.services.kineis_client import get_access_token, retrieve_device_list

    async def _devices():
        token_resp = await get_access_token(
            username=username,
            password=password,
            client_id=client_id,
        )
        return await retrieve_device_list(access_token=token_resp["access_token"])

    result = _run(_devices())
    _print_json(result)


@cli.command()
@_common_options
@click.option("--from-dt", required=True, help="Start datetime (e.g. 2026-03-01T00:00:00.000Z)")
@click.option("--to-dt", required=True, help="End datetime (e.g. 2026-03-09T00:00:00.000Z)")
@click.option("--page-size", default=100, show_default=True, type=int, help="Pagination page size")
@click.option("--device-refs", default=None, help="Comma-separated device refs to filter")
@click.option("--device-uids", default=None, help="Comma-separated device UIDs to filter")
@click.option("--no-metadata", is_flag=True, help="Omit metadata from response")
@click.option("--no-raw-data", is_flag=True, help="Omit raw data from response")
def bulk(username, password, client_id, from_dt, to_dt, page_size, device_refs, device_uids, no_metadata, no_raw_data):
    """Fetch bulk telemetry for a time window."""
    from app.services.kineis_client import get_access_token, retrieve_bulk_telemetry

    async def _bulk():
        token_resp = await get_access_token(
            username=username,
            password=password,
            client_id=client_id,
        )
        return await retrieve_bulk_telemetry(
            access_token=token_resp["access_token"],
            from_datetime=from_dt,
            to_datetime=to_dt,
            page_size=page_size,
            device_refs=_parse_list(device_refs),
            device_uids=_parse_int_list(device_uids),
            retrieve_metadata=not no_metadata,
            retrieve_raw_data=not no_raw_data,
        )

    messages = _run(_bulk())
    click.echo(f"Fetched {len(messages)} messages", err=True)
    _print_json(messages)


@cli.command()
@_common_options
@click.option("--checkpoint", default=0, show_default=True, type=int, help="Checkpoint (0 = last 6 hours)")
@click.option("--device-refs", default=None, help="Comma-separated device refs to filter")
@click.option("--device-uids", default=None, help="Comma-separated device UIDs to filter")
@click.option("--no-metadata", is_flag=True, help="Omit metadata from response")
@click.option("--no-raw-data", is_flag=True, help="Omit raw data from response")
def realtime(username, password, client_id, checkpoint, device_refs, device_uids, no_metadata, no_raw_data):
    """Fetch realtime telemetry since a checkpoint."""
    from app.services.kineis_client import get_access_token, retrieve_realtime_telemetry

    async def _realtime():
        token_resp = await get_access_token(
            username=username,
            password=password,
            client_id=client_id,
        )
        return await retrieve_realtime_telemetry(
            access_token=token_resp["access_token"],
            checkpoint=checkpoint,
            device_refs=_parse_list(device_refs),
            device_uids=_parse_int_list(device_uids),
            retrieve_metadata=not no_metadata,
            retrieve_raw_data=not no_raw_data,
        )

    messages, new_checkpoint = _run(_realtime())
    click.echo(f"Fetched {len(messages)} messages, new checkpoint: {new_checkpoint}", err=True)
    _print_json(messages)


if __name__ == "__main__":
    cli()
