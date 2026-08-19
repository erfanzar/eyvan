def test_a_slow_start_leaves_the_server_running_instead_of_killing_it() -> None:
    """The failure that left a fleet with no control plane.

    Startup reconciles every cell, and each one provisioning or draining costs
    a provider round trip -- so on a real fleet the server is simply slower than
    the budget. Killing it there destroyed a reconcile in progress and left
    nothing running while TPU capacity was ACTIVE, which is strictly worse than
    waiting: the process was seconds from healthy.
    """

    import pytest

    from eyvan.sdk.client.endpoint import ControlPlaneEndpoint
    from eyvan.server.local.daemon import LocalDaemonStartupError, LocalDaemonSupervisor

    killed: list[object] = []

    class _StillStarting:
        """A process that is alive and has not answered yet."""

        pid = 4242

        def poll(self) -> None:
            return None

    supervisor = LocalDaemonSupervisor(
        launcher=type("L", (), {"start": lambda self, command: _StillStarting()})(),
        health_probe=lambda _endpoint: False,
        startup_timeout_s=0.01,
        clock=iter([0.0, 0.0, 1.0, 2.0, 3.0]).__next__,
        sleeper=lambda _seconds: None,
        process_terminator=lambda *args, **kwargs: killed.append(args),
        process_killer=lambda *args, **kwargs: killed.append(args),
    )

    with pytest.raises(LocalDaemonStartupError) as raised:
        supervisor.ensure(ControlPlaneEndpoint("http://127.0.0.1:8080"))

    assert "still starting" in str(raised.value)
    assert "left running" in str(raised.value)
    assert killed == [], "a server that has not died must not be killed for being slow"


def test_the_startup_budget_fits_a_fleet_rather_than_one_local_pool() -> None:
    """Twenty seconds was the budget of a plane holding a single local pool."""

    from eyvan.server.local.daemon import LocalDaemonSupervisor

    assert LocalDaemonSupervisor().startup_timeout_s >= 60
