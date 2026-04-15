"""Federation: many control planes, one tenant, one budget.

Running one Eyvan per region already works by construction, and for scheduling
that is the right answer -- placement is regional, so a plane that only knows its
own cells is not missing anything it could act on.

**Budgets are where it breaks, and the break is silent.** ``BudgetRecord.spent``
is a single durable counter in one database. A tenant with projects on three
planes therefore has three independent budgets, each enforcing its own limit and
each reporting full compliance while the tenant spends three times what its
operator set. Nothing surfaces that today: every plane's answer is locally true.

**Grants, not consensus.** The fix is not a distributed transaction on every
admission -- that would put a cross-plane round trip on the scheduler's hot path
and make one plane's outage everyone's outage. Instead the tenant's *home* plane
owns the total limit and issues grants: slices of that limit, each addressed to
one peer, each with an expiry. A peer admits work only up to its outstanding
grants. Since the home plane refuses to issue grants summing above the limit,
total spend across every plane is bounded by construction and no plane has to ask
anyone anything at admission time.

**The failure mode is under-use, and that is the correct direction to err.** A
peer holding an unused grant leaves headroom idle; a peer whose grant expires
stops admitting. Both are visible and recoverable. The alternative error --
over-spend discovered at the invoice -- is neither.
"""

from __future__ import annotations

import typing as tp
from datetime import datetime

from pydantic import Field, model_validator

from eyvan._compat import StrEnum
from eyvan.service.models import EyvanModel, utcnow


class GrantState(StrEnum):
    """Lifecycle of one budget or quota grant."""

    ACTIVE = "active"
    #: Withdrawn by the issuing plane. Spend already committed against it is not
    #: clawed back -- the money is gone -- so revocation stops *new* admission and
    #: says nothing about what already ran.
    REVOKED = "revoked"
    #: Past its expiry. Distinct from REVOKED because it needs no issuer action,
    #: which is what makes an unreachable home plane fail closed rather than
    #: leaving a peer spending forever.
    EXPIRED = "expired"


class PlanePeerRecord(EyvanModel):
    """Another control plane this one knows about.

    Deliberately not a cluster membership protocol. A peer is a URL an operator
    wrote down, because the alternative -- planes discovering each other -- means
    a joining plane can claim a tenant's budget by asserting it is the home, and
    there is no authority in this design that could refuse.
    """

    plane_id: str = Field(min_length=1, max_length=128)
    endpoint: str = Field(min_length=1, max_length=512)
    #: Free-text, for an operator reading a list of peers.
    description: str = Field(default="", max_length=512)
    enabled: bool = True
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    @model_validator(mode="after")
    def validate_peer(self) -> PlanePeerRecord:
        if not self.plane_id.strip():
            raise ValueError("plane id must not be blank")
        if not self.endpoint.startswith(("http://", "https://")):
            raise ValueError("plane endpoint must be an http(s) URL")
        return self


class BudgetGrantRecord(EyvanModel):
    """One slice of a tenant's budget, issued by its home plane to a peer.

    The grant is the peer's *entire* authority to spend for this tenant. It has an
    expiry with no default of "never", because a grant that cannot lapse turns a
    home plane that goes away permanently into an unbounded spend on every peer
    that ever held one.
    """

    id: str = Field(min_length=1, max_length=96)
    org_id: str = Field(min_length=1, max_length=128)
    project_id: str = Field(min_length=1, max_length=128)
    #: The plane that owns the tenant's total limit and issued this slice.
    home_plane_id: str = Field(min_length=1, max_length=128)
    #: The plane authorized to spend it.
    grantee_plane_id: str = Field(min_length=1, max_length=128)
    amount: float = Field(ge=0)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    state: GrantState = GrantState.ACTIVE
    issued_at: datetime = Field(default_factory=utcnow)
    expires_at: datetime
    #: What the grantee reported spending against it, as of its last report. A
    #: *report*, never an authority: the grantee's own budget is the enforcement
    #: point, and this is how the home plane sees the fleet-wide total.
    reported_spend: float = Field(default=0.0, ge=0)
    reported_at: datetime | None = None

    @model_validator(mode="after")
    def validate_grant(self) -> BudgetGrantRecord:
        if self.currency != "USD":
            raise ValueError("Eyvan v0 budgets support only USD")
        if self.home_plane_id == self.grantee_plane_id:
            raise ValueError("a plane does not grant budget to itself; it holds the limit directly")
        if self.expires_at <= self.issued_at:
            raise ValueError("a budget grant must expire after it is issued")
        return self

    def active_at(self, moment: datetime) -> bool:
        """Return whether this grant authorizes spend at ``moment``."""

        return self.state is GrantState.ACTIVE and moment < self.expires_at


def outstanding_total(grants: tp.Sequence[BudgetGrantRecord], *, now: datetime) -> float:
    """Return the total currently authorized on peers."""

    return float(sum(grant.amount for grant in grants if grant.active_at(now)))


def issuable_amount(
    limit: float,
    grants: tp.Sequence[BudgetGrantRecord],
    *,
    now: datetime,
    local_spent: float = 0.0,
    replacing: str | None = None,
) -> float:
    """Return how much the home plane may still grant away.

    Local spend counts against the same limit: a home plane that has itself spent
    most of the budget cannot grant the rest away as if it were untouched, or the
    two together would exceed the limit an operator set.

    ``replacing`` excludes one existing grant, so re-issuing a peer's grant at a
    new amount is measured against the headroom without it rather than against
    headroom that already counts it.
    """

    committed = float(
        sum(grant.amount for grant in grants if grant.active_at(now) and (replacing is None or grant.id != replacing))
    )
    return max(0.0, limit - committed - local_spent)


def federated_spend(
    local_spent: float,
    grants: tp.Sequence[BudgetGrantRecord],
) -> float:
    """Return the tenant's total reported spend across every plane.

    Sums the *reported* figures, which are as fresh as each peer's last report and
    no fresher. That staleness is why this number informs and alerts but never
    admits: admission is enforced locally against a grant that was already bounded
    when it was issued.
    """

    return float(local_spent + sum(grant.reported_spend for grant in grants))


__all__ = (
    "BudgetGrantRecord",
    "GrantState",
    "PlanePeerRecord",
    "federated_spend",
    "issuable_amount",
    "outstanding_total",
)
