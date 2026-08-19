// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/**
 * How a request is described, and the multiplication it must not invent.
 *
 * A TPU slice type states its own size: `v5p-16` is one slice of 8 chips. The
 * page paired that chip count with the slice name as `8x v5p-16`, which reads
 * as eight slices -- 64 chips -- for a request that asked for one. Every shape
 * on the page was inflated by its own chip count.
 */

import { describe, expect, it } from "vitest";

import { formatAcceleratorRequest } from "./format";

describe("describing an accelerator request", () => {
  it("does not multiply a slice by its own chip count", () => {
    expect(formatAcceleratorRequest("v5p-16", 8, 1)).toBe("v5p-16 · 8 chips");
    expect(formatAcceleratorRequest("v5p-16", 8, 1)).not.toContain("8x v5p-16");
  });

  it("uses the multiplier for replicas, which is what pods=N allocates", () => {
    expect(formatAcceleratorRequest("v5p-16", 8, 2)).toBe("2 x v5p-16 · 8 chips");
  });

  it("keeps the chip count, because that is what admission and quota count", () => {
    expect(formatAcceleratorRequest("v6e-64", 64, 1)).toContain("64 chips");
  });

  it("says None rather than inventing a shape", () => {
    expect(formatAcceleratorRequest(undefined, undefined, 1)).toBe("None");
  });

  it("reports chips alone when the shape is unknown", () => {
    // A pool can carry a resource vector without an accelerator type; the
    // chips are still real and dropping them would report no accelerator.
    expect(formatAcceleratorRequest(undefined, 4, 1)).toBe("4 chips");
  });
});
