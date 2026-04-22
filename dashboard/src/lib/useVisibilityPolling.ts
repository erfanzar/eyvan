/*
 * Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { useEffect, useRef } from "react";

/**
 * Invoke `callback` every `intervalMs` while the tab is visible.
 *
 * Polling pauses while the document is hidden, fires immediately when the tab
 * becomes visible again, and is fully cleaned up on unmount or when `active`
 * turns false.
 */
export function useVisibilityPolling(callback: () => void, intervalMs: number, active = true): void {
  const savedCallback = useRef<() => void>(() => {});

  useEffect(() => {
    savedCallback.current = callback;
  }, [callback]);

  useEffect(() => {
    if (!active) return;
    let interval: ReturnType<typeof globalThis.setInterval> | undefined;
    const tick = () => savedCallback.current();
    const start = () => {
      if (interval === undefined) interval = globalThis.setInterval(tick, intervalMs);
    };
    const stop = () => {
      if (interval !== undefined) {
        globalThis.clearInterval(interval);
        interval = undefined;
      }
    };
    const onVisibilityChange = () => {
      if (globalThis.document?.hidden) {
        stop();
      } else {
        tick();
        start();
      }
    };
    if (!globalThis.document?.hidden) start();
    globalThis.document?.addEventListener("visibilitychange", onVisibilityChange);
    return () => {
      stop();
      globalThis.document?.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [intervalMs, active]);
}
